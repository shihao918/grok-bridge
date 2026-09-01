"""Replacement backend v2: native Grok Bot chat with YOUR models.

Flow: App SendGrokBotUserMessage → we run the agent loop (your gateway/Ollama)
→ we append transcript entries (send_message / assistant_text) → the app's
WatchGrokBotTranscripts stream delivers them → the app UI renders natively.

Zero Grok inference. Zero quota.
"""

import base64
import hashlib
import json
import os
import re
import secrets
import struct
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

import httpx  # noqa: E402

LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\backend_calls.jsonl"
RUNTIME_LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\backend_server_runtime.log"
os.makedirs(os.path.dirname(LOG), exist_ok=True)
LOG_BODY_MAX = 1200
PERSISTENCE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state", "backend_transcript_state.json")
AVATAR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state", "agent_avatars")
PERSISTENCE_VERSION = 1
MAX_AVATAR_BYTES = 4 * 1024 * 1024
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_LOG_SENSITIVE_KEY = re.compile(
    r"(?:token|secret|password|credential|authorization|api[_-]?key|cookie)",
    re.IGNORECASE,
)
_LOG_BLOB_KEY = re.compile(r"(?:pngbase64|avatardataurl|avatarbytes)", re.IGNORECASE)
_LOG_SENSITIVE_PATH = re.compile(r"(?:oauth/token|token|credential|secret)", re.IGNORECASE)
_LOG_BEARER = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]+")
_LOG_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}(?:\.[A-Za-z0-9_-]+)?\b")
_LOG_KEY_VALUE = re.compile(
    r"(?i)((?:token|secret|password|credential|authorization|api[_-]?key)\s*[:=]\s*)([^,&\s\"'}]+)"
)

# ---- transcript store: agent_id → {generation, next_seq, entries:[entry]}
TRANSCRIPTS = {}
LOCK = threading.Lock()
TRANSCRIPT_CHANGED = threading.Condition(LOCK)
SSE_SUBSCRIBERS = {}  # Handler -> per-connection write lock for gateway /events
TRANSCRIPT_EPOCH = str(uuid.uuid4())
AGENT_INDEX = {}  # agent_id → metadata visible to the app
PROMPT_ACCEPTANCE = {}  # (agent_id, client_nonce) → acceptance record
PROMPT_CLAIM_LOCK = threading.Lock()
AGENT_EXECUTORS = {}
AGENT_EXECUTORS_LOCK = threading.Lock()
AGENT_CREATE_LOCK = threading.Lock()
LOCAL_EXEC_CREDENTIAL = "bridge-local-exec-credential"
LOCAL_EXEC_TOKEN = "bridge-local-exec-token"
LOCAL_EXEC_BASE_URL = "http://127.0.0.1:9000"
LOCAL_EXEC_PROVIDERS = {}
RUNTIME_CAPABILITIES = {
    # These flags describe the local replacement backend, not the remote Grok
    # service. Temporal creation remains disabled because execution is local
    # box/Ollama only.
    "durableIdentityEnabled": True,
    "durableIdentityWritesEnabled": True,
    "temporalCreationEnabled": False,
    "agentMessagingEnabled": True,
}
SAND_MACHINE = {
    "machineId": "",
    "label": bc.config().get("label", "Local Grok Bridge"),
    "localToolPermission": "ask",
}


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(RUNTIME_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _default_agent_meta(agent_id: str, *, name: str = "Bridge Bot", description: str = "Local Ollama-backed Grok Bot") -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "name": name,
        "description": description,
        "title": name,
        "createdAtMs": now_ms,
        "updatedAtMs": now_ms,
        "legacyAgentId": agent_id,
        "avatarShape": "",
        "avatarColor": "",
        "avatarVersion": "",
        "agentId": agent_id,
        "harness": "box",
        "role": "assistant",
        "viewerIsOwner": True,
        "origin": "local",
        "purpose": "",
        "kickstartRequested": False,
        "introductionSuppressed": False,
        "creationClientNonce": "",
    }


def _state_snapshot_locked() -> dict:
    """Build the complete durable state while TRANSCRIPT_CHANGED is held."""
    return {
        "version": PERSISTENCE_VERSION,
        "epoch": TRANSCRIPT_EPOCH,
        "agents": {str(agent_id): dict(meta) for agent_id, meta in AGENT_INDEX.items()},
        "transcripts": {
            str(agent_id): {
                "generation": int(transcript.get("generation", 1)),
                "next_seq": int(transcript.get("next_seq", 1)),
                "entries": [dict(entry) for entry in transcript.get("entries", [])],
            }
            for agent_id, transcript in TRANSCRIPTS.items()
        },
        "acceptance": [
            {"agentId": agent_id, "clientNonce": nonce, "record": dict(record)}
            for (agent_id, nonce), record in PROMPT_ACCEPTANCE.items()
        ],
    }


def _persist_state_locked() -> None:
    """Atomically replace the durable transcript state.

    The temporary file is created beside the target so os.replace is atomic on
    the local filesystem. A completed write therefore contains either the old
    snapshot or the new one, never a partially-written JSON document.
    """
    os.makedirs(os.path.dirname(PERSISTENCE_FILE), exist_ok=True)
    temp_path = f"{PERSISTENCE_FILE}.{os.getpid()}.tmp"
    payload = json.dumps(_state_snapshot_locked(), ensure_ascii=False, separators=(",", ":"))
    try:
        with open(temp_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, PERSISTENCE_FILE)
    except Exception:
        try:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        except OSError:
            pass
        raise


def _load_persisted_state() -> None:
    """Restore the last complete snapshot; tolerate an absent/corrupt file."""
    global TRANSCRIPT_EPOCH
    if not os.path.exists(PERSISTENCE_FILE):
        return
    try:
        with open(PERSISTENCE_FILE, encoding="utf-8") as f:
            snapshot = json.load(f)
    except Exception as exc:
        print(f"[state] ignored unreadable transcript snapshot: {type(exc).__name__}: {str(exc)[:120]}", flush=True)
        return
    if not isinstance(snapshot, dict) or snapshot.get("version") != PERSISTENCE_VERSION:
        print("[state] ignored unsupported transcript snapshot version", flush=True)
        return

    epoch = str(snapshot.get("epoch") or "").strip()
    if epoch:
        TRANSCRIPT_EPOCH = epoch

    agents = snapshot.get("agents") if isinstance(snapshot.get("agents"), dict) else {}
    transcripts = snapshot.get("transcripts") if isinstance(snapshot.get("transcripts"), dict) else {}
    acceptance = snapshot.get("acceptance") if isinstance(snapshot.get("acceptance"), list) else []
    with TRANSCRIPT_CHANGED:
        for agent_id, meta in agents.items():
            aid = str(agent_id).strip()
            if aid and isinstance(meta, dict):
                AGENT_INDEX[aid] = dict(meta)
        for agent_id, raw in transcripts.items():
            aid = str(agent_id).strip()
            if not aid or not isinstance(raw, dict):
                continue
            entries = [dict(entry) for entry in raw.get("entries", []) if isinstance(entry, dict)]
            try:
                generation = max(1, int(raw.get("generation", 1)))
            except (TypeError, ValueError):
                generation = 1
            try:
                next_seq = max(1, int(raw.get("next_seq", 1)))
            except (TypeError, ValueError):
                next_seq = 1
            max_seq = max((int(entry.get("seq", 0)) for entry in entries), default=0)
            TRANSCRIPTS[aid] = {
                "generation": generation,
                "next_seq": max(next_seq, max_seq + 1),
                "entries": entries,
            }
            AGENT_INDEX.setdefault(aid, _default_agent_meta(aid))
        for item in acceptance:
            if not isinstance(item, dict):
                continue
            aid = str(item.get("agentId") or "").strip()
            nonce = str(item.get("clientNonce") or "").strip()
            record = item.get("record")
            if aid and nonce and isinstance(record, dict):
                PROMPT_ACCEPTANCE[(aid, nonce)] = dict(record)
                AGENT_INDEX.setdefault(aid, _default_agent_meta(aid))
                TRANSCRIPTS.setdefault(aid, {"generation": 1, "next_seq": 1, "entries": []})
    total_entries = sum(len(t.get("entries", [])) for t in TRANSCRIPTS.values())
    print(
        f"[state] restored agents={len(AGENT_INDEX)} entries={total_entries} acceptances={len(PROMPT_ACCEPTANCE)}",
        flush=True,
    )


_load_persisted_state()


def _redact_log_obj(value):
    if isinstance(value, dict):
        return {
            key: "<redacted>"
            if _LOG_SENSITIVE_KEY.search(str(key)) or _LOG_BLOB_KEY.search(str(key))
            else _redact_log_obj(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_log_obj(item) for item in value]
    return value


def _redact_log_body(path: str, body: bytes) -> str:
    """Return a bounded request-body summary without persisting credentials."""
    if not body:
        return ""
    if _LOG_SENSITIVE_PATH.search(path or ""):
        return f"<redacted body len={len(body)}>"
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return f"<opaque body len={len(body)}>"
    try:
        text = json.dumps(_redact_log_obj(json.loads(text)), ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError, json.JSONDecodeError):
        text = _LOG_KEY_VALUE.sub(r"\1<redacted>", text)
    text = _LOG_BEARER.sub(r"\1<redacted>", text)
    text = _LOG_JWT.sub("<redacted-jwt>", text)
    return text[:LOG_BODY_MAX]


def b64(s: str) -> str:
    import base64

    return base64.b64encode(s.encode("utf-8")).decode()


def _append_entry_locked(agent_id: str, kind: str, obj: dict) -> tuple[dict, int]:
    """Append one row; caller holds TRANSCRIPT_CHANGED."""
    t = TRANSCRIPTS.setdefault(agent_id, {"generation": 1, "next_seq": 1, "entries": []})
    seq = t["next_seq"]
    entry_id = str(uuid.uuid4())
    body_obj = dict(obj) if isinstance(obj, dict) else obj
    if isinstance(body_obj, dict):
        # The native transcript reducer keys and validates decoded bodies by
        # their own `id`; keep it identical to the wire entry id so replay,
        # dedupe, and optimistic-echo reconciliation agree.
        body_obj.setdefault("id", entry_id)
    entry = {
        "seq": seq,
        "entryKind": kind,
        "body": b64(json.dumps(body_obj, ensure_ascii=False)),
        "updatedSeq": seq,
        "entryId": entry_id,
        "bodyOmitted": False,
    }
    t["next_seq"] += 1
    t["entries"].append(entry)
    return entry, t["generation"]


def append_entry(agent_id: str, kind: str, obj: dict) -> dict:
    with TRANSCRIPT_CHANGED:
        entry, generation = _append_entry_locked(agent_id, kind, obj)
        _persist_state_locked()
        TRANSCRIPT_CHANGED.notify_all()
    _broadcast_transcript_event(agent_id, entry, generation)
    return entry


def claim_user_prompt(agent_id: str, prompt: str, client_nonce: str) -> tuple[dict | None, dict | None]:
    """Atomically claim a user prompt and create its optimistic echo record."""
    with PROMPT_CLAIM_LOCK:
        with TRANSCRIPT_CHANGED:
            existing = PROMPT_ACCEPTANCE.get((agent_id, client_nonce))
            if existing is not None:
                return None, dict(existing)

            user_entry, generation = _append_entry_locked(
                agent_id,
                "message",
                {
                    "kind": "message",
                    "role": "user",
                    "content": prompt,
                    "isStreaming": False,
                    "clientNonce": client_nonce,
                    "timestampMs": int(time.time() * 1000),
                },
            )
            record = {
                "status": "accepted",
                "echoEntryId": user_entry["entryId"],
                "acceptedAtMs": int(time.time() * 1000),
            }
            PROMPT_ACCEPTANCE[(agent_id, client_nonce)] = record
            # User echo + acceptance are committed in one snapshot. A retry
            # after restart therefore cannot manufacture a second echo.
            _persist_state_locked()
            TRANSCRIPT_CHANGED.notify_all()
        _broadcast_transcript_event(agent_id, user_entry, generation)
        return user_entry, record


def update_prompt_acceptance(agent_id: str, client_nonce: str, **changes) -> dict | None:
    """Durably update an accepted prompt's completion/failure metadata."""
    if not client_nonce:
        return None
    with TRANSCRIPT_CHANGED:
        current = PROMPT_ACCEPTANCE.get((agent_id, client_nonce))
        if current is None:
            return None
        current.update(changes)
        record = dict(current)
        _persist_state_locked()
        TRANSCRIPT_CHANGED.notify_all()
        return record


def _commit_prompt_result(agent_id: str, client_nonce: str, obj: dict, *, failed: bool = False, error: str = "") -> dict:
    """Append a model result and update its acceptance record in one snapshot."""
    with TRANSCRIPT_CHANGED:
        entry, generation = _append_entry_locked(agent_id, "message", obj)
        if client_nonce:
            record = PROMPT_ACCEPTANCE.get((agent_id, client_nonce))
            if record is not None:
                record.update(
                    {
                        "status": "failed" if failed else "accepted",
                        "completedAtMs": int(time.time() * 1000),
                        **({"rejectionCode": "LOCAL_MODEL_ERROR", "error": error} if failed else {"assistantEntryId": entry["entryId"]}),
                    }
                )
        _persist_state_locked()
        TRANSCRIPT_CHANGED.notify_all()
    _broadcast_transcript_event(agent_id, entry, generation)
    return entry


def _entry_body_obj(entry: dict) -> dict:
    body = entry.get("body")
    if not isinstance(body, str) or not body:
        return {}
    try:
        decoded = base64.b64decode(body, validate=True)
        value = json.loads(decoded.decode("utf-8"))
        return dict(value) if isinstance(value, dict) else {}
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return {}


def _model_task(agent_id: str, task: str, client_nonce: str = "") -> str:
    """Add bounded prior turns so local Ollama sees conversation context."""
    with TRANSCRIPT_CHANGED:
        entries = list(TRANSCRIPTS.get(agent_id, {}).get("entries", []))
    turns = []
    for entry in entries:
        body = _entry_body_obj(entry)
        if body.get("kind") != "message" or body.get("clientNonce") == client_nonce:
            continue
        role = str(body.get("role") or "assistant")
        content = str(body.get("content") or "").strip()
        if content:
            turns.append((role, content[:2000]))
    turns = turns[-12:]
    if not turns:
        return task
    history = "\n".join(f"{role}: {content}" for role, content in turns)
    return f"Conversation history:\n{history}\n\nCurrent user message:\n{task}"


def _group_member_ids(agent_id: str) -> list[str] | None:
    """Return a stable non-group roster snapshot, or ``None`` for a normal Bot."""
    with TRANSCRIPT_CHANGED:
        meta = AGENT_INDEX.get(agent_id)
        if not meta or not meta.get("isGroup"):
            return None
        # Roster validation rejects nested groups.  Re-check here so a group
        # whose metadata is edited externally cannot dispatch into another
        # group or a missing agent.
        return [
            member_id
            for member_id in meta.get("memberIds", [])
            if member_id in AGENT_INDEX and not AGENT_INDEX[member_id].get("isGroup")
        ]


def _group_completed_members(group_id: str, client_nonce: str) -> dict[str, dict]:
    """Read durable per-member results for a group nonce before resuming work."""
    completed = {}
    with TRANSCRIPT_CHANGED:
        entries = list(TRANSCRIPTS.get(group_id, {}).get("entries", []))
    for entry in entries:
        body = _entry_body_obj(entry)
        if body.get("kind") != "message" or body.get("clientNonce") != client_nonce:
            continue
        member_id = _text_field(body.get("memberAgentId"))
        if not member_id:
            continue
        completed.setdefault(
            member_id,
            {
                "status": "failed" if body.get("isError") else "accepted",
                "entryId": str(entry.get("entryId") or body.get("id") or ""),
                **({"error": str(body.get("content") or "")} if body.get("isError") else {}),
            },
        )
    return completed


def _group_agent_loop(group_id: str, task: str, client_nonce: str = "") -> None:
    """Fan one accepted group prompt out to each member and merge replies."""
    member_ids = _group_member_ids(group_id) or []
    completed = _group_completed_members(group_id, client_nonce) if client_nonce else {}
    assistant_entry_ids = [
        result["entryId"]
        for result in completed.values()
        if result.get("status") == "accepted" and result.get("entryId")
    ]
    failed_member_ids = [member_id for member_id, result in completed.items() if result.get("status") == "failed"]

    for member_id in member_ids:
        if member_id in completed:
            continue
        try:
            reply = call_model(_model_task(member_id, task, client_nonce))
            entry = _commit_prompt_result(
                group_id,
                "",
                {
                    "kind": "message",
                    "role": "assistant",
                    "content": reply,
                    "isStreaming": False,
                    "clientNonce": client_nonce,
                    "memberAgentId": member_id,
                    "authorId": member_id,
                    "timestampMs": int(time.time() * 1000),
                },
            )
            assistant_entry_ids.append(entry["entryId"])
            completed[member_id] = {"status": "accepted", "entryId": entry["entryId"]}
            log(f"[group-loop] {group_id[:8]} member={member_id[:8]} assistant committed ({len(reply)} chars)")
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:240]}"
            failed_member_ids.append(member_id)
            try:
                entry = _commit_prompt_result(
                    group_id,
                    "",
                    {
                        "kind": "message",
                        "role": "assistant",
                        "content": f"Local model execution failed for member {member_id}: {detail}",
                        "isStreaming": False,
                        "isError": True,
                        "errorCode": "LOCAL_MODEL_ERROR",
                        "clientNonce": client_nonce,
                        "memberAgentId": member_id,
                        "authorId": member_id,
                        "timestampMs": int(time.time() * 1000),
                    },
                )
                completed[member_id] = {"status": "failed", "entryId": entry["entryId"], "error": detail}
            except Exception as commit_error:
                log(
                    f"[group-loop] failure transcript commit error member={member_id[:8]}: "
                    f"{type(commit_error).__name__}: {str(commit_error)[:120]}"
                )
                completed[member_id] = {"status": "failed", "error": detail}
            log(f"[group-loop] member={member_id[:8]} error: {detail[:280]}")

        # Persist progress after each member.  If the process crashes and the
        # accepted nonce is resumed, transcript rows let us skip completed
        # members without manufacturing duplicate replies.
        if client_nonce:
            update_prompt_acceptance(
                group_id,
                client_nonce,
                groupMemberResults={member: dict(result) for member, result in completed.items()},
            )

    if client_nonce:
        failures = [member for member in member_ids if completed.get(member, {}).get("status") == "failed"]
        changes = {
            "status": "failed" if failures else "accepted",
            "completedAtMs": int(time.time() * 1000),
            "groupMemberResults": {member: dict(result) for member, result in completed.items()},
            "assistantEntryIds": list(assistant_entry_ids),
            **({"failedMemberIds": failures, "rejectionCode": "LOCAL_MODEL_ERROR", "error": "one or more group members failed"} if failures else {}),
        }
        update_prompt_acceptance(group_id, client_nonce, **changes)


def _submit_agent_loop(agent_id: str, task: str, client_nonce: str = "") -> None:
    """Serialize model work per agent while keeping different agents independent."""
    group_member_ids = _group_member_ids(agent_id)
    worker = _group_agent_loop if group_member_ids is not None else agent_loop
    with AGENT_EXECUTORS_LOCK:
        executor = AGENT_EXECUTORS.get(agent_id)
        if executor is None:
            executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"bridge-{agent_id[:8]}")
            AGENT_EXECUTORS[agent_id] = executor
    executor.submit(worker, agent_id, task, client_nonce)


def resume_pending_prompts() -> int:
    """Requeue accepted prompts that had no durable model result before a crash."""
    pending = []
    with TRANSCRIPT_CHANGED:
        for (agent_id, client_nonce), record in PROMPT_ACCEPTANCE.items():
            if record.get("status") != "accepted" or record.get("completedAtMs"):
                continue
            echo_id = record.get("echoEntryId")
            for entry in TRANSCRIPTS.get(agent_id, {}).get("entries", []):
                if entry.get("entryId") != echo_id:
                    continue
                prompt = str(_entry_body_obj(entry).get("content") or "").strip()
                if prompt:
                    pending.append((agent_id, prompt, client_nonce))
                break
    for agent_id, prompt, client_nonce in pending:
        _submit_agent_loop(agent_id, prompt, client_nonce)
    return len(pending)


def call_model(task: str) -> str:
    """Run the configured local Ollama model without any remote fallback."""
    cfg = bc.config()
    ollama_url = cfg.get("ollama_url", "http://127.0.0.1:11434").rstrip("/")
    ollama_model = cfg.get("ollama_model", "gemma4-26b-qat-uncensored:q4km")
    try:
        r = httpx.post(
            f"{ollama_url}/api/chat",
            json={
                "model": ollama_model,
                "messages": [{"role": "user", "content": task}],
                "stream": False,
            },
            timeout=300,
            trust_env=False,
        )
        r.raise_for_status()
        content = (r.json().get("message") or {}).get("content", "")
        if content:
            log(f"[model] ollama {ollama_model} replied ({len(content)} chars)")
            return content
        detail = "response did not contain message.content"
    except Exception as exc:
        detail = f"{type(exc).__name__}: {str(exc)[:160]}"
    message = f"local Ollama unavailable at {ollama_url} using {ollama_model}: {detail}"
    log(f"[model] local-only failure; gateway disabled: {message}")
    raise RuntimeError(message)


def agent_loop(agent_id: str, task: str, client_nonce: str = "") -> None:
    """Run the local model and append a renderer-valid assistant message."""
    try:
        reply = call_model(_model_task(agent_id, task, client_nonce))
        _commit_prompt_result(
            agent_id,
            client_nonce,
            {
                "kind": "message",
                "role": "assistant",
                "content": reply,
                "isStreaming": False,
                "timestampMs": int(time.time() * 1000),
            },
        )
        log(f"[loop] {agent_id[:8]} assistant_text committed ({len(reply)} chars)")
    except Exception as e:
        detail = f"{type(e).__name__}: {str(e)[:240]}"
        log(f"[loop] error: {detail[:280]}")
        try:
            _commit_prompt_result(
                agent_id,
                client_nonce,
                {
                    "kind": "message",
                    "role": "assistant",
                    "content": f"Local model execution failed: {detail}",
                    "isStreaming": False,
                    "isError": True,
                    "errorCode": "LOCAL_MODEL_ERROR",
                    "timestampMs": int(time.time() * 1000),
                },
                failed=True,
                error=detail,
            )
        except Exception as commit_error:
            log(f"[loop] failure transcript commit error: {type(commit_error).__name__}: {str(commit_error)[:120]}")


# ---- OAuth (M1, unchanged) ----
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def mint_jwt(claims: dict) -> str:
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = b64url(json.dumps(claims).encode())
    sig = b64url(hmac.new(b"grok-bridge-dev-secret", f"{header}.{payload}".encode(), hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"


import hashlib  # noqa: E402
import hmac  # noqa: E402


def handle_oauth_token(body: dict) -> dict:
    sub, email = "grok|bridge-user", None
    rt = body.get("refresh_token")
    if rt:
        try:
            payload = json.loads(b64url_dec(rt.split(".")[1]).decode())
            sub = payload.get("sub", sub)
            email = payload.get("email")
        except Exception:
            pass
    now = int(time.time())
    access = mint_jwt({"sub": sub, **({"email": email} if email else {}), "exp": now + 3600 * 24, "iss": "grok-bridge"})
    return {
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": 3600 * 24,
        "refresh_token": rt or mint_jwt({"sub": sub, "exp": now + 3600 * 24 * 30}),
        "scope": "openid profile email offline_access",
    }


# ---- identity (M2, unchanged) ----
RESPONSES = {
    "/aiserver.v1.DashboardService/GetMe": {
        "authId": "grok|bridge-user",
        "userId": 1,
        "email": "bridge@local",
        "firstName": "Bridge",
        "lastName": "Local",
        "isEnterpriseUser": False,
    },
    "/aiserver.v1.DashboardService/GetTeams": {"teams": []},
    "/aiserver.v1.DashboardService/GetUserPrivacyMode": {
        # NO_TRAINING permits the 0.30 multi-machine roster while preserving
        # the local-only, no-training privacy contract. NO_STORAGE (1) makes
        # the desktop reject machine storage before ListSandMachines is sent.
        "privacyMode": 2,
        "hoursRemainingInGracePeriod": 0,
        "isEnforcedByTeam": False,
        "isNotMigratedToServerSourceOfTruth": False,
        "partnerDataShare": False,
        "hasAcknowledgedGracePeriodDisclaimer": True,
    },
    "/aiserver.v1.GrokBotService/EnsureSandBox": {
        "cluster": "local",
        "tenantId": "local-tenant",
        "podId": "local-pod",
        "networkToken": "",
        "execDaemonAuthToken": "bridge-local-exec-token",
        "execDaemonUrl": LOCAL_EXEC_BASE_URL,
        "vncUrl": "",
        "terminalsFolder": bc.config().get("local_root", ""),
        "forkVncBaseUrl": "",
        "gatewayUrl": LOCAL_EXEC_BASE_URL,
        "gatewayToken": LOCAL_EXEC_TOKEN,
    },
    "/aiserver.v1.GrokBotService/GetSandBoxRunState": {"state": "SAND_BOX_RUN_STATE_RUNNING", "imageUpdateAvailable": False},
    "/aiserver.v1.GrokBotService/ListSandBoxes": {"boxes": [{"running": True}]},
    "/aiserver.v1.DashboardService/GetSandAccessStatus": {"hasAccess": True},
    "/aiserver.v1.DashboardService/GetSandTrialClaimStatus": {"status": 1},
    "/aiserver.v1.DashboardService/GetHardLimit": {"noUsageBasedAllowed": True},
    "/aiserver.v1.GrokBotService/ListGrokBotAgents": {"agents": []},  # placeholder, filled dynamically
}


def ensure_agent(agent_id: str, *, name: str = "Bridge Bot", description: str = "Local Ollama-backed Grok Bot") -> dict:
    """Materialize an agent lazily so an app-created or restored id always has a store."""
    with TRANSCRIPT_CHANGED:
        now_ms = int(time.time() * 1000)
        changed = False
        if agent_id not in AGENT_INDEX:
            AGENT_INDEX[agent_id] = _default_agent_meta(agent_id, name=name, description=description)
            changed = True
        meta = AGENT_INDEX[agent_id]
        if meta.get("harness") == "local":
            meta["harness"] = "box"
            changed = True
        if agent_id not in TRANSCRIPTS:
            TRANSCRIPTS[agent_id] = {"generation": 1, "next_seq": 1, "entries": []}
            changed = True
        if changed:
            _persist_state_locked()
        return dict(meta)


def _text_field(value, default: str = "") -> str:
    if not isinstance(value, str):
        return default
    value = value.strip()
    return value if value else default


class GatewayContractError(ValueError):
    """Structured local-gateway validation failure."""

    def __init__(self, code: str, message: str, *, status: int = 400, **details) -> None:
        super().__init__(message)
        self.status = status
        self.payload = {"code": code, "message": message, **details}


def _normalize_group_member_ids(value) -> list[str]:
    if not isinstance(value, list):
        raise GatewayContractError(
            "INVALID_GROUP_MEMBERS",
            "memberAgentIds must be a non-empty array of agent ids",
        )
    member_ids = []
    seen = set()
    for raw_member_id in value:
        member_id = _text_field(raw_member_id)
        if not member_id:
            raise GatewayContractError(
                "INVALID_GROUP_MEMBERS",
                "memberAgentIds must contain only non-empty agent ids",
            )
        if member_id not in seen:
            seen.add(member_id)
            member_ids.append(member_id)
    if not member_ids:
        raise GatewayContractError(
            "INVALID_GROUP_MEMBERS",
            "memberAgentIds must contain at least one agent id",
        )
    return member_ids


def _validate_group_member_ids_locked(member_ids: list[str]) -> None:
    """Validate a group roster while ``TRANSCRIPT_CHANGED`` is held."""
    for member_id in member_ids:
        member = AGENT_INDEX.get(member_id)
        if member is None:
            raise GatewayContractError(
                "UNKNOWN_GROUP_MEMBER",
                "memberAgentIds contains an unknown agent id",
                memberAgentId=member_id,
            )
        if member.get("isGroup"):
            raise GatewayContractError(
                "GROUP_MEMBER_NOT_ALLOWED",
                "a channel cannot contain another channel",
                memberAgentId=member_id,
            )


def _create_gateway_group(req: dict) -> dict:
    """Create or replay one durable 0.30 roster group."""
    name = _text_field(req.get("name"))
    if not name:
        raise GatewayContractError("INVALID_GROUP_NAME", "name must not be empty")
    description = _text_field(req.get("description"))
    member_ids = _normalize_group_member_ids(req.get("memberAgentIds"))
    fingerprint = hashlib.sha256(
        json.dumps(
            {"name": name, "memberAgentIds": member_ids},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    with AGENT_CREATE_LOCK:
        with TRANSCRIPT_CHANGED:
            for existing_id, existing_meta in AGENT_INDEX.items():
                if existing_meta.get("isGroup") and existing_meta.get("groupCreationFingerprint") == fingerprint:
                    meta = dict(existing_meta)
                    group_id = existing_id
                    break
            else:
                _validate_group_member_ids_locked(member_ids)

                group_id = str(uuid.uuid4())
                meta = _default_agent_meta(group_id, name=name, description=description)
                meta.update(
                    {
                        "title": name,
                        "origin": "user",
                        "isGroup": True,
                        "memberIds": member_ids,
                        "groupCreationFingerprint": fingerprint,
                    }
                )
                AGENT_INDEX[group_id] = meta
                TRANSCRIPTS[group_id] = {"generation": 1, "next_seq": 1, "entries": []}
                _persist_state_locked()
                TRANSCRIPT_CHANGED.notify_all()
                meta = dict(meta)

    group = _agent_public(group_id, meta)
    _broadcast_gateway_event(
        "agent-upserted",
        {"agent": group, "activeAgentId": group_id},
    )
    return {"agent": group}


def _set_gateway_group_members(req: dict) -> dict:
    """Replace the member roster of one durable local group channel."""
    group_id = _text_field(req.get("id"), _text_field(req.get("agentId")))
    if not group_id:
        raise GatewayContractError("INVALID_AGENT_ID", "id must not be empty")
    raw_members = req.get("memberAgentIds")
    if raw_members is None:
        # A few early gateway callers used the roster-facing spelling. Keep
        # this alias harmlessly compatible while retaining one canonical wire
        # representation in responses and persistence.
        raw_members = req.get("memberIds")
    member_ids = _normalize_group_member_ids(raw_members)

    with TRANSCRIPT_CHANGED:
        group = AGENT_INDEX.get(group_id)
        if group is None:
            raise GatewayContractError("UNKNOWN_AGENT", "agent id does not exist", agentId=group_id)
        if not group.get("isGroup"):
            raise GatewayContractError("NOT_A_GROUP", "setGroupMembers requires a group channel", agentId=group_id)
        _validate_group_member_ids_locked(member_ids)
        changed = list(group.get("memberIds", [])) != member_ids
        if changed:
            group["memberIds"] = list(member_ids)
            group["updatedAtMs"] = int(time.time() * 1000)
            _persist_state_locked()
            TRANSCRIPT_CHANGED.notify_all()
        public_meta = dict(group)

    public = _agent_public(group_id, public_meta)

    # A complete roster event is understood by the 0.30 coordinator without
    # requiring a synthetic ordering cursor (unlike agent-upserted events).
    if changed:
        with TRANSCRIPT_CHANGED:
            roster_snapshot = [(aid, dict(meta)) for aid, meta in AGENT_INDEX.items()]
        agents = [_agent_public(aid, meta) for aid, meta in roster_snapshot]
        _broadcast_gateway_event("agents", {"agents": agents, "activeAgentId": group_id})
    return {"agent": public}


def _update_gateway_agent(req: dict) -> dict:
    """Apply the editable profile subset accepted by the 0.30 roster API."""
    agent_id = _text_field(req.get("id"), _text_field(req.get("agentId")))
    if not agent_id:
        raise GatewayContractError("INVALID_AGENT_ID", "id must not be empty")
    profile = req.get("profile")
    if profile is None:
        # Be tolerant of direct-field callers while keeping profile the
        # canonical request shape emitted by the 0.30 renderer.
        profile = req
    if not isinstance(profile, dict):
        raise GatewayContractError("INVALID_AGENT_PROFILE", "profile must be an object")

    with TRANSCRIPT_CHANGED:
        stored = AGENT_INDEX.get(agent_id)
        if stored is None:
            raise GatewayContractError("UNKNOWN_AGENT", "agent id does not exist", agentId=agent_id)
        updates = {}
        if "name" in profile:
            name = _text_field(profile.get("name"))
            if not name:
                raise GatewayContractError("INVALID_AGENT_NAME", "name must not be empty")
            updates["name"] = name
        if "description" in profile:
            description = profile.get("description")
            if not isinstance(description, str):
                raise GatewayContractError("INVALID_AGENT_DESCRIPTION", "description must be a string")
            updates["description"] = description.strip()
        if "title" in profile:
            title = _text_field(profile.get("title"))
            if title:
                updates["title"] = title
        for key in ("avatarShape", "avatarColor"):
            if key in profile:
                value = profile.get(key)
                if not isinstance(value, str):
                    raise GatewayContractError("INVALID_AGENT_PROFILE", f"{key} must be a string")
                updates[key] = value.strip()

        changed = False
        for key, value in updates.items():
            if stored.get(key) != value:
                stored[key] = value
                changed = True
        if changed:
            stored["updatedAtMs"] = int(time.time() * 1000)
            _persist_state_locked()
            TRANSCRIPT_CHANGED.notify_all()
        public_meta = dict(stored)

    public = _agent_public(agent_id, public_meta)

    if changed:
        with TRANSCRIPT_CHANGED:
            roster_snapshot = [(aid, dict(meta)) for aid, meta in AGENT_INDEX.items()]
        agents = [_agent_public(aid, meta) for aid, meta in roster_snapshot]
        _broadcast_gateway_event("agents", {"agents": agents, "activeAgentId": agent_id})
    return {"agent": public}


def _delete_gateway_agents(req: dict) -> dict:
    """Delete a batch of local agents without leaving dangling group rosters."""
    value = req.get("ids")
    if value is None:
        value = req.get("agentIds")
    if value is None:
        raise GatewayContractError("INVALID_AGENT_IDS", "ids must be an array of agent ids")
    if not isinstance(value, list):
        raise GatewayContractError("INVALID_AGENT_IDS", "ids must be an array of agent ids")
    ids = []
    seen = set()
    for raw_id in value:
        agent_id = _text_field(raw_id)
        if not agent_id:
            raise GatewayContractError("INVALID_AGENT_IDS", "ids must contain only non-empty agent ids")
        if agent_id not in seen:
            seen.add(agent_id)
            ids.append(agent_id)
    if not ids:
        return {"deletedIds": [], "agents": []}

    with TRANSCRIPT_CHANGED:
        unknown = [agent_id for agent_id in ids if agent_id not in AGENT_INDEX]
        if unknown:
            raise GatewayContractError("UNKNOWN_AGENT", "ids contains an unknown agent id", agentId=unknown[0])
        if "bridge-agent-local" in ids:
            raise GatewayContractError(
                "PROTECTED_AGENT",
                "the local bridge agent cannot be deleted",
                agentId="bridge-agent-local",
            )
        deleted_groups = {agent_id for agent_id in ids if AGENT_INDEX[agent_id].get("isGroup")}
        in_use = []
        for candidate in ids:
            if candidate in deleted_groups:
                continue
            owners = [
                group_id
                for group_id, meta in AGENT_INDEX.items()
                if meta.get("isGroup") and group_id not in deleted_groups and candidate in meta.get("memberIds", [])
            ]
            if owners:
                in_use.append((candidate, owners[0]))
        if in_use:
            candidate, owner = in_use[0]
            raise GatewayContractError(
                "AGENT_IN_USE",
                "agent is still a member of a group channel",
                agentId=candidate,
                groupId=owner,
            )

        for agent_id in ids:
            AGENT_INDEX.pop(agent_id, None)
            TRANSCRIPTS.pop(agent_id, None)
            for key in [key for key in PROMPT_ACCEPTANCE if key[0] == agent_id]:
                PROMPT_ACCEPTANCE.pop(key, None)
        _persist_state_locked()
        TRANSCRIPT_CHANGED.notify_all()

    # Stop per-agent workers and remove avatar files after durable state has
    # committed; these operations are best-effort cleanup and never roll back
    # a successful metadata deletion.
    with AGENT_EXECUTORS_LOCK:
        executors = [AGENT_EXECUTORS.pop(agent_id, None) for agent_id in ids]
    for executor in executors:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
    for agent_id in ids:
        try:
            avatar_path = _avatar_path(agent_id)
            if os.path.exists(avatar_path):
                os.unlink(avatar_path)
        except OSError:
            pass

    with TRANSCRIPT_CHANGED:
        roster_snapshot = [(aid, dict(meta)) for aid, meta in AGENT_INDEX.items()]
    agents = [_agent_public(aid, meta) for aid, meta in roster_snapshot]
    _broadcast_gateway_event("agents", {"agents": agents, "activeAgentId": None})
    return {"deletedIds": ids, "agents": agents}


def _avatar_path(agent_id: str) -> str:
    digest = hashlib.sha256(str(agent_id).encode("utf-8")).hexdigest()
    return os.path.join(AVATAR_DIR, f"{digest}.png")


def _decode_avatar_bytes(value) -> bytes | None:
    """Decode the PNG payload accepted by the 0.30 avatar command.

    ``null`` and an empty string clear the avatar.  The renderer sends raw
    base64, but accepting a data URL keeps the local gateway tolerant of
    callers that use the browser representation directly.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("pngBase64 must be a base64 string or null")
    encoded = value.strip()
    if not encoded:
        return None
    if encoded.lower().startswith("data:"):
        prefix, separator, encoded = encoded.partition(",")
        if not separator or "base64" not in prefix.lower():
            raise ValueError("pngBase64 data URL must use base64 encoding")
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError, base64.binascii.Error) as exc:
        raise ValueError("pngBase64 is not valid base64") from exc
    if len(decoded) > MAX_AVATAR_BYTES:
        raise ValueError(f"avatar exceeds {MAX_AVATAR_BYTES} bytes")
    if not decoded.startswith(_PNG_SIGNATURE):
        raise ValueError("avatar must be a PNG image")
    return decoded


def _find_agent_by_creation_nonce(client_nonce: str) -> str:
    if not client_nonce:
        return ""
    with TRANSCRIPT_CHANGED:
        for agent_id, meta in AGENT_INDEX.items():
            if meta.get("creationClientNonce") == client_nonce:
                return agent_id
    return ""


def _broadcast_gateway_event(channel: str, payload: dict) -> None:
    event = {"channel": channel, "payload": payload}
    with LOCK:
        subscribers = list(SSE_SUBSCRIBERS.items())
    for handler, write_lock in subscribers:
        try:
            with write_lock:
                handler._sse_write_unlocked(event)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            with LOCK:
                SSE_SUBSCRIBERS.pop(handler, None)


def _create_gateway_agent(req: dict) -> dict:
    """Create or replay one local agent using the 0.30 gateway contract."""
    with AGENT_CREATE_LOCK:
        client_nonce = _text_field(req.get("clientNonce"))
        agent_id = _text_field(req.get("agentId"))
        replay_agent_id = _find_agent_by_creation_nonce(client_nonce)
        if replay_agent_id:
            # createAgentWithRetry reuses the same nonce, but a retry may carry
            # a reduced payload. Return the original durable record verbatim,
            # rather than treating omitted optional fields as new defaults.
            # The nonce is the idempotency key and therefore outranks a stale
            # or mismatched caller-supplied agent id.
            agent_id = replay_agent_id
            with TRANSCRIPT_CHANGED:
                meta = dict(AGENT_INDEX[agent_id])
            agent = _agent_public(agent_id, meta)
            _broadcast_gateway_event(
                "agent-upserted",
                {"agent": agent, "activeAgentId": agent_id},
            )
            return {"agent": agent}
        if not agent_id:
            agent_id = str(uuid.uuid4())

        name = _text_field(req.get("name"), "Bridge Bot")
        description = _text_field(req.get("description"), "Local Ollama-backed Grok Bot")
        ensure_agent(agent_id, name=name, description=description)
        changed = False
        with TRANSCRIPT_CHANGED:
            stored = AGENT_INDEX[agent_id]
            updates = {
                "name": name,
                "description": description,
                "title": _text_field(req.get("title"), name),
                "avatarShape": _text_field(req.get("avatarShape")),
                "avatarColor": _text_field(req.get("avatarColor")),
                # The gateway createAgent contract is a user-facing create
                # operation. `local` is reserved for the synthetic bridge
                # agent in the internal roster, not for new GUI-created bots.
                "origin": _text_field(req.get("origin"), "user"),
                "purpose": _text_field(req.get("purpose")),
                "kickstartRequested": bool(req.get("isKickstartRequested", False)),
                "introductionSuppressed": bool(req.get("isIntroductionSuppressed", False)),
            }
            if client_nonce:
                updates["creationClientNonce"] = client_nonce
            for key, value in updates.items():
                if stored.get(key) != value:
                    stored[key] = value
                    changed = True
            if changed:
                stored["updatedAtMs"] = int(time.time() * 1000)
                _persist_state_locked()
                TRANSCRIPT_CHANGED.notify_all()
            meta = dict(stored)

    agent = _agent_public(agent_id, meta)
    _broadcast_gateway_event(
        "agent-upserted",
        {"agent": agent, "activeAgentId": agent_id},
    )
    return {"agent": agent}


def _set_gateway_agent_avatar(agent_id: str, value) -> tuple[dict, str]:
    decoded = _decode_avatar_bytes(value)
    ensure_agent(agent_id)
    path = _avatar_path(agent_id)
    if decoded is None:
        try:
            if os.path.exists(path):
                os.unlink(path)
        except OSError as exc:
            raise ValueError(f"could not clear avatar: {exc}") from exc
        version = ""
    else:
        os.makedirs(AVATAR_DIR, exist_ok=True)
        temp_path = f"{path}.{os.getpid()}.tmp"
        try:
            with open(temp_path, "wb") as handle:
                handle.write(decoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        except OSError as exc:
            try:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            except OSError:
                pass
            raise ValueError(f"could not persist avatar: {exc}") from exc
        version = hashlib.sha256(decoded).hexdigest()

    with TRANSCRIPT_CHANGED:
        meta = AGENT_INDEX[agent_id]
        meta["avatarVersion"] = version
        meta["updatedAtMs"] = int(time.time() * 1000)
        _persist_state_locked()
        TRANSCRIPT_CHANGED.notify_all()
        public_meta = dict(meta)
    public = _agent_public(agent_id, public_meta)
    _broadcast_gateway_event(
        "agent-upserted",
        {"agent": public, "activeAgentId": agent_id},
    )
    return public, version


def _gateway_agent_avatar(agent_id: str) -> dict:
    ensure_agent(agent_id)
    with TRANSCRIPT_CHANGED:
        meta = dict(AGENT_INDEX.get(agent_id, {}))
    version = _text_field(meta.get("avatarVersion"))
    path = _avatar_path(agent_id)
    if not version or not os.path.exists(path):
        return {"dataUrl": None, "version": None}
    try:
        with open(path, "rb") as handle:
            encoded = base64.b64encode(handle.read()).decode("ascii")
    except OSError:
        return {"dataUrl": None, "version": None}
    return {"dataUrl": f"data:image/png;base64,{encoded}", "version": version}


def _first_agent_id(fields) -> str:
    """Read a restored agent id from either JSON fields or parse_qs output."""
    for key in ("id", "agentId", "agent_id"):
        value = fields.get(key, "") if hasattr(fields, "get") else ""
        if isinstance(value, (list, tuple)):
            value = value[0] if value else ""
        value = str(value or "").strip()
        if value:
            return value
    return ""


def _connect_frame(obj: dict) -> bytes:
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return struct.pack(">BI", 0, len(payload)) + payload


_MACHINE_ID_SUFFIX = re.compile(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")


def _observe_sand_machine(headers, request: dict | None = None) -> str:
    """Bind the bridge roster to the desktop machine id on the current call."""
    request = request or {}
    machine_id = str(request.get("machineId", request.get("machine_id", ""))).strip()
    if not machine_id:
        checksum = str(headers.get("x-cursor-checksum", ""))
        match = _MACHINE_ID_SUFFIX.search(checksum)
        if match is not None:
            machine_id = match.group(1)
    if machine_id:
        SAND_MACHINE["machineId"] = machine_id
    return SAND_MACHINE["machineId"]


def _decode_connect_json(body: bytes) -> dict:
    """Decode the first Connect JSON request frame, tolerating plain JSON probes."""
    if not body:
        return {}
    if body[:1] == b"{" or body[:1] == b"[":
        try:
            value = json.loads(body.decode("utf-8"))
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    if len(body) >= 5:
        flags = body[0]
        length = struct.unpack(">I", body[1:5])[0]
        if flags == 0 and length <= len(body) - 5:
            try:
                value = json.loads(body[5 : 5 + length].decode("utf-8"))
                return value if isinstance(value, dict) else {}
            except Exception:
                return {}
    return {}


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift >= 70:
            raise ValueError("protobuf varint is too long")
    raise ValueError("truncated protobuf varint")


def _varint(value: int) -> bytes:
    value = int(value)
    if value < 0:
        value &= (1 << 64) - 1
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _proto_fields(data: bytes):
    """Yield (field_number, wire_type, value) for a protobuf message."""
    offset = 0
    while offset < len(data):
        key, offset = _read_varint(data, offset)
        number, wire = key >> 3, key & 0x07
        if number <= 0:
            raise ValueError("invalid protobuf field number")
        if wire == 0:
            value, offset = _read_varint(data, offset)
        elif wire == 1:
            if offset + 8 > len(data):
                raise ValueError("truncated protobuf fixed64")
            value = int.from_bytes(data[offset : offset + 8], "little")
            offset += 8
        elif wire == 2:
            length, offset = _read_varint(data, offset)
            end = offset + length
            if end > len(data):
                raise ValueError("truncated protobuf bytes")
            value = data[offset:end]
            offset = end
        elif wire == 5:
            if offset + 4 > len(data):
                raise ValueError("truncated protobuf fixed32")
            value = int.from_bytes(data[offset : offset + 4], "little")
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        yield number, wire, value


def _proto_schema_field(
    name: str,
    kind: str,
    *,
    repeated: bool = False,
    schema=None,
    enum=None,
    emit_empty: bool = False,
):
    return {
        "name": name,
        "kind": kind,
        "repeated": repeated,
        "schema": schema,
        "enum": enum or {},
        "emit_empty": emit_empty,
    }


def _proto_decode_message(data: bytes, schema: dict) -> dict:
    result = {}
    by_number = schema
    for number, wire, raw in _proto_fields(data):
        field = by_number.get(number)
        if field is None:
            continue
        kind = field["kind"]
        if kind in ("str", "bytes", "msg") and wire != 2:
            continue
        if kind in ("int", "enum", "bool") and wire != 0:
            continue
        if kind == "str":
            value = raw.decode("utf-8", errors="replace")
        elif kind == "bytes":
            value = bytes(raw)
        elif kind == "msg":
            value = _proto_decode_message(raw, field["schema"])
        elif kind == "bool":
            value = bool(raw)
        elif kind == "enum":
            value = int(raw)
        else:
            value = int(raw)
        name = field["name"]
        if field["repeated"]:
            result.setdefault(name, []).append(value)
        else:
            result[name] = value
    return result


def _proto_encode_message(obj: dict | None, schema: dict) -> bytes:
    obj = obj or {}
    out = bytearray()
    for number, field in schema.items():
        name = field["name"]
        value = obj.get(name)
        if value is None:
            continue
        values = value if field["repeated"] else [value]
        if not isinstance(values, (list, tuple)):
            values = [values]
        for item in values:
            kind = field["kind"]
            if kind in ("int", "enum", "bool"):
                if kind == "bool":
                    if not item:
                        continue
                    encoded = _varint(1)
                elif kind == "enum":
                    if isinstance(item, str):
                        item = field["enum"].get(item, 0)
                    if not item:
                        continue
                    encoded = _varint(int(item))
                else:
                    if not item:
                        continue
                    encoded = _varint(int(item))
                out.extend(_varint(number << 3))
                out.extend(encoded)
                continue
            if kind == "str":
                if item == "":
                    continue
                encoded = str(item).encode("utf-8")
            elif kind == "bytes":
                if isinstance(item, str):
                    try:
                        encoded = base64.b64decode(item)
                    except Exception:
                        encoded = item.encode("utf-8")
                else:
                    encoded = bytes(item)
                if not encoded:
                    continue
            elif kind == "msg":
                encoded = _proto_encode_message(item, field["schema"])
                if not encoded and not field.get("emit_empty"):
                    continue
            else:
                continue
            out.extend(_varint((number << 3) | 2))
            out.extend(_varint(len(encoded)))
            out.extend(encoded)
    return bytes(out)


def _decode_connect_proto(body: bytes, *, streamed: bool = False, schema: dict | None = None) -> dict:
    payload = body
    if streamed:
        if len(body) < 5:
            return {}
        flags = body[0]
        length = struct.unpack(">I", body[1:5])[0]
        if flags & 0x01 or length > len(body) - 5:
            return {}
        payload = body[5 : 5 + length]
    try:
        return _proto_decode_message(payload, schema or {})
    except (ValueError, UnicodeError):
        return {}


def _connect_envelope(payload: bytes, flags: int = 0) -> bytes:
    return bytes([flags]) + struct.pack(">I", len(payload)) + payload


def _is_proto_content_type(content_type: str) -> bool:
    return "application/proto" in content_type.lower() or "application/connect+proto" in content_type.lower()


# The native 0.30 client uses protobuf by default. Keep the schema table small and
# explicit, covering the identity, agent and transcript RPCs needed for local chat.
_ENUM_MODE = {"MODE_OFF": 1, "MODE_SHADOW": 2, "MODE_LIVE": 3, "MODE_LOCAL": 4, "MODE_BOX": 4}
_ENUM_DELIVERY = {"DELIVERY_ACCEPTED_BOX": 1, "ACCEPTED_BOX": 1, "DELIVERY_ACCEPTED_TEMPORAL": 2}
_ENUM_AGENT_HARNESS = {
    "GROK_BOT_AGENT_HARNESS_KIND_UNSPECIFIED": 0,
    "GROK_BOT_AGENT_HARNESS_KIND_BOX": 1,
    "GROK_BOT_AGENT_HARNESS_KIND_TEMPORAL": 2,
    "BOX": 1,
    "TEMPORAL": 2,
}

_PROTO_TRANSCRIPT_ENTRY = {
    1: _proto_schema_field("seq", "int"),
    2: _proto_schema_field("entryKind", "str"),
    3: _proto_schema_field("body", "bytes"),
    4: _proto_schema_field("blobHash", "str"),
    5: _proto_schema_field("updatedSeq", "int"),
    6: _proto_schema_field("entryId", "str"),
    7: _proto_schema_field("bodyOmitted", "bool"),
}
_PROTO_TRANSCRIPT_CURSOR = {
    1: _proto_schema_field("agentId", "str"),
    2: _proto_schema_field("generation", "int"),
    3: _proto_schema_field("afterUpdatedSeq", "int"),
}
_PROTO_COMPUTER_CAPABILITIES = {
    1: _proto_schema_field("messagesOp", "bool"),
    2: _proto_schema_field("messagesOpGeneration", "int"),
}
_PROTO_COMPUTER_HELLO = {
    1: _proto_schema_field("label", "str"),
    2: _proto_schema_field("localRoot", "str"),
    3: _proto_schema_field("terminalsFolder", "str"),
    4: _proto_schema_field("standing", "str"),
    5: _proto_schema_field("supervised", "bool"),
    6: _proto_schema_field("variant", "str"),
    7: _proto_schema_field("serverAuthoritative", "bool"),
    8: _proto_schema_field("capabilities", "msg", schema=_PROTO_COMPUTER_CAPABILITIES),
}
_PROTO_COMPUTER_WATCH_EVENT = {
    1: _proto_schema_field(
        "connected",
        "msg",
        schema={1: _proto_schema_field("pendingRequestCount", "int")},
        emit_empty=True,
    ),
    2: _proto_schema_field("notify", "msg", schema={}, emit_empty=True),
    3: _proto_schema_field(
        "heartbeat",
        "msg",
        schema={1: _proto_schema_field("pendingRequestCount", "int")},
        emit_empty=True,
    ),
}
_PROTO_AGENT = {
    1: _proto_schema_field("id", "str"),
    2: _proto_schema_field("legacyAgentId", "str"),
    3: _proto_schema_field("name", "str"),
    4: _proto_schema_field("description", "str"),
    5: _proto_schema_field("title", "str"),
    6: _proto_schema_field("avatarShape", "str"),
    7: _proto_schema_field("avatarColor", "str"),
    8: _proto_schema_field("avatarVersion", "str"),
    9: _proto_schema_field("avatarUrl", "str"),
    10: _proto_schema_field("createdAtMs", "int"),
    11: _proto_schema_field("updatedAtMs", "int"),
    12: _proto_schema_field("agentId", "str"),
    13: _proto_schema_field("harness", "str"),
    14: _proto_schema_field("role", "str"),
    15: _proto_schema_field("visibility", "enum"),
    16: _proto_schema_field("teamId", "int"),
    17: _proto_schema_field("viewerIsOwner", "bool"),
}
_PROTO_SAND_MACHINE = {
    1: _proto_schema_field("machineId", "str"),
    2: _proto_schema_field("label", "str"),
    3: _proto_schema_field("localToolPermission", "str"),
}
_PROTO_WATCH_FRAME = {
    1: _proto_schema_field(
        "connected", "msg", schema={
            1: _proto_schema_field("streamId", "str"),
            2: _proto_schema_field("serverTimeMs", "int"),
            3: _proto_schema_field("absoluteLifetimeMs", "int"),
        }
    ),
    2: _proto_schema_field(
        "rows", "msg", schema={
            1: _proto_schema_field("agentId", "str"),
            2: _proto_schema_field("generation", "int"),
            3: _proto_schema_field("entries", "msg", repeated=True, schema=_PROTO_TRANSCRIPT_ENTRY),
            4: _proto_schema_field("deletes", "msg", repeated=True, schema={
                1: _proto_schema_field("seq", "int"),
                2: _proto_schema_field("updatedSeq", "int"),
                3: _proto_schema_field("entryId", "str"),
            }),
            5: _proto_schema_field("replay", "bool"),
        }
    ),
    5: _proto_schema_field("heartbeat", "msg", schema={1: _proto_schema_field("serverTimeMs", "int")}),
}

_PROTO_SCHEMAS = {
    "send_req": {
        1: _proto_schema_field("agentId", "str"), 2: _proto_schema_field("messageId", "str"),
        3: _proto_schema_field("text", "str"), 4: _proto_schema_field("sentAtMs", "int"),
        5: _proto_schema_field("richText", "str"), 6: _proto_schema_field("replyToId", "str"),
        7: _proto_schema_field("isFork", "bool"), 8: _proto_schema_field("attachmentPaths", "str", repeated=True),
        9: _proto_schema_field("attachmentNames", "str", repeated=True), 10: _proto_schema_field("traceparent", "str"),
        11: _proto_schema_field("enterEpochMs", "int"), 12: _proto_schema_field("composedAtMs", "int"),
        13: _proto_schema_field("source", "enum"),
    },
    "send_resp": {
        1: _proto_schema_field("dispatched", "bool"), 2: _proto_schema_field("mode", "enum", enum=_ENUM_MODE),
        3: _proto_schema_field("workflowId", "str"), 4: _proto_schema_field("delivery", "enum", enum=_ENUM_DELIVERY),
    },
    "send_status_req": {
        1: _proto_schema_field("agentId", "str"),
        2: _proto_schema_field("messageId", "str"),
    },
    "send_status_resp": {
        1: _proto_schema_field(
            "status",
            "enum",
            enum={
                "GROK_BOT_SEND_STATUS_UNSPECIFIED": 0,
                "GROK_BOT_SEND_STATUS_NOT_FOUND": 1,
                "GROK_BOT_SEND_STATUS_ACCEPTED": 2,
                "GROK_BOT_SEND_STATUS_REJECTED": 3,
                "GROK_BOT_SEND_STATUS_PENDING": 4,
                "GROK_BOT_SEND_STATUS_UNKNOWN_DURABILITY": 5,
                "STATUS_UNSPECIFIED": 0,
                "STATUS_NOT_FOUND": 1,
                "STATUS_ACCEPTED": 2,
                "STATUS_REJECTED": 3,
                "STATUS_PENDING": 4,
                "STATUS_UNKNOWN_DURABILITY": 5,
            },
        ),
        2: _proto_schema_field("echoEntryId", "str"),
        3: _proto_schema_field("rejectionCode", "str"),
        4: _proto_schema_field("acceptedAtMs", "int"),
    },
    "watch_req": {
        1: _proto_schema_field("cursors", "msg", repeated=True, schema=_PROTO_TRANSCRIPT_CURSOR),
        2: _proto_schema_field("includeUnlistedAgents", "bool"), 3: _proto_schema_field("inlineBodyMaxBytes", "int"),
    },
    "computer_watch_req": {
        1: _proto_schema_field("machineId", "str"),
        2: _proto_schema_field("credential", "str"),
        3: _proto_schema_field("hello", "msg", schema=_PROTO_COMPUTER_HELLO),
    },
    "register_machine_req": {
        1: _proto_schema_field("label", "str"),
        2: _proto_schema_field("localToolPermission", "str"),
    },
    "update_machine_label_req": {
        1: _proto_schema_field("machineId", "str"),
        2: _proto_schema_field("label", "str"),
    },
    "update_machine_permission_req": {
        1: _proto_schema_field("machineId", "str"),
        2: _proto_schema_field("localToolPermission", "str"),
    },
    "list_entries_req": {
        1: _proto_schema_field("agentId", "str"), 2: _proto_schema_field("generation", "int"),
        3: _proto_schema_field("beforeSeq", "int"), 4: _proto_schema_field("limit", "int"),
    },
    "list_entries_resp": {
        1: _proto_schema_field("entries", "msg", repeated=True, schema=_PROTO_TRANSCRIPT_ENTRY),
        2: _proto_schema_field("generation", "int"),
    },
    "list_agents_resp": {1: _proto_schema_field("agents", "msg", repeated=True, schema=_PROTO_AGENT)},
    "create_agent_req": {
        1: _proto_schema_field("legacyAgentId", "str"), 2: _proto_schema_field("name", "str"),
        3: _proto_schema_field("description", "str"), 4: _proto_schema_field("title", "str"),
        5: _proto_schema_field("avatarShape", "str"), 6: _proto_schema_field("avatarColor", "str"),
        7: _proto_schema_field("avatarDataUrl", "str"), 8: _proto_schema_field("agentId", "str"),
        9: _proto_schema_field("harness", "enum"), 10: _proto_schema_field("kickstartRequested", "bool"),
        11: _proto_schema_field("introductionSuppressed", "bool"), 12: _proto_schema_field("purpose", "str"),
        13: _proto_schema_field("origin", "str"),
    },
    "create_agent_resp": {
        1: _proto_schema_field("agent", "msg", schema=_PROTO_AGENT), 2: _proto_schema_field("harness", "enum"),
    },
    "get_me_resp": {
        1: _proto_schema_field("authId", "str"), 2: _proto_schema_field("userId", "int"), 3: _proto_schema_field("email", "str"),
        4: _proto_schema_field("firstName", "str"), 5: _proto_schema_field("lastName", "str"),
        6: _proto_schema_field("workosId", "str"), 7: _proto_schema_field("teamId", "int"), 8: _proto_schema_field("createdAt", "str"),
        9: _proto_schema_field("isEnterpriseUser", "bool"), 10: _proto_schema_field("teamName", "str"),
        11: _proto_schema_field("emailDomainType", "str"), 12: _proto_schema_field("country", "str"),
        13: _proto_schema_field("profilePictureUrl", "str"), 14: _proto_schema_field("cursorReviewOnboardingUseCursorGithubApp", "bool"),
        15: _proto_schema_field("organizationId", "int"), 16: _proto_schema_field("organizationPublicId", "str"),
        17: _proto_schema_field("isTeamAdmin", "bool"), 18: _proto_schema_field("hasActiveMobileSession", "bool"),
        19: _proto_schema_field("publicUserId", "str"),
    },
    "privacy_resp": {
        1: _proto_schema_field("privacyMode", "enum"), 2: _proto_schema_field("hoursRemainingInGracePeriod", "int"),
        3: _proto_schema_field("isEnforcedByTeam", "bool"), 4: _proto_schema_field("isNotMigratedToServerSourceOfTruth", "bool"),
        5: _proto_schema_field("partnerDataShare", "bool"), 6: _proto_schema_field("hasAcknowledgedGracePeriodDisclaimer", "bool"),
    },
    "access_resp": {
        1: _proto_schema_field("state", "enum"), 2: _proto_schema_field("purchaseChannel", "enum"),
        3: _proto_schema_field("blockReason", "enum"), 4: _proto_schema_field("purchasableTiers", "str", repeated=True),
        6: _proto_schema_field("isPaidTrialPlan", "bool"), 7: _proto_schema_field("unpaidAdminNeedsPaidSeat", "bool"),
        9: _proto_schema_field("privacyDisclaimerRequired", "bool"), 10: _proto_schema_field("canSkipOnboarding", "bool"),
        11: _proto_schema_field("proAndSuperGrokPlansGrantAccess", "bool"),
    },
    "trial_resp": {1: _proto_schema_field("status", "enum")},
    "hard_limit_resp": {
        1: _proto_schema_field("hardLimit", "int"), 2: _proto_schema_field("noUsageBasedAllowed", "bool"),
        3: _proto_schema_field("hardLimitPerUser", "int"), 4: _proto_schema_field("perUserMonthlyLimitDollars", "int"),
        5: _proto_schema_field("isDynamicTeamLimit", "bool"), 6: _proto_schema_field("hasPendingSetupOnboardingCreditClaim", "bool"),
        7: _proto_schema_field("isSetupPromoActive", "bool"), 8: _proto_schema_field("setupPromoAmountCents", "int"),
        9: _proto_schema_field("setupPromoCreditValidityDays", "int"), 10: _proto_schema_field("onDemandSpendDisabledByOrganization", "bool"),
    },
    "ensure_resp": {
        1: _proto_schema_field("cluster", "str"), 2: _proto_schema_field("tenantId", "str"), 3: _proto_schema_field("podId", "str"),
        4: _proto_schema_field("networkToken", "str"), 5: _proto_schema_field("execDaemonAuthToken", "str"), 6: _proto_schema_field("execDaemonUrl", "str"),
        7: _proto_schema_field("vncUrl", "str"), 8: _proto_schema_field("terminalsFolder", "str"), 9: _proto_schema_field("imageUpdateAvailable", "bool"),
        10: _proto_schema_field("gatewayUrl", "str"), 11: _proto_schema_field("gatewayToken", "str"), 12: _proto_schema_field("forkVncBaseUrl", "str"),
    },
    "run_state_resp": {1: _proto_schema_field("state", "enum"), 2: _proto_schema_field("imageUpdateAvailable", "bool")},
    "list_boxes_resp": {1: _proto_schema_field("boxes", "msg", repeated=True, schema={1: _proto_schema_field("running", "bool")})},
    "runtime_caps_resp": {
        1: _proto_schema_field("capabilities", "msg", schema={
            1: _proto_schema_field("durableIdentityEnabled", "bool"), 2: _proto_schema_field("durableIdentityWritesEnabled", "bool"),
            3: _proto_schema_field("temporalCreationEnabled", "bool"), 4: _proto_schema_field("agentMessagingEnabled", "bool"),
        })
    },
    "issue_credential_resp": {
        1: _proto_schema_field("credential", "str"), 2: _proto_schema_field("expiresAtMs", "int"), 3: _proto_schema_field("serverAuthoritative", "bool"),
    },
    "migration_event": {
        1: _proto_schema_field("phase", "enum"), 2: _proto_schema_field("detail", "str"), 3: _proto_schema_field("atMs", "int"),
        4: _proto_schema_field("offsetKey", "str"), 5: _proto_schema_field("operationId", "str"),
    },
    "list_machines_resp": {
        1: _proto_schema_field("machines", "msg", repeated=True, schema=_PROTO_SAND_MACHINE),
    },
    "update_machine_resp": {
        1: _proto_schema_field("machine", "msg", schema=_PROTO_SAND_MACHINE),
    },
}


def _request_schema_for_path(path: str):
    if path.endswith("/SendGrokBotUserMessage"):
        return _PROTO_SCHEMAS["send_req"]
    if path.endswith("/WatchGrokBotTranscripts"):
        return _PROTO_SCHEMAS["watch_req"]
    if path.endswith("/WatchGrokBotUserComputerRequests"):
        return _PROTO_SCHEMAS["computer_watch_req"]
    if path.endswith("/ListGrokBotTranscriptEntries"):
        return _PROTO_SCHEMAS["list_entries_req"]
    if path.endswith("/CreateGrokBotAgent") or path.endswith("/CreateGrokBotTemporalAgent"):
        return _PROTO_SCHEMAS["create_agent_req"]
    if path.endswith("/IssueGrokBotUserComputerCredential"):
        return {1: _proto_schema_field("machineId", "str")}
    if path.endswith("/RegisterSandMachine"):
        return _PROTO_SCHEMAS["register_machine_req"]
    if path.endswith("/UpdateSandMachineLabel"):
        return _PROTO_SCHEMAS["update_machine_label_req"]
    if path.endswith("/UpdateSandMachineLocalToolPermission"):
        return _PROTO_SCHEMAS["update_machine_permission_req"]
    if path.endswith("/GetGrokBotSendStatus"):
        return _PROTO_SCHEMAS["send_status_req"]
    return {}


def _response_schema_for_path(path: str):
    suffix_map = {
        "/SendGrokBotUserMessage": "send_resp", "/ListGrokBotTranscriptEntries": "list_entries_resp",
        "/ListGrokBotAgents": "list_agents_resp", "/CreateGrokBotAgent": "create_agent_resp",
        "/CreateGrokBotTemporalAgent": "create_agent_resp", "/GetMe": "get_me_resp", "/GetUserPrivacyMode": "privacy_resp",
        "/GetSandAccessStatus": "access_resp", "/GetSandTrialClaimStatus": "trial_resp", "/GetHardLimit": "hard_limit_resp",
        "/EnsureSandBox": "ensure_resp", "/EnsureSandBoxWindow": "ensure_resp", "/GetSandBoxRunState": "run_state_resp",
        "/ListSandBoxes": "list_boxes_resp", "/GetGrokBotRuntimeCapabilities": "runtime_caps_resp",
        "/IssueGrokBotUserComputerCredential": "issue_credential_resp", "/WatchSandBoxMigration": "migration_event",
        "/ListSandMachines": "list_machines_resp", "/UpdateSandMachineLabel": "update_machine_resp",
        "/UpdateSandMachineLocalToolPermission": "update_machine_resp",
        "/GetGrokBotSendStatus": "send_status_resp",
    }
    for suffix, name in suffix_map.items():
        if path.endswith(suffix):
            return _PROTO_SCHEMAS[name]
    return {}


def _proto_response_obj(path: str, obj):
    """Normalize local JSON-shaped objects into protobuf field values."""
    if not isinstance(obj, dict):
        return obj
    if path.endswith("/SendGrokBotUserMessage"):
        return {"dispatched": bool(obj.get("dispatched")), "mode": obj.get("mode", 4), "delivery": obj.get("delivery", 1)}
    if path.endswith("/GetSandBoxRunState"):
        return {"state": {"SAND_BOX_RUN_STATE_RUNNING": 3, "SAND_BOX_RUN_STATE_HIBERNATED": 2, "SAND_BOX_RUN_STATE_ABSENT": 1}.get(obj.get("state"), obj.get("state", 0)), "imageUpdateAvailable": bool(obj.get("imageUpdateAvailable"))}
    if path.endswith("/GetSandAccessStatus"):
        return {**obj, "state": {"GRANTED": 1, "UNAVAILABLE": 2, "PAYMENT_REQUIRED": 3}.get(obj.get("state"), obj.get("state", 0))}
    if path.endswith("/GetSandTrialClaimStatus"):
        return {"status": obj.get("status", 0)}
    if path.endswith("/GetGrokBotSendStatus"):
        status = obj.get("status", 0)
        if isinstance(status, str):
            status = _PROTO_SCHEMAS["send_status_resp"][1]["enum"].get(status, 0)
        return {
            "status": int(status or 0),
            "echoEntryId": obj.get("echoEntryId", obj.get("echo_entry_id", "")),
            "rejectionCode": obj.get("rejectionCode", obj.get("rejection_code", "")),
            "acceptedAtMs": obj.get("acceptedAtMs", obj.get("accepted_at_ms", 0)),
        }
    if path.endswith("/CreateGrokBotAgent") or path.endswith("/CreateGrokBotTemporalAgent"):
        return {"agent": obj.get("agent", {}), "harness": obj.get("harness", 0)}
    if path.endswith("/GetGrokBotRuntimeCapabilities"):
        return {"capabilities": obj.get("capabilities", {})}
    return obj


def _read_body(handler: BaseHTTPRequestHandler) -> bytes:
    transfer_encoding = (handler.headers.get("transfer-encoding") or "").lower()
    if "chunked" in transfer_encoding:
        chunks = []
        while True:
            line = handler.rfile.readline(65537)
            if not line:
                break
            size_text = line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError:
                break
            if size == 0:
                # Consume optional trailer headers and the terminating blank line.
                while True:
                    trailer = handler.rfile.readline(65537)
                    if not trailer or trailer in (b"\r\n", b"\n"):
                        break
                break
            chunk = handler.rfile.read(size)
            if len(chunk) != size:
                break
            chunks.append(chunk)
            # RFC 9112 requires a CRLF after every chunk.
            handler.rfile.read(2)
        return b"".join(chunks)
    length = int(handler.headers.get("content-length") or 0)
    if length > 0:
        return handler.rfile.read(length)
    return b""


def _entry_after(entry: dict, cursor: int) -> bool:
    try:
        return int(entry.get("updatedSeq", entry.get("seq", 0))) > cursor
    except (TypeError, ValueError):
        return False


def _gateway_transcript_entry(agent_id: str, entry: dict) -> dict:
    """Decode one stored transcript row into the gateway transcript-page shape.

    ``ListGrokBotTranscriptEntries`` and ``WatchGrokBotTranscripts`` expose the
    raw protobuf row contract (base64 JSON body plus row metadata).  The legacy
    gateway fallback instead returns decoded transcript entries, which the
    renderer indexes directly by ``entry.id``.
    """
    body_obj = {}
    body = entry.get("body")
    if isinstance(body, str) and body:
        try:
            decoded = base64.b64decode(body, validate=True)
            value = json.loads(decoded.decode("utf-8"))
            if isinstance(value, dict):
                body_obj = dict(value)
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            body_obj = {}

    entry_id = str(body_obj.get("id") or entry.get("entryId") or "").strip()
    if not entry_id:
        # Keep malformed/legacy rows renderable without changing the raw store;
        # sequence numbers are unique within an agent transcript.
        entry_id = f"{agent_id}:{entry.get('seq', entry.get('updatedSeq', ''))}"

    kind = str(body_obj.get("kind") or entry.get("entryKind") or "").strip()
    if kind in {"send_message", "assistant_text"}:
        kind = "message"
    if not kind:
        kind = "message"
    body_obj["kind"] = kind
    body_obj["id"] = entry_id

    if kind == "message":
        role = str(body_obj.get("role") or "").strip()
        body_obj["role"] = role or "assistant"
        content = body_obj.get("content")
        if content is None:
            body_obj["content"] = ""
        elif not isinstance(content, str):
            body_obj["content"] = str(content)
        body_obj.setdefault("isStreaming", False)

    return body_obj


def _transcript_event(agent_id: str, entry: dict, generation: int) -> dict:
    """Build one gateway SSE event consumed by the 0.30 coordinator."""
    sequence = int(entry.get("updatedSeq", entry.get("seq", 0)))
    return {
        "channel": "transcript",
        "payload": {
            "type": "appended",
            "agentId": agent_id,
            "entry": _gateway_transcript_entry(agent_id, entry),
            "ordered": {
                "replicaKey": f"transcript:{agent_id}",
                "epoch": f"{TRANSCRIPT_EPOCH}:{int(generation)}",
                "sequence": sequence,
            },
        },
    }


def _broadcast_transcript_event(agent_id: str, entry: dict, generation: int) -> None:
    """Fan out an appended transcript event to live gateway SSE clients."""
    event = _transcript_event(agent_id, entry, generation)
    with LOCK:
        subscribers = list(SSE_SUBSCRIBERS.items())
    for handler, write_lock in subscribers:
        try:
            with write_lock:
                handler._sse_write_unlocked(event)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            with LOCK:
                SSE_SUBSCRIBERS.pop(handler, None)


def _agent_public(agent_id: str, meta: dict) -> dict:
    """Return the JSON shape used by the 0.30 local gateway roster API."""
    now_ms = int(time.time() * 1000)
    with TRANSCRIPT_CHANGED:
        transcript = TRANSCRIPTS.get(agent_id, {"entries": []})
        entries = transcript.get("entries", [])
        last_entry = dict(entries[-1]) if entries else None
    return {
        "id": agent_id,
        "legacyAgentId": agent_id,
        "agentId": agent_id,
        "name": meta.get("name", "Bridge Bot"),
        "description": meta.get("description", "Local Ollama-backed Grok Bot"),
        "title": meta.get("title", meta.get("name", "Bridge Bot")),
        # The 0.30 renderer validates these roster fields before accepting the
        # array. Keep the local representation compatible with its Xkn guard.
        "path": meta.get("path", agent_id),
        "createdAt": meta.get("createdAt", meta.get("createdAtMs", now_ms)),
        "updatedAt": meta.get("updatedAt", meta.get("updatedAtMs", now_ms)),
        "hasUnread": bool(meta.get("hasUnread", False)),
        "notificationsEnabled": bool(meta.get("notificationsEnabled", True)),
        "notifyOnUpdatesEnabled": bool(meta.get("notifyOnUpdatesEnabled", False)),
        "isGroup": bool(meta.get("isGroup", False)),
        "origin": meta.get("origin", "local"),
        "lastMessageId": meta.get("lastMessageId", last_entry.get("entryId") if last_entry else None),
        "lastEntry": last_entry,
        "awaitingUserResponse": meta.get("awaitingUserResponse"),
        "memberIds": list(meta.get("memberIds", [])),
        "avatarShape": meta.get("avatarShape", ""),
        "avatarColor": meta.get("avatarColor", ""),
        # Keep the roster lightweight.  The renderer fetches the PNG through
        # getAgentAvatar when this version changes.
        "avatarVersion": meta.get("avatarVersion", ""),
        "createdAtMs": meta.get("createdAtMs", now_ms),
        "updatedAtMs": meta.get("updatedAtMs", now_ms),
        "harness": meta.get("harness", "box"),
        "role": meta.get("role", "assistant"),
        "viewerIsOwner": True,
    }


def _transcript_tail(agent_id: str, limit: int, before_seq=None) -> dict:
    """Read the local transcript using the 0.30 gateway command contract."""
    agent_id = str(agent_id or "bridge-agent-local")
    try:
        limit = max(1, min(int(limit or 200), 1000))
    except (TypeError, ValueError):
        limit = 200
    try:
        before = None if before_seq in (None, "") else int(before_seq)
    except (TypeError, ValueError):
        before = None

    # The desktop restores old agent ids before the roster has necessarily been
    # rebuilt. Materialize those ids so a valid empty page is returned instead of
    # the malformed `{}` fallback that made the renderer retry once per second.
    ensure_agent(agent_id)
    with TRANSCRIPT_CHANGED:
        entries = list(TRANSCRIPTS[agent_id]["entries"])
    if before is not None:
        eligible = [entry for entry in entries if int(entry.get("seq", 0)) < before]
    else:
        eligible = entries
    page = eligible[-limit:]
    first_seq = int(page[0].get("seq", 0)) if page else 0
    next_before = first_seq - 1 if first_seq > 1 else None
    return {
        "entries": [_gateway_transcript_entry(agent_id, entry) for entry in page],
        "nextBeforeSeq": next_before,
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle(self):  # noqa: A003
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # A reconnecting renderer may close an idle keepalive socket while
            # BaseHTTPRequestHandler is reading the next request line. That is
            # a normal client lifecycle event, not a backend fault.
            return

    def _request_is_proto(self) -> bool:
        return _is_proto_content_type(self.headers.get("content-type", ""))

    def _request_is_stream_proto(self) -> bool:
        return "application/connect+proto" in self.headers.get("content-type", "").lower()

    def _record(self, body: bytes, status: int | None = None) -> None:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "method": self.command, "path": self.path, "status": status,
                                "body": _redact_log_body(self.path, body)}) + "\n")

    def _reply(self, status: int, obj, *, schema: dict | None = None) -> None:
        proto = self._request_is_proto() and schema is not None
        if proto:
            data = _proto_encode_message(_proto_response_obj(self.path, obj), schema)
        else:
            data = json.dumps(obj if obj is not None else {}, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/proto" if proto else "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            # The 0.30 renderer cancels superseded polling requests while a
            # reconnect is in flight. Treat that as normal disconnect noise;
            # never let socket teardown produce a traceback on stderr.
            return

    def _connect_stream_start(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/connect+proto" if self._request_is_stream_proto() else "application/connect+json")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

    def _connect_stream_write(self, obj: dict, *, schema: dict | None = None) -> None:
        if self._request_is_stream_proto():
            frame_schema = schema or _PROTO_WATCH_FRAME
            if schema is None and self.path.endswith("/WatchGrokBotUserComputerRequests"):
                frame_schema = _PROTO_COMPUTER_WATCH_EVENT
            data = _connect_envelope(_proto_encode_message(_proto_response_obj(self.path, obj), frame_schema))
        else:
            data = _connect_frame(obj)
        self.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _connect_stream_end(self) -> None:
        """Write the Connect end-stream envelope after the final data frame."""
        # Connect uses a JSON EndStreamResponse payload even for proto streams.
        data = _connect_envelope(b"{}", flags=0x02)
        self.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def _sse_start(self) -> None:
        self._sse_write_lock = threading.Lock()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        # Match the 0.18 gateway contract.  The local-exec client treats data
        # frames as request frames, so keepalive must be an SSE comment rather
        # than a synthetic {"kind":"ping"} request it cannot parse.
        self._sse_comment("retry: 1000")

    def _sse_comment(self, line: str) -> None:
        data = f"{line}\n\n".encode("utf-8")
        with self._sse_write_lock:
            self._sse_write_unlocked(data)

    def _sse_write(self, obj: dict) -> None:
        data = f"data: {json.dumps(obj, ensure_ascii=False, separators=(',', ':'))}\n\n".encode("utf-8")
        with self._sse_write_lock:
            self._sse_write_unlocked(data)

    def _sse_write_unlocked(self, data_or_obj) -> None:
        data = data_or_obj
        if isinstance(data_or_obj, dict):
            data = f"data: {json.dumps(data_or_obj, ensure_ascii=False, separators=(',', ':'))}\n\n".encode("utf-8")
        self.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _watch_transcripts(self, body: bytes) -> None:
        request = _decode_connect_proto(body, streamed=True, schema=_PROTO_SCHEMAS["watch_req"]) if self._request_is_stream_proto() else _decode_connect_json(body)
        cursors = request.get("cursors") or []
        include_unlisted = bool(request.get("includeUnlistedAgents", request.get("include_unlisted_agents", False)))
        cursor_map = {}
        for cursor in cursors:
            if not isinstance(cursor, dict):
                continue
            aid = cursor.get("agentId", cursor.get("agent_id", ""))
            if not aid:
                continue
            try:
                after = int(cursor.get("afterUpdatedSeq", cursor.get("after_updated_seq", 0)))
            except (TypeError, ValueError):
                after = 0
            cursor_map[aid] = after

        stream_id = str(uuid.uuid4())
        self._connect_stream_start()
        self._connect_stream_write(
            {
                "connected": {
                    "streamId": stream_id,
                    "serverTimeMs": int(time.time() * 1000),
                    "absoluteLifetimeMs": 3600000,
                }
            }
        )
        log(f"[transcript] watch connected stream={stream_id[:8]} cursors={len(cursor_map)}")

        # A fresh client may not yet have a cursor for a lazily created bridge agent.
        with TRANSCRIPT_CHANGED:
            agents = list(AGENT_INDEX) if include_unlisted else list(cursor_map)
            if not agents and AGENT_INDEX:
                agents = [next(iter(AGENT_INDEX))]
            snapshots = []
            for aid in agents:
                transcript = TRANSCRIPTS.get(aid)
                if transcript is None:
                    continue
                after = cursor_map.get(aid, 0)
                entries = [dict(entry) for entry in transcript["entries"] if _entry_after(entry, after)]
                if entries:
                    snapshots.append((aid, transcript["generation"], entries))
        for aid, generation, entries in snapshots:
            self._connect_stream_write(
                {
                    "rows": {
                        "agentId": aid,
                        "generation": generation,
                        "entries": entries,
                        "deletes": [],
                        "replay": True,
                    }
                }
            )

        # Keep the stream alive and wake it whenever SendGrokBotUserMessage or the
        # local model appends an entry. The heartbeat prevents idle proxies from
        # declaring the connection dead.
        last_heartbeat = time.monotonic()
        watched = set(cursor_map)
        while True:
            with TRANSCRIPT_CHANGED:
                TRANSCRIPT_CHANGED.wait(timeout=15)
                if include_unlisted:
                    watched.update(AGENT_INDEX)
                pending = []
                for aid in list(watched):
                    transcript = TRANSCRIPTS.get(aid)
                    if transcript is None:
                        continue
                    after = cursor_map.get(aid, 0)
                    entries = [dict(entry) for entry in transcript["entries"] if _entry_after(entry, after)]
                    if entries:
                        pending.append((aid, transcript["generation"], entries))
                        cursor_map[aid] = max(int(e["updatedSeq"]) for e in entries)
            for aid, generation, entries in pending:
                self._connect_stream_write(
                    {
                        "rows": {
                            "agentId": aid,
                            "generation": generation,
                            "entries": entries,
                            "deletes": [],
                            "replay": False,
                        }
                    }
                )
            if time.monotonic() - last_heartbeat >= 15:
                self._connect_stream_write({"heartbeat": {"serverTimeMs": int(time.time() * 1000)}})
                last_heartbeat = time.monotonic()

    def _watch_user_computer_requests(self, body: bytes) -> None:
        request = (
            _decode_connect_proto(body, streamed=True, schema=_PROTO_SCHEMAS["computer_watch_req"])
            if self._request_is_stream_proto()
            else _decode_connect_json(body)
        )
        credential = request.get("credential", "")
        log(
            f"[computer] watch request keys={sorted(request)} machine={str(request.get('machineId', ''))[:24]} "
            f"credential_prefix={str(credential)[:16]!r} credential_len={len(str(credential))} "
            f"content_type={self.headers.get('content-type', '')!r} body_len={len(body)} head={body[:16].hex()}"
        )
        if credential != LOCAL_EXEC_CREDENTIAL:
            log("[computer] watch rejected: invalid credential")
            self._reply(401, {"error": "invalid computer credential"})
            return

        machine_id = request.get("machineId", request.get("machine_id", ""))
        hello = request.get("hello") or {}
        self._connect_stream_start()
        self._connect_stream_write({"connected": {"pendingRequestCount": 0}})
        log(
            f"[computer] watch connected machine={str(machine_id)[:24]} "
            f"label={str(hello.get('label', ''))[:40]}"
        )

        # No computer requests are queued locally yet. Keep the native presence
        # stream open with the exact protobuf event shape expected by 0.30.
        last_heartbeat = time.monotonic()
        while True:
            time.sleep(1)
            if time.monotonic() - last_heartbeat < 15:
                continue
            self._connect_stream_write({"heartbeat": {"pendingRequestCount": 0}})
            last_heartbeat = time.monotonic()

    def _local_exec_requests(self) -> None:
        provider_id = str(uuid.uuid4())
        LOCAL_EXEC_PROVIDERS[provider_id] = {"connectedAt": time.time(), "hello": None}
        self._sse_start()
        self._sse_write({"kind": "welcome", "providerId": provider_id})
        log(f"[local-exec] request stream connected provider={provider_id[:8]}")
        try:
            while True:
                time.sleep(15)
                self._sse_comment(":ping")
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            LOCAL_EXEC_PROVIDERS.pop(provider_id, None)
            log(f"[local-exec] request stream closed provider={provider_id[:8]}")

    def do_GET(self):  # noqa: N802
        # Grok Bot 0.30's local-exec daemon opens the request stream with GET
        # (the response batches are POSTed separately to /local-exec/responses).
        if self.path == "/local-exec/requests":
            self._local_exec_requests()
            return

        # The renderer's local gateway read-model calls are ordinary GETs.  The
        # previous catch-all `{}` response left the native window without a
        # roster or transcript tail, so it stayed in reconnect/saved-message
        # state and never opened WatchGrokBotTranscripts.
        parsed = urlsplit(self.path)
        route = parsed.path
        query = parse_qs(parsed.query, keep_blank_values=False)

        if route == "/health":
            self._record(b"")
            self._reply(200, {"ok": True})
            return

        if route == "/events":
            # The 0.30 coordinator marks the gateway transport connected only
            # after this SSE body remains readable.  Returning `{}` here closes
            # the body immediately, producing `stream-ended` and preventing
            # transcript/watch setup.  Keep the local stream open with SSE
            # comments; transcript RPCs remain a separate local Connect stream.
            self._sse_start()
            with LOCK:
                SSE_SUBSCRIBERS[self] = self._sse_write_lock
            log("[events] gateway SSE connected")
            try:
                while True:
                    time.sleep(15)
                    self._sse_comment(":ping")
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                log("[events] gateway SSE disconnected")
            finally:
                with LOCK:
                    SSE_SUBSCRIBERS.pop(self, None)
            return

        agent_id = _first_agent_id(query)
        if route.startswith("/api/") and agent_id:
            # The desktop may restore an older persisted selection before the
            # current roster has been fetched.  Materialize that id locally so
            # the next roster response and transcript tail address the same
            # conversation without changing Grok persistence.
            ensure_agent(agent_id)

        if route == "/api/listAgents":
            if not AGENT_INDEX:
                ensure_agent("bridge-agent-local")
            self._record(b"")
            self._reply(200, [_agent_public(aid, meta) for aid, meta in AGENT_INDEX.items()])
            return

        if route == "/api/countAgents":
            self._record(b"")
            self._reply(200, len(AGENT_INDEX))
            return

        if route == "/api/getAgentTranscriptTail":
            limit = query.get("limit", [None])[0]
            before_seq = query.get("beforeSeq", [None])[0]
            try:
                limit = int(limit) if limit is not None else None
            except (TypeError, ValueError):
                limit = None
            try:
                before_seq = int(before_seq) if before_seq is not None else None
            except (TypeError, ValueError):
                before_seq = None
            self._record(b"")
            self._reply(200, _transcript_tail(agent_id, limit, before_seq))
            return

        if route == "/api/getHostStatus":
            self._record(b"")
            self._reply(
                200,
                {
                    "hostVersion": None,
                    "latestHostVersion": None,
                    "hostUpdateAvailable": False,
                    "isBusy": False,
                    "capabilities": ["transcript", "send", "local-ollama"],
                },
            )
            return

        self._record(b"")
        self._reply(200, {})

    def do_POST(self):  # noqa: N802
        body = _read_body(self)
        request_schema = _request_schema_for_path(self.path) if self._request_is_proto() else None
        if request_schema is not None:
            req = _decode_connect_proto(body, streamed=self._request_is_stream_proto(), schema=request_schema)
        else:
            try:
                req = json.loads(body) if body else {}
            except Exception:
                req = {}
        if not isinstance(req, dict):
            req = {}
        self._record(body)

        # Several renderer read-model calls carry the restored selection as an
        # `id` field before `/api/listAgents` is fetched.  Keep those calls on
        # the same local transcript store instead of letting the selected agent
        # disappear during reconnect.
        if self.path.startswith("/api/") and self.path not in {
            "/api/createAgent",
            "/api/createAgentFromTemplate",
            "/api/setGroupMembers",
            "/api/updateAgent",
            "/api/setAgentAvatarBytes",
        }:
            agent_id = _first_agent_id(req)
            if agent_id:
                ensure_agent(agent_id)

        if self.path == "/oauth/token":
            self._reply(200, handle_oauth_token(req))
            return

        # The 0.30 renderer uses these local gateway commands for its initial
        # roster and transcript hydration. Returning `{}` here is treated as a
        # malformed host reply, so the UI retries continuously and stays in its
        # reconnecting/saved-message state.
        if self.path == "/api/listAgents":
            if not AGENT_INDEX:
                ensure_agent("bridge-agent-local")
            self._reply(200, [_agent_public(aid, meta) for aid, meta in AGENT_INDEX.items()])
            return

        if self.path == "/api/countAgents":
            self._reply(200, len(AGENT_INDEX))
            return

        if self.path == "/api/getAgentTranscriptTail":
            self._reply(200, _transcript_tail(req.get("id"), req.get("limit"), req.get("beforeSeq")))
            return

        if self.path == "/api/getHostStatus":
            self._reply(
                200,
                {
                    "hostVersion": None,
                    "latestHostVersion": None,
                    "hostUpdateAvailable": False,
                    "isBusy": False,
                    "capabilities": ["transcript", "send", "local-ollama"],
                },
            )
            return

        if self.path == "/api/createAgent":
            template_id = _text_field(req.get("templateId"))
            if template_id:
                self._reply(
                    501,
                    {
                        "code": "LOCAL_TEMPLATE_UNSUPPORTED",
                        "message": "The local gateway cannot apply remote bot templates yet",
                        "templateId": template_id,
                    },
                )
                return
            self._reply(200, _create_gateway_agent(req))
            return

        if self.path == "/api/createAgentFromTemplate":
            self._reply(
                501,
                {
                    "code": "LOCAL_TEMPLATE_UNSUPPORTED",
                    "message": "The local gateway cannot import remote bot templates yet",
                },
            )
            return

        if self.path == "/api/createGroup":
            try:
                response = _create_gateway_group(req)
            except GatewayContractError as exc:
                self._reply(exc.status, exc.payload)
                return
            self._reply(200, response)
            return

        if self.path == "/api/setGroupMembers":
            try:
                response = _set_gateway_group_members(req)
            except GatewayContractError as exc:
                self._reply(exc.status, exc.payload)
                return
            self._reply(200, response)
            return

        if self.path == "/api/updateAgent":
            try:
                response = _update_gateway_agent(req)
            except GatewayContractError as exc:
                self._reply(exc.status, exc.payload)
                return
            self._reply(200, response)
            return

        if self.path == "/api/deleteAgents":
            try:
                response = _delete_gateway_agents(req)
            except GatewayContractError as exc:
                self._reply(exc.status, exc.payload)
                return
            self._reply(200, response)
            return

        if self.path == "/api/setAgentAvatarBytes":
            agent_id = _first_agent_id(req)
            if not agent_id:
                self._reply(400, {"code": "INVALID_AGENT_ID", "message": "id must not be empty"})
                return
            try:
                agent, version = _set_gateway_agent_avatar(agent_id, req.get("pngBase64"))
            except ValueError as exc:
                self._reply(400, {"code": "INVALID_AVATAR", "message": str(exc)})
                return
            self._reply(200, {"agent": agent, "version": version or None})
            return

        if self.path == "/api/getAgentAvatar":
            agent_id = _first_agent_id(req)
            if not agent_id:
                self._reply(400, {"code": "INVALID_AGENT_ID", "message": "id must not be empty"})
                return
            self._reply(200, _gateway_agent_avatar(agent_id))
            return

        if self.path == "/api/sendPrompt":
            agent_id = str(req.get("agentId") or "").strip() or "bridge-agent-local"
            prompt = str(req.get("prompt") or "").strip()
            client_nonce = str(req.get("clientNonce") or "").strip()
            if not prompt:
                self._reply(400, {"error": "prompt must not be empty"})
                return
            if not client_nonce:
                self._reply(400, {"error": "clientNonce must not be empty"})
                return

            ensure_agent(agent_id)
            user_entry, existing = claim_user_prompt(agent_id, prompt, client_nonce)
            if user_entry is None:
                # sendPrompt is retried when the client loses the response;
                # preserve exactly-once transcript semantics for that nonce.
                log(f"[api-send] duplicate nonce accepted agent={agent_id[:8]} nonce={client_nonce[:16]}")
                self._reply(200, {"accepted": True})
                return

            log(
                f"[api-send] accepted agent={agent_id[:8]} nonce={client_nonce[:16]} "
                f"echo={user_entry['entryId'][:8]}"
            )
            _submit_agent_loop(agent_id, prompt, client_nonce)
            self._reply(200, {"accepted": True})
            return

        if self.path == "/api/promptAcceptanceStatus":
            agent_id = str(req.get("agentId") or "").strip()
            client_nonce = str(req.get("clientNonce") or "").strip()
            with TRANSCRIPT_CHANGED:
                record = dict(PROMPT_ACCEPTANCE.get((agent_id, client_nonce), {}))
            if not record:
                self._reply(200, {"outcome": "not-found"})
                return
            self._reply(200, {"outcome": "found", "record": record})
            return

        if self.path.endswith("/WatchSandBoxMigration"):
            # Server-streaming: one DONE event, then an explicit Connect
            # EndStreamResponse.  EOF after a data envelope is an incomplete
            # stream and the 0.30 client reports it as InvalidArgument.
            migration = {"phase": 6, "detail": "", "atMs": int(time.time() * 1000), "offsetKey": ""}
            self._connect_stream_start()
            self._connect_stream_write(migration, schema=_PROTO_SCHEMAS["migration_event"])
            self._connect_stream_end()
            return

        if self.path.endswith("/RegisterSandMachine"):
            _observe_sand_machine(self.headers, req)
            label = str(req.get("label", "")).strip()
            permission = str(req.get("localToolPermission", "")).strip()
            if label:
                SAND_MACHINE["label"] = label
            if permission in {"always", "ask", "never"}:
                SAND_MACHINE["localToolPermission"] = permission
            self._reply(200, {}, schema={})
            return

        if self.path.endswith("/ListSandMachines"):
            machine_id = _observe_sand_machine(self.headers, req)
            machines = [dict(SAND_MACHINE)] if machine_id else []
            self._reply(200, {"machines": machines}, schema=_PROTO_SCHEMAS["list_machines_resp"])
            return

        if self.path.endswith("/UpdateSandMachineLabel"):
            machine_id = _observe_sand_machine(self.headers, req)
            label = str(req.get("label", "")).strip()
            if label and req.get("machineId") == machine_id:
                SAND_MACHINE["label"] = label
            self._reply(200, {"machine": dict(SAND_MACHINE)}, schema=_PROTO_SCHEMAS["update_machine_resp"])
            return

        if self.path.endswith("/UpdateSandMachineLocalToolPermission"):
            machine_id = _observe_sand_machine(self.headers, req)
            permission = str(req.get("localToolPermission", "")).strip()
            if permission in {"always", "ask", "never"} and req.get("machineId") == machine_id:
                SAND_MACHINE["localToolPermission"] = permission
            self._reply(200, {"machine": dict(SAND_MACHINE)}, schema=_PROTO_SCHEMAS["update_machine_resp"])
            return

        if self.path == "/sand-box/local-exec-daemon-credential":
            self._reply(
                200,
                {
                    "credential": LOCAL_EXEC_CREDENTIAL,
                    "expiresAtMs": int((time.time() + 86400) * 1000),
                },
            )
            return

        if self.path == "/sand-box/local-exec-connection":
            credential = req.get("credential", "")
            if credential != LOCAL_EXEC_CREDENTIAL:
                self._reply(401, {"error": "invalid local-exec credential"})
                return
            self._reply(200, {"baseUrl": LOCAL_EXEC_BASE_URL, "token": LOCAL_EXEC_TOKEN, "networkToken": ""})
            return

        if self.path == "/local-exec/requests":
            self._local_exec_requests()
            return

        if self.path == "/local-exec/responses":
            frames = req.get("frames") or []
            provider_id = req.get("providerId", "")
            if provider_id and provider_id in LOCAL_EXEC_PROVIDERS:
                for frame in frames:
                    if isinstance(frame, dict) and frame.get("kind") == "hello":
                        LOCAL_EXEC_PROVIDERS[provider_id]["hello"] = frame
                log(f"[local-exec] response batch provider={provider_id[:8]} frames={len(frames)}")
            self._reply(200, {"acceptedCount": len(frames)})
            return

        if self.path.endswith("/WatchGrokBotTranscripts"):
            try:
                self._watch_transcripts(body)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                log("[transcript] watch disconnected")
            return

        if self.path.endswith("/WatchGrokBotUserComputerRequests"):
            try:
                self._watch_user_computer_requests(body)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                log("[computer] watch disconnected")
            return

        if self.path.endswith("/SendGrokBotUserMessage"):
            agent_id = req.get("agentId", "")
            text = req.get("text", "")
            client_nonce = str(req.get("messageId") or uuid.uuid4())
            ensure_agent(agent_id or "bridge-agent-local")
            agent_id = agent_id or "bridge-agent-local"
            user_entry, existing = claim_user_prompt(agent_id, text, client_nonce)
            if user_entry is None:
                self._reply(
                    200,
                    {"dispatched": True, "mode": "MODE_LOCAL", "delivery": "ACCEPTED_BOX"},
                    schema=_PROTO_SCHEMAS["send_resp"],
                )
                return
            self._reply(
                200,
                {"dispatched": True, "mode": "MODE_LOCAL", "delivery": "ACCEPTED_BOX"},
                schema=_PROTO_SCHEMAS["send_resp"],
            )
            _submit_agent_loop(agent_id, text, client_nonce)
            return

        if self.path.endswith("/ListGrokBotTranscriptEntries"):
            agent_id = req.get("agentId", "")
            with TRANSCRIPT_CHANGED:
                t = TRANSCRIPTS.get(agent_id, {"generation": 1, "entries": []})
                entries = [dict(e) for e in t["entries"]]
                gen = t["generation"]
            self._reply(200, {"entries": entries, "generation": gen}, schema=_PROTO_SCHEMAS["list_entries_resp"])
            return

        if self.path.endswith("/ListGrokBotAgents"):
            if not AGENT_INDEX:
                ensure_agent("bridge-agent-local")
            agents = [_agent_public(aid, meta) for aid, meta in AGENT_INDEX.items()]
            self._reply(200, {"agents": agents}, schema=_PROTO_SCHEMAS["list_agents_resp"])
            return

        if self.path.endswith("/GetGrokBotRuntimeCapabilities"):
            self._reply(
                200,
                {"capabilities": dict(RUNTIME_CAPABILITIES)},
                schema=_PROTO_SCHEMAS["runtime_caps_resp"],
            )
            return

        if self.path.endswith("/CreateGrokBotAgent") or self.path.endswith("/CreateGrokBotTemporalAgent"):
            aid = req.get("agentId") or str(uuid.uuid4())
            meta = ensure_agent(
                aid,
                name=req.get("name", "Bridge Bot"),
                description=req.get("description", "Local Ollama-backed Grok Bot"),
            )
            log(f"[agent] created {aid} {meta['name']}")
            self._reply(
                200,
                {
                    "agent": {"id": aid, "legacyAgentId": aid, "agentId": aid, **meta},
                    # This is GrokBotAgentHarnessKind, where BOX=1. The value
                    # 4 belongs to the separate TemporalHarnessMode enum and
                    # makes a protobuf client reject an otherwise valid agent.
                    "harness": _ENUM_AGENT_HARNESS["GROK_BOT_AGENT_HARNESS_KIND_BOX"],
                },
                schema=_PROTO_SCHEMAS["create_agent_resp"],
            )
            return

        if self.path.endswith("/IssueGrokBotUserComputerCredential"):
            _observe_sand_machine(self.headers, req)
            self._reply(
                200,
                {
                    "credential": LOCAL_EXEC_CREDENTIAL,
                    "expiresAtMs": int((time.time() + 86400) * 1000),
                    "serverAuthoritative": False,
                },
                schema=_PROTO_SCHEMAS["issue_credential_resp"],
            )
            return

        if self.path.endswith("/GetGrokBotSendStatus"):
            agent_id = str(req.get("agentId") or "").strip()
            client_nonce = str(req.get("messageId") or "").strip()
            with TRANSCRIPT_CHANGED:
                record = dict(PROMPT_ACCEPTANCE.get((agent_id, client_nonce), {}))
            if not record:
                status = "GROK_BOT_SEND_STATUS_NOT_FOUND"
                response = {"status": status}
            elif record.get("status") == "failed":
                response = {
                    "status": "GROK_BOT_SEND_STATUS_REJECTED",
                    "echoEntryId": record.get("echoEntryId", ""),
                    "rejectionCode": record.get("rejectionCode", "LOCAL_MODEL_ERROR"),
                    "acceptedAtMs": record.get("acceptedAtMs", 0),
                }
            else:
                response = {
                    "status": "GROK_BOT_SEND_STATUS_ACCEPTED",
                    "echoEntryId": record.get("echoEntryId", ""),
                    "acceptedAtMs": record.get("acceptedAtMs", 0),
                }
            self._reply(200, response, schema=_PROTO_SCHEMAS["send_status_resp"])
            return

        if RESPONSES.get(self.path) is not None:
            self._reply(200, RESPONSES[self.path], schema=_response_schema_for_path(self.path))
            return

        self._reply(200, {}, schema=_response_schema_for_path(self.path))

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("grok-bridge replacement backend v2 (native chat via transcript store)", flush=True)
    resumed = resume_pending_prompts()
    if resumed:
        print(f"[state] resumed pending prompts={resumed}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
