"""Replacement backend v2: native Grok Bot chat with YOUR models.

Flow: App SendGrokBotUserMessage → we run the agent loop (your gateway/Ollama)
→ we append transcript entries (send_message / assistant_text) → the app's
WatchGrokBotTranscripts stream delivers them → the app UI renders natively.

Zero Grok inference. Zero quota.
"""

import base64
import json
import os
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

import httpx  # noqa: E402

LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\backend_calls.jsonl"
os.makedirs(os.path.dirname(LOG), exist_ok=True)

GATEWAY = bc.config().get("gateway", "").rstrip("/")
GATEWAY_KEY = bc.get_codex_key()
MODEL = "gpt-5.6-terra"

# ---- transcript store: agent_id → {generation, entries:[{seq, entryKind, body_b64, role, text}]}}
TRANSCRIPTS = {}
LOCK = threading.Lock()
AGENT_INDEX = {"list": []}  # agents the app can see


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def b64(s: str) -> str:
    import base64

    return base64.b64encode(s.encode("utf-8")).decode()


def append_entry(agent_id: str, kind: str, obj: dict) -> dict:
    with LOCK:
        t = TRANSCRIPTS.setdefault(agent_id, {"generation": 1, "next_seq": 1, "entries": []})
        entry = {
            "seq": t["next_seq"],
            "entryKind": kind,
            "body": b64(json.dumps(obj, ensure_ascii=False)),
        }
        t["next_seq"] += 1
        t["entries"].append(entry)
    return entry


def call_model(task: str) -> str:
    r = httpx.post(
        f"{GATEWAY}/chat/completions",
        headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
        json={"model": MODEL, "messages": [{"role": "user", "content": task}], "max_tokens": 800},
        timeout=240,
        trust_env=False,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def agent_loop(agent_id: str, task: str) -> None:
    """Our agent loop: your model → assistant_text entry (tool exec via channels comes later)."""
    try:
        reply = call_model(task)
        with LOCK:
            t = TRANSCRIPTS.get(agent_id)
            if t is None:
                return
            seq = t["next_seq"]
            t["next_seq"] += 1
        append = {
            "seq": seq,
            "entryKind": "assistant_text",
            "body": b64(json.dumps({"role": "assistant", "content": reply}, ensure_ascii=False)),
        }
        with LOCK:
            TRANSCRIPTS[agent_id]["entries"].append(append)
        log(f"[loop] {agent_id[:8]} assistant_text committed ({len(reply)} chars)")
    except Exception as e:
        log(f"[loop] error: {type(e).__name__}: {str(e)[:120]}")


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
        "privacyMode": 1,
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
    },
    "/aiserver.v1.GrokBotService/GetSandBoxRunState": {"state": "SAND_BOX_RUN_STATE_RUNNING", "imageUpdateAvailable": False},
    "/aiserver.v1.GrokBotService/ListSandBoxes": {"boxes": [{"running": True}]},
    "/aiserver.v1.DashboardService/GetSandAccessStatus": {"hasAccess": True},
    "/aiserver.v1.DashboardService/GetSandTrialClaimStatus": {"status": 1},
    "/aiserver.v1.DashboardService/GetHardLimit": {"noUsageBasedAllowed": True},
    "/aiserver.v1.GrokBotService/ListGrokBotAgents": {"agents": []},  # placeholder, filled dynamically
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _record(self, body: bytes, status: int) -> None:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "method": self.command, "path": self.path, "status": status,
                                "body": body.decode("utf-8", errors="replace")[:1200]}) + "\n")

    def _reply(self, status: int, obj) -> None:
        data = json.dumps(obj if obj is not None else {}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        try:
            req = json.loads(body) if body else {}
        except Exception:
            req = {}
        self._record(body, 200)

        if self.path == "/oauth/token":
            self._reply(200, handle_oauth_token(req))
            return

        if self.path.endswith("/WatchSandBoxMigration"):
            # server-streaming: one DONE event, then end — sandbox always "ready"
            import struct

            data = json.dumps({"phase": 6, "detail": "", "atMs": int(time.time() * 1000), "offsetKey": ""}).encode("utf-8")
            framed = struct.pack(">BI", 0, len(data)) + data
            self.send_response(200)
            self.send_header("Content-Type", "application/connect+json")
            self.send_header("Content-Length", str(len(framed)))
            self.end_headers()
            self.wfile.write(framed)
            return

        if self.path.endswith("/SendGrokBotUserMessage"):
            agent_id = req.get("agentId", "")
            text = req.get("text", "")
            append_entry(agent_id, "send_message", {"role": "user", "content": text})
            self._reply(200, {"dispatched": True, "mode": 1, "delivery": 1})
            threading.Thread(target=agent_loop, args=(agent_id, text), daemon=True).start()
            return

        if self.path.endswith("/ListGrokBotTranscriptEntries"):
            agent_id = req.get("agentId", "")
            with LOCK:
                t = TRANSCRIPTS.get(agent_id, {"generation": 1, "entries": []})
                entries = [dict(e) for e in t["entries"]]
                gen = t["generation"]
            self._reply(200, {"entries": entries, "generation": gen})
            return

        if self.path.endswith("/ListGrokBotAgents"):
            agents = []
            for aid, meta in AGENT_INDEX["list"].items():
                agents.append({"agentId": aid, **meta})
            self._reply(200, {"agents": agents})
            return

        if self.path.endswith("/CreateGrokBotAgent"):
            aid = req.get("agentId") or str(uuid.uuid4())
            meta = {"name": req.get("name", "Bridge Bot"), "description": req.get("description", ""), "title": req.get("title", "")}
            AGENT_INDEX["list"][aid] = meta
            TRANSCRIPTS.setdefault(aid, {"generation": 1, "next_seq": 1, "entries": []})
            log(f"[agent] created {aid} {meta['name']}")
            self._reply(200, {"agent": {"id": "1", "agentId": aid, **meta}})
            return

        if RESPONSES.get(self.path) is not None:
            self._reply(200, RESPONSES[self.path])
            return

        self._reply(200, {})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("grok-bridge replacement backend v2 (native chat via transcript store)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
