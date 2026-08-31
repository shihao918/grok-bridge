"""Grok Bot UserComputer bridge daemon (production location).

Maintains a Watch streaming connection (presence), polls the request queue,
routes exec/messagesOp frames to local multi-agent handlers (LangGraph live,
AutoGen live), submits results via the official Submit endpoint.

Token is derived at runtime from the Grok Bot app's encrypted store (DPAPI +
AES-GCM); nothing sensitive is kept in plaintext on disk.

Handler contract (message_json / exec serverMessageJson):
  {"handler": "langgraph"|"autogen"|"echo", "task": "<text>"}
"""

import json
import os
import struct
import sys
import threading
import time
import uuid

import httpx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"
LABEL = bc.config()["label"]
LOCAL_ROOT = bc.config()["local_root"]
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "daemon.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


state = bc.load_state()
if "machine_id" not in state:
    state["machine_id"] = str(uuid.uuid4())
    bc.save_state(state)

GATEWAY = "http://127.0.0.1:18082"
LG_DEV = "http://127.0.0.1:2024"
AGS = "http://127.0.0.1:8081/api"
AGS_USER = bc.config()["ags_user"]


def bearer() -> dict:
    return {
        "authorization": f"Bearer {bc.get_grok_access_token()}",
        "content-type": "application/json",
        "connect-protocol-version": "1",
        "x-ghost-mode": "true",
    }


def stream_headers() -> dict:
    h = bearer()
    h["content-type"] = "application/connect+json"
    return h


def envelope(payload: bytes) -> bytes:
    return struct.pack(">BI", 0, len(payload)) + payload


def call(method: str, body: dict) -> dict:
    r = httpx.post(f"{BASE}/{method}", headers=bearer(), json=body, timeout=30, trust_env=False)
    r.raise_for_status()
    return r.json() if r.text else {}


def refresh_credential() -> str:
    cred = call("IssueGrokBotUserComputerCredential", {"machineId": state["machine_id"]})
    state["credential"] = cred["credential"]
    state["credential_expires_at_ms"] = cred.get("expiresAtMs")
    bc.save_state(state)
    return cred["credential"]


def credential_valid() -> bool:
    try:
        exp = float(state.get("credential_expires_at_ms", 0))
    except (TypeError, ValueError):
        return False
    return bool(state.get("credential")) and exp > time.time() * 1000 + 60_000


HELLO = {
    "label": LABEL,
    "localRoot": LOCAL_ROOT,
    "capabilities": {"messagesOp": True, "messagesOpGeneration": 1},
}


# ---------- handlers ----------
def handle_langgraph(task: str) -> dict:
    lg = httpx.Client(timeout=240, trust_env=False)
    t = lg.post(f"{LG_DEV}/threads", json={}).json()
    r = lg.post(
        f"{LG_DEV}/threads/{t['thread_id']}/runs/wait",
        json={"assistant_id": "groupchat", "input": {"messages": [{"role": "user", "content": task}]}},
    )
    msgs = r.json().get("messages", [])
    return {
        "ok": bool(msgs),
        "engine": "langgraph",
        "thread_id": t["thread_id"],
        "turns": [
            {"speaker": m.get("name") or m.get("type"), "text": str(m.get("content"))[:500]}
            for m in msgs
            if (m.get("name") or m.get("type")) != "human"
        ],
    }


def handle_autogen(task: str) -> dict:
    import asyncio
    import websockets

    async def _run():
        if "ags_session_id" not in state:
            r = httpx.post(
                f"{AGS}/sessions/", headers={"content-type": "application/json"},
                json={"user_id": AGS_USER, "team_id": state.get("ags_team_id", 1)},
                timeout=30, trust_env=False,
            )
            r.raise_for_status()
            state["ags_session_id"] = r.json()["data"]["id"]
            bc.save_state(state)
        sid = state["ags_session_id"]

        team = httpx.get(
            f"{AGS}/teams/{state.get('ags_team_id', 1)}",
            params={"user_id": AGS_USER}, timeout=30, trust_env=False,
        ).json()["data"]["component"]
        run = httpx.post(
            f"{AGS}/runs/", headers={"content-type": "application/json"},
            json={"session_id": sid, "user_id": AGS_USER}, timeout=30, trust_env=False,
        ).json()
        run_id = run["data"]["run_id"]

        collected = []
        uri = f"ws://127.0.0.1:8081/api/ws/runs/{run_id}"
        async with websockets.connect(uri, max_size=20 * 1024 * 1024) as ws:
            await ws.send(json.dumps({"type": "start", "task": task, "team_config": team}))
            deadline = time.time() + 300
            while time.time() < deadline:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(5, deadline - time.time()))
                msg = json.loads(raw)
                t = msg.get("type")
                if t == "completion":
                    tr = (msg.get("data") or {}).get("task_result") or {}
                    for m in tr.get("messages") or []:
                        if m.get("source") and m.get("source") != "user":
                            collected.append({"speaker": m.get("source"), "text": str(m.get("content"))[:600]})
                    break
                if t == "error":
                    collected.append({"speaker": "error", "text": json.dumps(msg)[:400]})
                    break
                d = msg.get("data")
                if isinstance(d, dict) and d.get("source") and d.get("source") != "user" and d.get("content"):
                    collected.append({"speaker": d.get("source"), "text": str(d.get("content"))[:300]})
        return {"ok": bool(collected), "engine": "autogen", "turns": collected[-6:]}

    return asyncio.run(_run())


def handle_echo(task: str) -> dict:
    return {"ok": True, "engine": "echo", "echo": task}


HANDLERS = {"langgraph": handle_langgraph, "autogen": handle_autogen, "echo": handle_echo}


def dispatch(payload: dict) -> dict:
    """Single dispatch point shared by all transports (Grok channel + standalone HTTP)."""
    task = payload.get("task", "")
    handler = HANDLERS.get(payload.get("handler", "echo"), handle_echo)
    log(f"  dispatch: handler={payload.get('handler')} task={task[:60]!r}")
    try:
        result = handler(task)
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return {"bridge": LABEL, **result}


def run_handler_response(rid: str, client: dict) -> dict:
    try:
        payload = json.loads(client.get("messageJson", "{}"))
    except Exception:
        payload = {"raw": client.get("messageJson", "")}
    res = dispatch(payload)
    return {"requestId": rid, "client": {"messageJson": json.dumps(res), "cwdState": LOCAL_ROOT}}


def process_frame(qr: dict) -> dict:
    frame = qr.get("frame", {})
    rid = frame.get("requestId", qr.get("id", ""))

    op = frame.get("messagesOp")
    if op:
        return run_handler_response(rid, op.get("client", {}))

    ex = frame.get("exec")
    if ex:
        log(f"  request {rid[:8]}: exec frame (standing={ex.get('authorizedByStanding')}, approval={ex.get('authorizedByApproval')})")
        raw = ex.get("serverMessageJson", "")
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {"task": raw}
        res = dispatch(payload)
        return {"requestId": rid, "client": {"messageJson": json.dumps(res), "cwdState": LOCAL_ROOT}}

    kinds = [k for k in ("upload", "download", "cancel", "retireApproval") if frame.get(k)]
    log(f"  request {rid[:8]}: declining {kinds}")
    return {"requestId": rid, "fileError": {"resultJson": json.dumps({"error": f"bridge handles exec/messagesOp; got {kinds}"})}}


# ---------- standalone transport (local HTTP, Grok-independent) ----------
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: E402

STANDALONE_BIND = bc.config().get("standalone_bind", "127.0.0.1")
STANDALONE_PORT = int(bc.config().get("standalone_port", 18083))


class StandaloneHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, code: int, obj: dict) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802
        if self.path != "/run":
            self._json(404, {"error": "not found (POST /run)"})
            return
        length = int(self.headers.get("content-length") or 0)
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            self._json(400, {"error": "invalid json"})
            return
        self._json(200, dispatch(payload))

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._json(200, {"ok": True, "label": LABEL, "engines": sorted(HANDLERS), "transports": sorted(TRANSPORTS)})
            return
        self._json(404, {"error": "not found (GET /health)"})

    def log_message(self, fmt, *args):  # silence stderr
        pass


def standalone_server() -> None:
    srv = ThreadingHTTPServer((STANDALONE_BIND, STANDALONE_PORT), StandaloneHandler)
    log(f"[standalone] listening on {STANDALONE_BIND}:{STANDALONE_PORT} (POST /run, GET /health)")
    srv.serve_forever()


# ---------- watch (presence) ----------
def watch_loop(credential: str):
    while True:
        try:
            body = {"machineId": state["machine_id"], "credential": credential, "hello": HELLO}
            with httpx.stream(
                "POST",
                f"{BASE}/WatchGrokBotUserComputerRequests",
                headers=stream_headers(),
                content=envelope(json.dumps(body).encode()),
                timeout=httpx.Timeout(300, read=None),
                trust_env=False,
            ) as r:
                log(f"[watch] connected ({r.status_code})")
                buf = b""
                for chunk in r.iter_raw():
                    buf += chunk
                    while True:
                        try:
                            (ln,) = struct.unpack(">I", buf[1:5])
                        except Exception:
                            break
                        if len(buf) < 5 + ln:
                            break
                        payload = buf[5 : 5 + ln]
                        buf = buf[5 + ln :]
                        try:
                            ev = json.loads(payload)
                        except Exception:
                            continue
                        if "connected" in ev:
                            log("[watch] server acknowledged connection")
                        elif "notify" in ev:
                            log("[watch] notify received")
        except Exception as e:
            log(f"[watch] disconnected ({type(e).__name__}), retrying in 5s")
            time.sleep(5)


# ---------- main ----------
TRANSPORTS = set(bc.config().get("transports", ["grok", "standalone"]))
if not TRANSPORTS <= {"grok", "standalone"}:
    raise SystemExit(f"invalid transports in config: {sorted(TRANSPORTS - {'grok', 'standalone'})}")
if not TRANSPORTS:
    raise SystemExit("no transports enabled in config (transports: [])")


def grok_loop() -> None:
    if not credential_valid():
        refresh_credential()
    log(f"machine_id: {state['machine_id']} (credential exp {state.get('credential_expires_at_ms')})")
    threading.Thread(target=watch_loop, args=(state["credential"],), daemon=True).start()
    while True:
        if not credential_valid():
            refresh_credential()
            log("credential refreshed")
        poll = call(
            "PollGrokBotUserComputerRequests",
            {"machineId": state["machine_id"], "credential": state["credential"], "ackIds": [], "limit": 10},
        )
        queued = poll.get("requests", [])
        if queued:
            log(f"got {len(queued)} request(s)")
            frames, acks = [], []
            for qr in queued:
                try:
                    resp = process_frame(qr)
                    if resp:
                        frames.append(resp)
                    acks.append(qr["id"])
                except Exception as e:
                    log(f"  frame error: {e}")
                    acks.append(qr["id"])
            if frames:
                r = call(
                    "SubmitGrokBotUserComputerResponses",
                    {"machineId": state["machine_id"], "credential": state["credential"], "frames": frames},
                )
                log(f"  submitted, accepted={r.get('acceptedCount')}")
            call(
                "PollGrokBotUserComputerRequests",
                {"machineId": state["machine_id"], "credential": state["credential"], "ackIds": acks, "limit": 1},
            )
        time.sleep(3)


def main() -> None:
    log(f"transports: {sorted(TRANSPORTS)}")
    if "standalone" in TRANSPORTS:
        threading.Thread(target=standalone_server, daemon=True).start()
    if "grok" in TRANSPORTS:
        grok_loop()
    else:
        log("[grok] transport disabled by config; standalone-only mode")
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("stopped")
