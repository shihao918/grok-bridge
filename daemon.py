"""Grok Bot UserComputer bridge daemon (production).

Transports (config: transports = ["grok", "standalone"]):
  - grok:       Watch presence + Poll queue + Submit responses (cloud-dispatched exec frames)
  - standalone: local HTTP entry (POST /run, GET /health, GET /ui) — Grok-independent

Quota watcher: polls GetSandUsageStatus; on hitting the threshold pops a Windows
dialog letting you choose "switch to local models (Ollama)" or "wait for reset".

Token is derived at runtime from the Grok Bot app's encrypted store; no plaintext
secrets on disk. Handler contract: {"handler": "...", "task": "..."}.
"""

import json
import os
import struct
import subprocess
import sys
import threading
import time
import uuid

import httpx
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "daemon.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

_cfg = bc.config()
LABEL = _cfg["label"]
LOCAL_ROOT = _cfg["local_root"]
GATEWAY = "http://127.0.0.1:18082"
LG_DEV = "http://127.0.0.1:2024"
AGS = "http://127.0.0.1:8081/api"
AGS_USER = _cfg["ags_user"]
OLLAMA_URL = _cfg.get("ollama_url", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = _cfg.get("ollama_model", "gemma4-26b-qat-uncensored:q4km")
QUOTA_THRESHOLD = int(_cfg.get("quota_threshold", 100))
QUOTA_CHECK_MINUTES = int(_cfg.get("quota_check_minutes", 10))
LOCAL_FALLBACK = bool(_cfg.get("local_fallback", True))


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


state = bc.load_state()
if "machine_id" not in state:
    state["machine_id"] = str(uuid.uuid4())
    bc.save_state(state)

runtime = {"quota": None, "exhausted": False, "policy": state.get("policy", "auto")}


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


def handle_local(task: str) -> dict:
    """Local model (Ollama) — used when Grok quota is exhausted or explicitly chosen."""
    payload_model = None
    r = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": OLLAMA_MODEL, "messages": [{"role": "user", "content": task}], "stream": False},
        timeout=300,
        trust_env=False,
    )
    r.raise_for_status()
    text = (r.json().get("message") or {}).get("content", "")
    return {"ok": bool(text), "engine": f"local({OLLAMA_MODEL})", "reply": text[:2000]}


HANDLERS = {"langgraph": handle_langgraph, "autogen": handle_autogen, "echo": handle_echo, "local": handle_local}

# ---------- quota queue (tasks for Grok agents, auto-released at reset) ----------
QUEUE_FILE = os.path.join(bc.STATE_DIR, "grok_queue.json")
GROK_AGENT_ID = _cfg.get("grok_agent_id", "")


def load_queue() -> list:
    if os.path.exists(QUEUE_FILE):
        return json.load(open(QUEUE_FILE, encoding="utf-8"))
    return []


def save_queue(q: list) -> None:
    json.dump(q, open(QUEUE_FILE, "w", encoding="utf-8"), indent=2)


def send_to_grok_agent(agent_id: str, task: str) -> dict:
    return call(
        "SendGrokBotUserMessage",
        {
            "agentId": agent_id,
            "messageId": str(uuid.uuid4()),
            "text": task,
            "sentAtMs": int(time.time() * 1000),
            "isFork": False,
        },
    )


def handle_grok_agent(task: str) -> dict:
    """Dispatch a task to a Grok cloud agent. Queues automatically while quota is exhausted."""
    if not GROK_AGENT_ID:
        return {"ok": False, "engine": "grok-agent", "error": "set grok_agent_id in state/config.json"}
    if runtime["exhausted"]:
        q = load_queue()
        q.append({"task": task, "queuedAt": time.time()})
        save_queue(q)
        return {"ok": True, "engine": "grok-agent", "queued": True, "position": len(q), "note": "will auto-dispatch at quota reset"}
    r = send_to_grok_agent(GROK_AGENT_ID, task)
    return {"ok": r.get("dispatched", False), "engine": "grok-agent", "delivery": r.get("delivery")}


def flush_grok_queue() -> int:
    q = load_queue()
    sent = 0
    for item in q:
        try:
            send_to_grok_agent(GROK_AGENT_ID, item["task"])
            sent += 1
        except Exception as e:
            log(f"[queue] flush error: {e}")
            break
    save_queue([])
    if sent:
        log(f"[queue] flushed {sent}/{len(q)} queued task(s) to Grok")
    return sent


def dispatch(payload: dict) -> dict:
    """Single dispatch point shared by all transports. Policy-aware routing."""
    explicit = payload.get("handler")
    default_handler = bc.config().get("default_handler", "echo")
    if explicit:
        handler_name = explicit
    elif runtime["exhausted"] and runtime["policy"] == "local" and LOCAL_FALLBACK:
        handler_name = "local"
        log("  dispatch: quota exhausted + policy=local → routing to local model")
    else:
        handler_name = default_handler

    handler = HANDLERS.get(handler_name)
    if handler is None:
        return {"bridge": LABEL, "ok": False, "error": f"unknown handler '{handler_name}'", "known": sorted(HANDLERS)}

    task = payload.get("task", "")
    log(f"  dispatch: handler={handler_name} task={task[:60]!r}")
    try:
        result = handler(task)
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return {"bridge": LABEL, **result}


# ---------- quota watcher + choice dialog ----------
def toast_choice() -> None:
    """Pop a Windows Yes/No dialog: switch to local models now?"""
    script = (
        "Add-Type -AssemblyName System.Windows.Forms;"
        "$r=[System.Windows.Forms.MessageBox]::Show("
        "'Grok 周配额已用完（100%）。是否切换到本地模型（Ollama）继续工作？',"
        "'Grok Bridge 配额提示','YesNo','Question',"
        "'Button1','MessageBoxOptions.DefaultDesktopOnly');"
        "if($r -eq 'Yes'){Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18083/policy'"
        " -Body '{\"mode\":\"local\"}' -ContentType 'application/json'}"
        "else{Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18083/policy'"
        " -Body '{\"mode\":\"wait\"}' -ContentType 'application/json'}"
    )
    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-STA", "-Command", script],
            creationflags=subprocess.DETACHED_PROCESS,
        )
        log("[quota] choice dialog popped (Yes=local / No=wait)")
    except Exception as e:
        log(f"[quota] dialog failed: {e}")


def quota_watch_loop() -> None:
    first = True
    while True:
        try:
            d = httpx.post(
                "https://api2.cursor.sh/aiserver.v1.DashboardService/GetSandUsageStatus",
                headers=bearer(), json={}, timeout=30, trust_env=False,
            ).json()
            pct = d.get("usagePercent")  # legacy weekly-percent shape (SuperGrok)
            zero_limit = d.get("includedLimitZero") is True  # new shape: no included quota this period
            exhausted = (pct is not None and pct >= QUOTA_THRESHOLD) or zero_limit
            episode = d.get("currentPeriodStart", "unknown")
            runtime["quota"] = {
                "usagePercent": pct,
                "includedLimitZero": zero_limit,
                "plan": d.get("grokPlanLabel"),
                "nextReset": d.get("nextResetTimestampUtc"),
                "exhausted": exhausted,
                "checkedAt": time.time(),
            }
            prev = runtime["exhausted"]
            runtime["exhausted"] = exhausted

            if exhausted != prev or first:
                log(f"[quota] exhausted={exhausted} pct={pct} zeroLimit={zero_limit} plan={d.get('grokPlanLabel')} reset={d.get('nextResetTimestampUtc')}")
            if exhausted and not prev:
                toast_choice() if False else None  # transition handled below (episode-aware)
            if exhausted and not prev and GROK_AGENT_ID:
                flush_grok_queue()
            if not exhausted and prev and GROK_AGENT_ID:
                flush_grok_queue()  # reset happened → release queued tasks

            # episode-aware choice dialog: ask once per billing period, only if unchosen
            chosen = state.get("policy", "auto") != "auto"
            shown_for_episode = state.get("dialog_episode") == episode
            if exhausted and not chosen and not shown_for_episode and not runtime.get("dialog_shown"):
                toast_choice()
                runtime["dialog_shown"] = True
                state["dialog_episode"] = episode
                bc.save_state(state)
            if not exhausted:
                runtime["dialog_shown"] = False  # recovered → allow asking next episode
            first = False
        except Exception as e:
            log(f"[quota] check failed: {type(e).__name__}: {str(e)[:80]}")
        time.sleep(QUOTA_CHECK_MINUTES * 60)


# ---------- grok transport ----------
def grok_loop() -> None:
    if not credential_valid():
        refresh_credential()
    log(f"machine_id: {state['machine_id']} (credential exp {state.get('credential_expires_at_ms')})")
    threading.Thread(target=quota_watch_loop, daemon=True).start()
    threading.Thread(target=watch_loop, args=(state["credential"],), daemon=True).start()
    while True:
        try:
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
        except Exception as e:
            msg = f"{type(e).__name__}: {str(e)[:100]}"
            log(f"[grok] poll cycle error ({msg}), continuing")
            if "401" in msg or "403" in msg:
                time.sleep(600)  # plan-gated: back off hard, don't hammer the gate
                continue
        time.sleep(3)


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


def run_handler_response(rid: str, client: dict) -> dict:
    try:
        payload = json.loads(client.get("messageJson", "{}"))
    except Exception:
        payload = {"raw": client.get("messageJson", "")}
    res = dispatch(payload)
    return {"requestId": rid, "client": {"messageJson": json.dumps(res), "cwdState": LOCAL_ROOT}}


def watch_loop(credential: str) -> None:
    backoff = 5
    while True:
        started = time.time()
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
                # clean stream end (server closed) — treat like a failure for backoff
                log(f"[watch] stream ended after {time.time() - started:.1f}s")
        except Exception as e:
            log(f"[watch] disconnected ({type(e).__name__}: {str(e)[:80]}) after {time.time() - started:.1f}s")
        log(f"[watch] reconnecting in {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
        if time.time() - started > 120:  # a healthy long-lived connection resets backoff
            backoff = 5


# ---------- standalone transport + web UI ----------
STANDALONE_BIND = _cfg.get("standalone_bind", "127.0.0.1")
STANDALONE_PORT = int(_cfg.get("standalone_port", 18083))
STANDALONE_TOKEN = _cfg.get("standalone_token", "")

UI_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>Grok Bridge</title>
<style>
body{font-family:Segoe UI,sans-serif;max-width:720px;margin:24px auto;padding:0 12px;background:#111;color:#eee}
.card{background:#1c1c24;border-radius:10px;padding:16px;margin-bottom:16px}
button{background:#4f6ef7;color:#fff;border:0;border-radius:6px;padding:8px 14px;cursor:pointer;margin:2px}
button.sel{background:#22c55e}
input,textarea,select{width:100%;box-sizing:border-box;background:#0d0d12;color:#eee;border:1px solid #333;border-radius:6px;padding:8px;margin:4px 0}
textarea{min-height:80px}
pre{background:#0d0d12;padding:10px;border-radius:6px;white-space:pre-wrap;word-break:break-all}
.pct{font-size:42px;font-weight:700}
.warn{color:#f87171}.ok{color:#4ade80}
</style></head><body>
<h2>Grok Bridge 控制台</h2>
<div class="card"><b>配额状态</b><div id="quota">加载中…</div>
<div><button onclick="refreshQuota()">刷新</button>
<button id="b-local" onclick="setPolicy('local')">切换到本地模型</button>
<button id="b-auto" onclick="setPolicy('auto')">跟随默认</button>
<span id="policy"></span></div></div>
<div class="card"><b>直接派任务（不经过 Grok）</b>
<select id="engine"><option value="local">本地模型 (Ollama)</option><option value="langgraph">LangGraph 群聊</option><option value="autogen">AutoGen 团队</option><option value="echo">echo 自检</option></select>
<textarea id="task" placeholder="任务内容…"></textarea>
<button onclick="run()">执行</button>
<pre id="out">（结果）</pre></div>
<script>
function refreshQuota(){fetch('/quota').then(r=>r.json()).then(d=>{const q=d.quota||{};
document.getElementById('quota').innerHTML=q.usagePercent==null?'（无数据）':
'<span class="pct '+(q.exhausted?'warn':'ok')+'">'+q.usagePercent+'%</span> 计划: '+q.plan+'<br>重置: '+q.nextReset;
document.getElementById('b-local').className=d.policy==='local'?'sel':'';document.getElementById('b-auto').className=d.policy!=='local'?'sel':'';
document.getElementById('policy').textContent='当前模式: '+(d.policy==='local'?'本地模型':'跟随默认');})}
function setPolicy(m){fetch('/policy',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:m})}).then(()=>refreshQuota())}
function run(){const e=document.getElementById('engine').value,t=document.getElementById('task').value;
document.getElementById('out').textContent='执行中…';
fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({handler:e,task:t})}).then(r=>r.json()).then(d=>{document.getElementById('out').textContent=JSON.stringify(d,null,2)}).catch(e=>document.getElementById('out').textContent='错误: '+e)}
refreshQuota();setInterval(refreshQuota,60000);
</script></body></html>"""


CHAT_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>Bridge Chat</title>
<style>
body{font-family:Segoe UI,sans-serif;max-width:760px;margin:0 auto;height:100vh;display:flex;flex-direction:column;background:#111;color:#eee}
header{padding:12px 16px;background:#1c1c24;display:flex;justify-content:space-between;align-items:center}
header select{background:#0d0d12;color:#eee;border:1px solid #333;border-radius:6px;padding:6px}
#chat{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:10px}
.msg{max-width:80%;padding:10px 14px;border-radius:12px;white-space:pre-wrap;word-break:break-word}
.user{align-self:flex-end;background:#4f6ef7;color:#fff}
.bot{align-self:flex-start;background:#1c1c24;border:1px solid #333}
.meta{font-size:11px;opacity:.6;margin-bottom:4px}
footer{display:flex;gap:8px;padding:12px 16px;background:#1c1c24}
input{flex:1;background:#0d0d12;color:#eee;border:1px solid #333;border-radius:8px;padding:12px}
button{background:#4f6ef7;color:#fff;border:0;border-radius:8px;padding:12px 20px;cursor:pointer}
</style></head><body>
<header><b>Bridge Chat（本地运行，不消耗 Grok 配额）</b>
<select id="engine"><option value="local">本地模型 (Ollama)</option><option value="langgraph">LangGraph 群聊</option><option value="autogen">AutoGen 团队</option><option value="echo">echo</option></select></header>
<div id="chat"></div>
<footer><input id="in" placeholder="输入消息，回车发送…" autofocus
 onkeydown="if(event.key==='Enter')send()"/><button onclick="send()">发送</button></footer>
<script>
const chat=document.getElementById('chat');
function add(role,text,meta){const d=document.createElement('div');d.className='msg '+role;
d.innerHTML=(meta?'<div class="meta">'+meta+'</div>':'')+text.replace(/&/g,'&amp;').replace(/</g,'&lt;');
chat.appendChild(d);chat.scrollTop=chat.scrollHeight;return d}
async function send(){const inp=document.getElementById('in');const t=inp.value.trim();if(!t)return;
inp.value='';add('user',t);
const e=document.getElementById('engine').value;
const w=add('bot','…','engine: '+e);
try{const r=await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({handler:e,task:t})});
const d=await r.json();
let text='';
if(d.engine&&d.engine.startsWith('local'))text=d.reply||'(空)';
else if(d.turns)text=d.turns.map(x=>'['+x.speaker+'] '+x.text).join('\\n\\n');
else text=d.echo!==undefined?d.echo:JSON.stringify(d,null,2);
w.innerHTML='<div class="meta">engine: '+(d.engine||e)+'</div>'+text.replace(/&/g,'&amp;').replace(/</g,'&lt;');}
catch(err){w.textContent='错误: '+err}}
</script></body></html>"""


class StandaloneHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle(self):  # noqa: A003
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # Browsers and local clients routinely cancel keepalive requests;
            # suppress expected socket teardown tracebacks from the daemon.
            return

    def _json(self, code: int, obj: dict) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):  # noqa: N802
        if STANDALONE_TOKEN and self.headers.get("x-bridge-token") != STANDALONE_TOKEN:
            self._json(403, {"error": "missing/wrong x-bridge-token"})
            return
        if self.path == "/run":
            length = int(self.headers.get("content-length") or 0)
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
            except Exception:
                self._json(400, {"error": "invalid json"})
                return
            self._json(200, dispatch(payload))
        elif self.path == "/policy":
            length = int(self.headers.get("content-length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
                mode = body.get("mode")
                if mode not in ("local", "auto", "wait"):
                    raise ValueError(mode)
                runtime["policy"] = mode
                state["policy"] = mode
                bc.save_state(state)
                log(f"[policy] set to {mode}")
                self._json(200, {"ok": True, "policy": mode})
            except Exception as e:
                self._json(400, {"error": str(e)})
        else:
            self._json(404, {"error": "not found (POST /run | /policy)"})

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._json(200, {"ok": True, "label": LABEL, "engines": sorted(HANDLERS), "transports": sorted(TRANSPORTS), "policy": runtime["policy"], "quota": runtime["quota"]})
        elif self.path == "/quota":
            self._json(200, {"quota": runtime["quota"], "policy": runtime["policy"]})
        elif self.path == "/ui":
            data = UI_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/chat":
            data = CHAT_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self._json(404, {"error": "not found (GET /health | /quota | /ui | /chat)"})

    def log_message(self, fmt, *args):  # silence stderr
        pass


def standalone_server() -> None:
    srv = ThreadingHTTPServer((STANDALONE_BIND, STANDALONE_PORT), StandaloneHandler)
    log(f"[standalone] listening on {STANDALONE_BIND}:{STANDALONE_PORT} (POST /run | /policy, GET /health | /quota | /ui)")
    srv.serve_forever()


# ---------- main ----------
TRANSPORTS = set(_cfg.get("transports", ["grok", "standalone"]))
if not TRANSPORTS <= {"grok", "standalone"}:
    raise SystemExit(f"invalid transports in config: {sorted(TRANSPORTS - {'grok', 'standalone'})}")
if not TRANSPORTS:
    raise SystemExit("no transports enabled in config (transports: [])")


def main() -> None:
    log(f"transports: {sorted(TRANSPORTS)} | policy: {runtime['policy']} | default_handler: {bc.config().get('default_handler', 'echo')}")
    if "standalone" in TRANSPORTS:
        threading.Thread(target=standalone_server, daemon=True).start()
    if "grok" in TRANSPORTS:
        grok_loop()
    else:
        log("[grok] transport disabled by config; standalone-only mode (zero Grok API calls)")
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("stopped")
