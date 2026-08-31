"""Full acceptance test: system works despite account quota state."""
import json
import sys
import time

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

c = httpx.Client(timeout=300, trust_env=False)
results = []


def check(name, fn):
    try:
        ok, detail = fn()
        results.append((name, "PASS" if ok else "FAIL", detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    except Exception as e:
        results.append((name, "FAIL", f"{type(e).__name__}: {str(e)[:80]}"))
        print(f"[FAIL] {name}: {type(e).__name__}: {str(e)[:80]}")


def t_health():
    h = c.get("http://127.0.0.1:18083/health", timeout=10).json()
    return h.get("ok") is True and "local" in h.get("engines", []), f"engines={h['engines']} transports={h['transports']}"


def t_quota():
    q = c.get("http://127.0.0.1:18083/quota", timeout=10).json()
    d = q.get("quota") or {}
    return d.get("exhausted") is True, f"pct={d.get('usagePercent')} plan={d.get('plan')} policy={q.get('policy')}"


def t_echo():
    r = c.post("http://127.0.0.1:18083/run", json={"handler": "echo", "task": "acceptance"}, timeout=15)
    d = r.json()
    return d.get("ok") is True and d.get("engine") == "echo", "round-trip"


def t_local():
    r = c.post("http://127.0.0.1:18083/run", json={"handler": "local", "task": "用中文一句话：本地算力的价值"}, timeout=300)
    d = r.json()
    return d.get("ok") is True and len(d.get("reply", "")) > 5, f"reply: {d.get('reply', '')[:80]}"


def t_langgraph():
    r = c.post("http://127.0.0.1:18083/run", json={"handler": "langgraph", "task": "验收测试：一句话说明多智能体分工"}, timeout=300)
    d = r.json()
    turns = d.get("turns", [])
    speakers = [t["speaker"] for t in turns]
    return d.get("ok") is True and len(turns) >= 3, f"turns={len(turns)} speakers={speakers}"


def t_grok_gate():
    """Document the current Grok dispatch gate state (expected: still gated)."""
    headers = {
        "authorization": f"Bearer {bc.get_grok_access_token()}",
        "content-type": "application/json",
        "connect-protocol-version": "1",
    }
    state = bc.load_state()
    frame = {"requestId": "acc-test", "exec": {"serverMessageJson": '{"handler":"echo","task":"x"}', "approvalId": ""}}
    r = c.post(
        "https://api2.cursor.sh/aiserver.v1.GrokBotService/OpenGrokBotUserComputerRequest",
        headers={**headers, "content-type": "application/connect+json"},
        content=json.dumps({"machineId": state["machine_id"], "frame": frame, "idempotencyKey": "acc-test"}).encode(),
        timeout=30,
    )
    return r.status_code == 200, f"open -> {r.status_code} (control plane)"


check("daemon /health", t_health)
check("quota watcher (live 100%)", t_quota)
check("standalone echo", t_echo)
check("local model (Ollama, own compute)", t_local)
check("langgraph 3-agent (gateway compute)", t_langgraph)
check("grok channel control plane", t_grok_gate)

print("\n==== SUMMARY ====")
passed = sum(1 for _, s, _ in results if s == "PASS")
for name, s, d in results:
    print(f"  {s}: {name}")
print(f"\n{passed}/{len(results)} passed")
