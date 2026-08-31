"""Probe Grok backend behavior under quota exhaustion."""
import json
import sys

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

token = bc.get_grok_access_token()
state = bc.load_state()
headers = {
    "authorization": f"Bearer {token}",
    "content-type": "application/json",
    "connect-protocol-version": "1",
    "x-ghost-mode": "true",
}
BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"
c = httpx.Client(timeout=30, trust_env=False)

for m in ("GetGrokBotRuntimeCapabilities", "ListSandBoxes", "ListGrokBotAgents"):
    try:
        r = c.post(f"{BASE}/{m}", headers=headers, json={})
        print(f"{m} -> {r.status_code} {r.text[:140]}")
    except Exception as e:
        print(f"{m} -> EXC {type(e).__name__}: {str(e)[:100]}")

frame = {
    "requestId": "quota-probe-1",
    "exec": {"serverMessageJson": '{"handler": "echo", "task": "quota probe"}', "approvalId": ""},
}
try:
    r = c.post(
        f"{BASE}/OpenGrokBotUserComputerRequest",
        headers={**headers, "content-type": "application/connect+json"},
        content=json.dumps({"machineId": state["machine_id"], "frame": frame, "idempotencyKey": "quota-probe-1"}).encode(),
    )
    print(f"Open(exec) -> {r.status_code} {r.text[:160]}")
except Exception as e:
    print(f"Open(exec) -> EXC {type(e).__name__}: {str(e)[:100]}")
