"""Probe the agent-channel endpoints to characterize the 'reconnecting' state."""
import sys

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

token = bc.get_grok_access_token()
headers = {
    "authorization": f"Bearer {token}",
    "content-type": "application/json",
    "connect-protocol-version": "1",
    "x-ghost-mode": "true",
}
BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"
c = httpx.Client(timeout=20, trust_env=False)

# identity/control (known good)
r = c.post(f"{BASE}/GetGrokBotRuntimeCapabilities", headers=headers, json={})
print(f"GetGrokBotRuntimeCapabilities -> {r.status_code} {r.text[:100]}")

# agent-channel: list agents
r = c.post(f"{BASE}/ListGrokBotAgents", headers=headers, json={})
print(f"ListGrokBotAgents -> {r.status_code} {r.text[:120]}")

# agent-channel: the streaming transcript watch (what the app UI connects to)
try:
    with c.stream("POST", f"{BASE}/WatchGrokBotTranscripts", headers=headers, content=b"", timeout=15) as r:
        print(f"WatchGrokBotTranscripts -> {r.status_code}")
        first = next(r.iter_raw(), b"")
        print("  first bytes:", first[:150])
except Exception as e:
    print(f"WatchGrokBotTranscripts -> EXC {type(e).__name__}: {str(e)[:100]}")
