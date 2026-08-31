"""Delete the test agent with correct field name (id)."""
import sys

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

headers = {
    "authorization": f"Bearer {bc.get_grok_access_token()}",
    "content-type": "application/json",
    "connect-protocol-version": "1",
}
r = httpx.post(
    "https://api2.cursor.sh/aiserver.v1.GrokBotService/DeleteGrokBotAgent",
    headers=headers,
    json={"id": 16460},
    timeout=30,
    trust_env=False,
)
print("delete ->", r.status_code, r.text[:120])
