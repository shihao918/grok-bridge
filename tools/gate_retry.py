"""Retry send 3x to check if 503 is persistent (quota gate) or transient."""
import json
import sys
import time
import uuid

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
AGENT = "<deleted-test-agent-id>"

for attempt in range(3):
    r = httpx.post(
        f"{BASE}/SendGrokBotUserMessage",
        headers=headers,
        json={
            "agentId": AGENT,
            "messageId": str(uuid.uuid4()),
            "text": f"gate diagnostic attempt {attempt+1}",
            "sentAtMs": int(time.time() * 1000),
            "isFork": False,
        },
        timeout=30,
        trust_env=False,
    )
    try:
        d = r.json()
        delivery = d.get("delivery") or (d.get("details") or {})
        print(f"attempt {attempt+1}: {r.status_code} delivery={d.get('delivery', d.get('code'))} {r.text[:120]}")
    except Exception:
        print(f"attempt {attempt+1}: {r.status_code} {r.text[:120]}")
    time.sleep(5)

