"""Diagnostic: where is the quota gate — dispatch or inference?"""
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
c = httpx.Client(timeout=30, trust_env=False)

# 1. create a throwaway agent (caller supplies ids)
aid = str(uuid.uuid4())
r = c.post(
    f"{BASE}/CreateGrokBotAgent",
    headers=headers,
    json={
        "agentId": aid,
        "legacyAgentId": aid,
        "name": "quota-gate-test",
        "description": "diagnostic",
        "title": "Quota Gate Test",
        "avatarShape": "robot",
        "avatarColor": "#4f6ef7",
    },
)
print("create:", r.status_code, r.text[:300])
agent_id = aid
if not agent_id:
    print("no agent id; abort")
    sys.exit(0)
print("agent_id:", agent_id)

# 2. send a user message to it (quota is 100%)
now_ms = int(time.time() * 1000)
r = c.post(
    f"{BASE}/SendGrokBotUserMessage",
    headers=headers,
    json={
        "agentId": agent_id,
        "messageId": str(uuid.uuid4()),
        "text": "gate diagnostic: respond with one word",
        "sentAtMs": now_ms,
        "isFork": False,
    },
)
print(f"send -> {r.status_code} {r.text[:400]}")
