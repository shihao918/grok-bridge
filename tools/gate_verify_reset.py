"""Scheduled for 2026-09-05 (quota reset day): verify whether the dispatch gate lifts at reset.

Writes results to logs/gate_verify_9_5.log — evidence for the 'gate at dispatch layer' hypothesis.
"""

import json
import sys
import time
import uuid

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\gate_verify_9_5.log"
BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


headers = {
    "authorization": f"Bearer {bc.get_grok_access_token()}",
    "content-type": "application/json",
    "connect-protocol-version": "1",
    "x-ghost-mode": "true",
}
c = httpx.Client(timeout=30, trust_env=False)

# 1. quota state after reset
r = c.post(f"{BASE.rsplit('/', 1)[0]}/DashboardService/GetSandUsageStatus", headers=headers, json={})
log(f"GetSandUsageStatus -> {r.status_code} {r.text[:200]}")

# 2. create + send + observe
aid = str(uuid.uuid4())
r = c.post(
    f"{BASE}/CreateGrokBotAgent",
    headers=headers,
    json={"agentId": aid, "legacyAgentId": aid, "name": "gate-verify", "description": "auto", "title": "Gate Verify", "avatarShape": "robot", "avatarColor": "#22c55e"},
)
log(f"CreateGrokBotAgent -> {r.status_code}")
try:
    agent_id = r.json()["agent"]["agentId"]
except Exception:
    log("no agentId; abort")
    sys.exit(1)

for attempt in range(2):
    r = c.post(
        f"{BASE}/SendGrokBotUserMessage",
        headers=headers,
        json={"agentId": agent_id, "messageId": str(uuid.uuid4()), "text": f"post-reset verification {attempt+1}", "sentAtMs": int(time.time() * 1000), "isFork": False},
    )
    log(f"Send #{attempt+1} -> {r.status_code} {r.text[:200]}")
    time.sleep(5)

# 3. cleanup
r = c.post(f"{BASE}/DeleteGrokBotAgent", headers=headers, json={"id": 16461}, timeout=30, trust_env=False)
log(f"cleanup -> {r.status_code} (numeric id guess; harmless if 400)")
log("VERDICT: if sends are 200/dispatched now but were 503 before reset → gate confirmed at dispatch layer")
