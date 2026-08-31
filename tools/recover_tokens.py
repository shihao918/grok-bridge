"""Recover real Grok tokens: refresh against the real backend, write back v10-encrypted."""
import base64
import json
import os
import sys
import time

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

REFRESH_SAVED = r"C:\Users\Dean\Code\GitHub\grok-bridge\state\bridge_refresh_token.json"
saved = json.load(open(REFRESH_SAVED, encoding="utf-8"))
refresh_token = saved["refresh_token"]

CLIENT_IDS = ["KbZUR41cY7W6zRSdpSUJ7I7mLYBKOCmB", "OzaBXLClY5CAGxNzUhQ2vlknpi07tGuE"]  # PROD, DEV

new_access = new_refresh = None
for cid in CLIENT_IDS:
    r = httpx.post(
        "https://api2.cursor.sh/oauth/token",
        json={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": cid},
        timeout=30,
        trust_env=False,
    )
    print(f"refresh (client {cid[:8]}...) -> {r.status_code}")
    if r.status_code == 200:
        d = r.json()
        new_access = d["access_token"]
        new_refresh = d.get("refresh_token", refresh_token)
        break

if not new_access:
    print("refresh failed on all client ids")
    sys.exit(1)
print("new access token obtained, len", len(new_access))

# write back into sand-secrets.json in Chromium v10 format (AES-256-GCM with app key)
key = bc._active_account_key()
store_path = bc.SAND_SECRETS
store = json.load(open(store_path, encoding="utf-8"))
accounts = json.loads(store["cursor-accounts"])
acct = accounts["accounts"][accounts["active"]]

from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: E402

nonce = os.urandom(12)


def v10_encrypt(plain: str) -> str:
    aes = AESGCM(key)
    ct = aes.encrypt(nonce, plain.encode("utf-8"), None)
    return base64.b64encode(b"v10" + nonce + ct).decode()


acct["cursor-access-token"] = v10_encrypt(new_access)
if new_refresh:
    acct["cursor-refresh-token"] = v10_encrypt(new_refresh)
store["cursor-accounts"] = json.dumps(accounts, ensure_ascii=False)
with open(store_path, "w", encoding="utf-8") as f:
    f.write(json.dumps(store, ensure_ascii=False))
print("sand-secrets.json updated")

# verify derivation now returns a working token
tok = bc.get_grok_access_token()
r2 = httpx.post(
    "https://api2.cursor.sh/aiserver.v1.GrokBotService/GetGrokBotRuntimeCapabilities",
    headers={"authorization": f"Bearer {tok}", "content-type": "application/json", "connect-protocol-version": "1"},
    json={},
    timeout=30,
    trust_env=False,
)
print("verify ->", r2.status_code, r2.text[:100])
