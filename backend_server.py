"""Replacement backend stage 1: OAuth token issuance + request recording.

The 0.30 app treats a localhost backend as a dev backend (DEV_AUTH_CLIENT_ID) and
refreshes its access token here. We mint a JWT access token (client-side it is only
parsed, not signature-verified) and record every other call so we can implement the
next endpoints iteratively.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\backend_calls.jsonl"
os.makedirs(os.path.dirname(LOG), exist_ok=True)

REFRESH_FILE = r"C:\Users\Dean\Code\GitHub\grok-bridge\state\bridge_refresh_token.json"


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def mint_jwt(claims: dict) -> str:
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = b64url(json.dumps(claims).encode())
    sig = b64url(hmac.new(b"grok-bridge-dev-secret", f"{header}.{payload}".encode(), hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"


def handle_oauth_token(body: dict) -> dict:
    # decode the incoming refresh token to reuse its identity claims
    sub, email = "grok|bridge-user", None
    rt = body.get("refresh_token")
    if rt:
        try:
            payload = json.loads(b64url_dec(rt.split(".")[1]).decode())
            sub = payload.get("sub", sub)
            email = payload.get("email")
            json.dump({"refresh_token": rt, "sub": sub}, open(REFRESH_FILE, "w"))
        except Exception:
            pass
    now = int(time.time())
    access = mint_jwt({"sub": sub, **({"email": email} if email else {}), "exp": now + 3600 * 24, "iss": "grok-bridge"})
    return {
        "access_token": access,
        "token_type": "Bearer",
        "expires_in": 3600 * 24,
        "refresh_token": rt or mint_jwt({"sub": sub, "exp": now + 3600 * 24 * 30}),
        "scope": "openid profile email offline_access",
    }


# M2: identity trio responses (shapes from dashboard_pb.ts / privacy_mode enum)
PRIVACY_MODE_NO_STORAGE = 1

RESPONSES = {
    "/aiserver.v1.DashboardService/GetMe": {
        "authId": "grok|bridge-user",
        "userId": 1,
        "email": "bridge@local",
        "firstName": "Bridge",
        "lastName": "Local",
        "isEnterpriseUser": False,
    },
    "/aiserver.v1.DashboardService/GetTeams": {"teams": []},
    "/aiserver.v1.DashboardService/GetUserPrivacyMode": {
        "privacyMode": PRIVACY_MODE_NO_STORAGE,
        "hoursRemainingInGracePeriod": 0,
        "isEnforcedByTeam": False,
        "isNotMigratedToServerSourceOfTruth": False,
        "partnerDataShare": False,
        "hasAcknowledgedGracePeriodDisclaimer": True,
    },
    "/aiserver.v1.GrokBotService/EnsureSandBox": {
        "cluster": "local",
        "tenantId": "local-tenant",
        "podId": "local-pod",
        "networkToken": "",
        "execDaemonAuthToken": "bridge-local-exec-token",
    },
    "/aiserver.v1.DashboardService/GetHardLimit": {"noUsageBasedAllowed": True},
    "/aiserver.v1.DashboardService/GetCurrentPeriodUsage": {
        "billingCycleStart": str(int(time.time() * 1000) - 7 * 86400_000),
        "billingCycleEnd": str(int(time.time() * 1000) + 7 * 86400_000),
        "planUsage": {"autoPercentUsed": 0, "apiPercentUsed": 0, "totalPercentUsed": 0},
        "spendLimitUsage": {"pooledLimit": 0, "pooledRemaining": 0, "individualLimit": 0, "limitType": "user", "overallLimit": 0},
    },
    "/aiserver.v1.DashboardService/GetSandUsageStatus": {
        "currentPeriodStart": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "nextResetTimestampUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 5 * 86400)),
        "usagePercent": 0,
        "hasAvailableUsage": True,
        "hasNonZeroIncludedLimit": True,
        "grokPlanLabel": "Bridge Local",
    },
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _record(self, body: bytes, status: int) -> None:
        entry = {
            "ts": time.time(),
            "method": self.command,
            "path": self.path,
            "status": status,
            "auth": (self.headers.get("authorization") or "")[:14] or None,
            "body": body.decode("utf-8", errors="replace")[:1500],
        }
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    def _reply(self, status: int, obj) -> None:
        data = json.dumps(obj if obj is not None else {}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        self._record(b"", 200)
        self._reply(200, {})

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        if self.path == "/oauth/token":
            try:
                req = json.loads(body)
            except Exception:
                req = {}
            resp = handle_oauth_token(req)
            self._record(body, 200)
            self._reply(200, resp)
            return
        if self.path in RESPONSES:
            self._record(body, 200)
            self._reply(200, RESPONSES[self.path])
            return
        # everything else: acknowledge and record for iteration
        self._record(body, 200)
        self._reply(200, {})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("grok-bridge replacement backend (stage 1: oauth) on 127.0.0.1:9000", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
