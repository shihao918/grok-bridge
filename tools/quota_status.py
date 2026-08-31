"""Query real quota state from the Grok Bot backend."""
import httpx

sys_token = open(r"C:\Users\Dean\Code\GitHub\grok-bridge\state\codex_key.bin", "rb")  # noqa: F841 (unused placeholder)
import sys  # noqa: E402

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

token = bc.get_grok_access_token()
headers = {
    "authorization": f"Bearer {token}",
    "content-type": "application/json",
    "connect-protocol-version": "1",
    "x-ghost-mode": "true",
}
c = httpx.Client(timeout=30, trust_env=False)

endpoints = [
    ("aiserver.v1.DashboardService", "GetSandUsageStatus"),
    ("aiserver.v1.DashboardService", "GetCurrentPeriodUsage"),
    ("aiserver.v1.DashboardService", "GetHardLimit"),
]
for svc, m in endpoints:
    try:
        r = c.post(f"https://api2.cursor.sh/{svc}/{m}", headers=headers, json={})
        print(f"{m} -> {r.status_code} {r.text[:400]}")
    except Exception as e:
        print(f"{m} -> EXC {type(e).__name__}: {str(e)[:100]}")
