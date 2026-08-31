"""Post-migration verification: inject echo + langgraph exec frames via grok-bridge token derivation."""
import json
import struct
import sys
import time
import uuid

import httpx

sys.path.insert(0, r"C:\Users\Dean\Code\GitHub\grok-bridge")
import bridge_common as bc  # noqa: E402

token = bc.get_grok_access_token()
state = bc.load_state()
MACHINE = state["machine_id"]
URL = "https://api2.cursor.sh/aiserver.v1.GrokBotService/OpenGrokBotUserComputerRequest"

headers = {
    "authorization": f"Bearer {token}",
    "content-type": "application/connect+json",
    "connect-protocol-version": "1",
    "x-ghost-mode": "true",
}


def inject(handler: str, task: str):
    frame = {
        "requestId": f"mig-{uuid.uuid4().hex[:8]}",
        "exec": {
            "serverMessageJson": json.dumps({"handler": handler, "task": task}),
            "approvalId": "",
        },
    }
    req = {"machineId": MACHINE, "frame": frame, "idempotencyKey": f"mig-{uuid.uuid4().hex[:8]}"}
    with httpx.Client(timeout=60, trust_env=False) as c:
        with c.stream("POST", URL, headers=headers, content=envelope_frames(json.dumps(req).encode())) as r:
            print(f"[{handler}] open -> {r.status_code}")
            buf = b""
            for chunk in r.iter_raw():
                buf += chunk
            i = 0
            while i + 5 <= len(buf):
                (ln,) = struct.unpack(">I", buf[i + 1 : i + 5])
                payload = buf[i + 5 : i + 5 + ln]
                i += 5 + ln
                try:
                    ev = json.loads(payload)
                    if "client" in ev:
                        res = json.loads(ev["client"]["messageJson"])
                        print(f"  RESULT ok={res.get('ok')} engine={res.get('engine')} turns={len(res.get('turns', []))}")
                        for t in res.get("turns", [])[:3]:
                            print(f"    [{t['speaker']}] {t['text'][:80]}")
                    elif "error" in ev:
                        print(f"  (stream error: {ev['error'].get('code')})")
                except Exception:
                    pass


def envelope_frames(payload: bytes) -> bytes:
    return struct.pack(">BI", 0, len(payload)) + payload


inject("echo", "post-migration self check")
print("waiting for langgraph run...", flush=True)
inject("langgraph", "Post-migration check: confirm the bridge works. One short sentence.")
