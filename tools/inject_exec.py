"""Example: inject an exec frame into a registered UserComputer bridge machine.

Usage:
  python tools/inject_exec.py <handler> "<task>"

Requires state/bridge_state.json (machine_id) and a logged-in Grok Bot desktop app.
"""

import json
import struct
import sys
import os

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge_common as bc  # noqa: E402

BASE = "https://api2.cursor.sh/aiserver.v1.GrokBotService"


def envelope(payload: bytes) -> bytes:
    return struct.pack(">BI", 0, len(payload)) + payload


def main() -> None:
    handler = sys.argv[1] if len(sys.argv) > 1 else "echo"
    task = sys.argv[2] if len(sys.argv) > 2 else "hello"
    state = bc.load_state()
    machine_id = state["machine_id"]

    frame = {
        "requestId": f"inj-{uuid.uuid4().hex[:8]}",
        "exec": {"serverMessageJson": json.dumps({"handler": handler, "task": task}), "approvalId": ""},
    }
    req = {"machineId": machine_id, "frame": frame, "idempotencyKey": f"inj-{uuid.uuid4().hex[:8]}"}

    headers = {
        "authorization": f"Bearer {bc.get_grok_access_token()}",
        "content-type": "application/connect+json",
        "connect-protocol-version": "1",
    }

    with httpx.Client(timeout=120, trust_env=False) as c:
        with c.stream("POST", f"{BASE}/OpenGrokBotUserComputerRequest", headers=headers, content=envelope(json.dumps(req).encode())) as r:
            print(f"open -> {r.status_code}")
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
                        print("result:", json.dumps(json.loads(ev["client"]["messageJson"]), ensure_ascii=False, indent=2)[:800])
                    elif "error" in ev:
                        print("stream error:", ev["error"].get("code"))
                except Exception:
                    pass


if __name__ == "__main__":
    import uuid

    main()
