"""Passive recorder: log every request the Grok Bot app makes to the replacement backend."""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = r"C:\Users\Dean\Code\GitHub\grok-bridge\logs\backend_probe.jsonl"
os.makedirs(os.path.dirname(LOG), exist_ok=True) if (os := __import__("os")) else None


class Recorder(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _log(self, body: bytes = b"") -> None:
        entry = {
            "ts": time.time(),
            "method": self.command,
            "path": self.path,
            "headers": {k: (v[:14] + "..." if k.lower() == "authorization" else v) for k, v in self.headers.items()},
            "body": body.decode("utf-8", errors="replace")[:2000],
        }
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    def _respond(self, code: int = 200, obj=None) -> None:
        data = json.dumps(obj if obj is not None else {}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        self._log()
        self._respond(200, {})

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        self._log(body)
        self._respond(200, {})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("recorder on 127.0.0.1:9000", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 9000), Recorder).serve_forever()
