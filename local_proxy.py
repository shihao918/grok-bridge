"""Local reverse proxy: 127.0.0.1:18082 -> Codex gateway (bypasses httpx2 proxy quirks).

Gateway URL from state/config.json; Sub2API key DPAPI-decrypted from state/codex_key.bin.
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(ROOT, "state", "config.json")

conf = json.load(open(CONFIG, encoding="utf-8")) if os.path.exists(CONFIG) else {}
GATEWAY = conf.get("gateway", "").rstrip("/")
HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host"}


def target_path(path: str) -> str:
    if GATEWAY.endswith("/v1") and path.startswith("/v1/"):
        return path[len("/v1"):]
    return path


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _forward(self, method: str) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        try:
            import httpx

            resp = httpx.request(
                method,
                GATEWAY + target_path(self.path),
                headers=headers,
                content=body,
                timeout=180.0,
                trust_env=False,
            )
            data = resp.content
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() in {"content-length", "transfer-encoding", "connection", "content-encoding"}:
                    continue
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:  # noqa: BLE001
            msg = str(e).encode()
            self.send_response(502)
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def do_GET(self):  # noqa: N802
        self._forward("GET")

    def do_POST(self):  # noqa: N802
        self._forward("POST")

    def log_message(self, fmt, *args):  # silence stderr
        pass


if not GATEWAY:
    raise SystemExit("state/config.json missing 'gateway'")

if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 18082), Handler).serve_forever()
