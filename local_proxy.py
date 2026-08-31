"""Local reverse proxy with multi-upstream failover: 127.0.0.1:18082.

Config (state/config.json):
  {
    "upstreams": [
      {"name": "primary",  "url": "http://host/v1"},
      {"name": "fallback", "url": "https://other/v1"}
    ]
  }
Backward compatible: a single "gateway" string is treated as one upstream.

Keys (optional, per upstream): state/gateway_keys.bin = DPAPI(JSON {name: key}).
When a key exists for an upstream, the proxy overrides the Authorization header;
otherwise the client's own header is forwarded unchanged.

Failover triggers: connect errors/timeouts and 5xx/429. Other 4xx pass through.
The last successful upstream is persisted (sticky) in state/gateway_state.json.
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge_common as bc  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = bc.STATE_DIR
CONFIG = os.path.join(STATE_DIR, "config.json")
KEYS_FILE = os.path.join(STATE_DIR, "gateway_keys.bin")
LAST_GOOD_FILE = os.path.join(STATE_DIR, "gateway_state.json")

HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host"}
FAILOVER_STATUS = {429, 500, 502, 503, 504}

conf = json.load(open(CONFIG, encoding="utf-8")) if os.path.exists(CONFIG) else {}
_upstreams = conf.get("upstreams")
if not _upstreams:
    _g = conf.get("gateway", "")
    _upstreams = [{"name": "primary", "url": _g}] if _g else []
UPSTREAMS = [{"name": u.get("name", f"u{i}"), "url": u["url"].rstrip("/")} for i, u in enumerate(_upstreams)]
if not UPSTREAMS:
    raise SystemExit("state/config.json missing 'upstreams' (or legacy 'gateway')")

_keys = {}
if os.path.exists(KEYS_FILE):
    _keys = json.loads(bc.dpapi_unprotect(open(KEYS_FILE, "rb").read()).decode("utf-8"))

_last_good_lock = threading.Lock()


def last_good() -> str | None:
    if os.path.exists(LAST_GOOD_FILE):
        try:
            return json.load(open(LAST_GOOD_FILE, encoding="utf-8")).get("last_good")
        except Exception:
            return None
    return None


def set_last_good(name: str) -> None:
    with _last_good_lock:
        json.dump({"last_good": name}, open(LAST_GOOD_FILE, "w", encoding="utf-8"))


def ordered_upstreams() -> list[dict]:
    lg = last_good()
    if lg:
        for i, u in enumerate(UPSTREAMS):
            if u["name"] == lg:
                return [u] + UPSTREAMS[:i] + UPSTREAMS[i + 1 :]
    return list(UPSTREAMS)


def target_path(path: str, url: str) -> str:
    if url.endswith("/v1") and path.startswith("/v1/"):
        return path[len("/v1"):]
    return path


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _forward(self, method: str) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else None
        base_headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}

        last_error = "no upstreams"
        for u in ordered_upstreams():
            headers = dict(base_headers)
            key = _keys.get(u["name"])
            if key:
                headers["authorization"] = f"Bearer {key}"
            url = u["url"] + target_path(self.path, u["url"])
            try:
                import httpx

                resp = httpx.request(
                    method, url, headers=headers, content=body, timeout=180.0, trust_env=False
                )
                if resp.status_code in FAILOVER_STATUS and len(UPSTREAMS) > 1:
                    last_error = f"{u['name']}: HTTP {resp.status_code}"
                    continue
                data = resp.content
                self.send_response(resp.status_code)
                for k, v in resp.headers.items():
                    if k.lower() in {"content-length", "transfer-encoding", "connection", "content-encoding"}:
                        continue
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                set_last_good(u["name"])
                return
            except Exception as e:  # connect/timeout errors → next upstream
                last_error = f"{u['name']}: {type(e).__name__}: {str(e)[:120]}"
                continue

        msg = json.dumps({"proxy_error": "all upstreams failed", "detail": last_error}).encode()
        self.send_response(502)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)

    def do_GET(self):  # noqa: N802
        self._forward("GET")

    def do_POST(self):  # noqa: N802
        self._forward("POST")

    def log_message(self, fmt, *args):  # silence stderr
        pass


if __name__ == "__main__":
    names = ", ".join(u["name"] for u in UPSTREAMS)
    print(f"upstreams: {names} (sticky last-good)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 18082), Handler).serve_forever()
