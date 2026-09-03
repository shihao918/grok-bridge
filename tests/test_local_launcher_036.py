import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "tools" / "start_grok_bot_036_local.ps1"
PWSH = shutil.which("pwsh")


@unittest.skipUnless(PWSH, "PowerShell 7 is required")
class LocalLauncher036Tests(unittest.TestCase):
    def run_launcher(self, *args: str, env=None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                PWSH,
                "-NoLogo",
                "-NoProfile",
                "-File",
                str(LAUNCHER),
                *args,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
            env=env,
        )

    def write_codex_config(
        self,
        directory: str,
        *,
        wire_api: str = "responses",
        base_url: str = "https://gateway.example/v1",
    ) -> Path:
        path = Path(directory) / "config.toml"
        path.write_text(
            textwrap.dedent(
                f"""
                model = "gpt-5.6-sol"
                model_provider = "cch"
                model_reasoning_effort = "xhigh"

                [model_providers.cch]
                name = "Sub2API"
                base_url = "{base_url}"
                wire_api = "{wire_api}"
                env_key = "SUB2API_USER_API_KEY"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return path

    def test_dry_run_binds_the_app_to_the_local_gateway(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "Grok Bot.exe"
            app.touch()
            codex_config = self.write_codex_config(directory)
            env = dict(os.environ, SUB2API_USER_API_KEY="launcher-test-secret")
            result = self.run_launcher(
                "-DryRun",
                "-SkipBackendHealthCheck",
                "-AppPath",
                str(app),
                "-CodexConfigPath",
                str(codex_config),
                env=env,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["gatewayUrl"], "http://127.0.0.1:9000")
        self.assertEqual(
            payload["environment"]["SAND_HOST_GATEWAY_URL"],
            "http://127.0.0.1:9000",
        )
        self.assertEqual(payload["environment"]["GROK_MODEL_BACKEND"], "codex")
        self.assertEqual(payload["modelBinding"]["providerKey"], "cch")
        self.assertEqual(payload["modelBinding"]["model"], "gpt-5.6-sol")
        self.assertEqual(payload["modelBinding"]["wireApi"], "responses")
        self.assertEqual(payload["modelBinding"]["reasoningEffort"], "xhigh")
        self.assertTrue(payload["modelBinding"]["authAvailable"])
        self.assertNotIn("launcher-test-secret", result.stdout)
        self.assertTrue(payload["restartExisting"])

    def test_remote_gateway_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "Grok Bot.exe"
            app.touch()
            result = self.run_launcher(
                "-DryRun",
                "-SkipBackendHealthCheck",
                "-AppPath",
                str(app),
                "-GatewayUrl",
                "https://example.com",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("loopback", result.stderr.lower())

    def test_codex_cross_wire_binding_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "Grok Bot.exe"
            app.touch()
            codex_config = self.write_codex_config(directory, wire_api="chat_completions")
            result = self.run_launcher(
                "-DryRun",
                "-SkipBackendHealthCheck",
                "-AppPath",
                str(app),
                "-CodexConfigPath",
                str(codex_config),
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cross-wire fallback", result.stderr)

    def test_remote_http_provider_requires_explicit_launcher_switch(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "Grok Bot.exe"
            app.touch()
            codex_config = self.write_codex_config(
                directory,
                base_url="http://gateway.example/v1",
            )
            env = dict(os.environ, SUB2API_USER_API_KEY="launcher-test-secret")
            rejected = self.run_launcher(
                "-DryRun",
                "-SkipBackendHealthCheck",
                "-AppPath",
                str(app),
                "-CodexConfigPath",
                str(codex_config),
                env=env,
            )
            allowed = self.run_launcher(
                "-DryRun",
                "-SkipBackendHealthCheck",
                "-AppPath",
                str(app),
                "-CodexConfigPath",
                str(codex_config),
                "-AllowInsecureRemoteHttpProvider",
                env=env,
            )

        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("non-loopback HTTP", rejected.stderr)
        self.assertEqual(allowed.returncode, 0, allowed.stderr)
        payload = json.loads(allowed.stdout)
        self.assertFalse(payload["modelBinding"]["transportSecure"])
        self.assertTrue(payload["modelBinding"]["transportAllowed"])
        self.assertTrue(payload["allowInsecureRemoteHttpProvider"])

    def test_legacy_backend_without_model_runtime_contract_requests_restart(self):
        class LegacyBackendHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'{"ok":true}' if self.path == "/health" else b"{}"
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format, *_args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), LegacyBackendHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                app = Path(directory) / "Grok Bot.exe"
                app.touch()
                codex_config = self.write_codex_config(directory)
                env = dict(os.environ, SUB2API_USER_API_KEY="launcher-test-secret")
                result = self.run_launcher(
                    "-AppPath",
                    str(app),
                    "-CodexConfigPath",
                    str(codex_config),
                    "-GatewayUrl",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "-NoRestartBackend",
                    "-NoRestartExisting",
                    env=env,
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("different or unknown model binding", result.stderr)
        self.assertNotIn("property 'ok' cannot be found", result.stderr.lower())

    def test_foreign_listener_with_backend_path_argument_is_not_terminated(self):
        server_code = textwrap.dedent(
            """
            from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    body = b'{"ok":true}' if self.path == "/health" else b"{}"
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, _format, *_args):
                    return

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            print(server.server_address[1], flush=True)
            server.serve_forever()
            """
        )
        foreign = subprocess.Popen(
            [sys.executable, "-c", server_code, str(ROOT / "backend_server.py")],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        try:
            self.assertIsNotNone(foreign.stdout)
            port = int(foreign.stdout.readline().strip())
            with tempfile.TemporaryDirectory() as directory:
                app = Path(directory) / "Grok Bot.exe"
                app.touch()
                codex_config = self.write_codex_config(directory)
                env = dict(os.environ, SUB2API_USER_API_KEY="launcher-test-secret")
                result = self.run_launcher(
                    "-AppPath",
                    str(app),
                    "-CodexConfigPath",
                    str(codex_config),
                    "-GatewayUrl",
                    f"http://127.0.0.1:{port}",
                    "-NoStartBackend",
                    env=env,
                )

            self.assertIsNone(
                foreign.poll(),
                "launcher terminated a foreign listener whose command line merely mentioned backend_server.py",
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("refusing to stop", result.stderr.lower())
        finally:
            if foreign.poll() is None:
                foreign.terminate()
                try:
                    foreign.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    foreign.kill()
                    foreign.wait(timeout=5)
            if foreign.stdout is not None:
                foreign.stdout.close()
            if foreign.stderr is not None:
                foreign.stderr.close()


if __name__ == "__main__":
    unittest.main()
