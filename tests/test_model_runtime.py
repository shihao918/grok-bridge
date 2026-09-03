import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

import model_runtime


class FakeResponse:
    def __init__(self, payload, *, status_error=None):
        self.payload = payload
        self.status_error = status_error

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def json(self):
        return self.payload


class ModelRuntimeTests(unittest.TestCase):
    def write_codex_config(self, directory: str, *, wire_api: str = "responses") -> Path:
        path = Path(directory) / "config.toml"
        path.write_text(
            textwrap.dedent(
                f"""
                model = "gpt-5.6-sol"
                model_provider = "cch"
                model_reasoning_effort = "xhigh"

                [model_providers.cch]
                name = "Sub2API"
                base_url = "https://gateway.example/v1"
                wire_api = "{wire_api}"
                env_key = "SUB2API_USER_API_KEY"
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return path

    def test_default_binding_tracks_the_active_codex_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = self.write_codex_config(directory)
            binding = model_runtime.resolve_model_binding({}, environ={}, codex_config_path=config_path)

        self.assertEqual(binding.backend, "responses")
        self.assertEqual(binding.provider_key, "cch")
        self.assertEqual(binding.base_url, "https://gateway.example/v1")
        self.assertEqual(binding.model, "gpt-5.6-sol")
        self.assertEqual(binding.wire_api, "responses")
        self.assertEqual(binding.reasoning_effort, "xhigh")
        self.assertEqual(binding.auth_env, "SUB2API_USER_API_KEY")

    def test_codex_binding_rejects_cross_wire_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = self.write_codex_config(directory, wire_api="chat_completions")
            with self.assertRaisesRegex(model_runtime.ModelConfigurationError, "cross-wire fallback"):
                model_runtime.resolve_model_binding({}, environ={}, codex_config_path=config_path)

    def test_responses_request_uses_codex_shape_without_storing_output(self):
        captured = {}
        binding = model_runtime.ModelBinding(
            backend="responses",
            source="test",
            provider_key="cch",
            base_url="https://gateway.example/v1",
            model="gpt-5.6-sol",
            wire_api="responses",
            reasoning_effort="xhigh",
            auth_env="SUB2API_USER_API_KEY",
        )

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured.update(kwargs)
            return FakeResponse({"output_text": "codex reply"})

        reply = model_runtime.execute_model(
            binding,
            "hello",
            post=fake_post,
            environ={"SUB2API_USER_API_KEY": "test-secret"},
        )

        self.assertEqual(reply, "codex reply")
        self.assertEqual(captured["url"], "https://gateway.example/v1/responses")
        self.assertEqual(captured["headers"], {"Authorization": "Bearer test-secret"})
        self.assertEqual(
            captured["json"],
            {
                "model": "gpt-5.6-sol",
                "input": "hello",
                "stream": False,
                "store": False,
                "reasoning": {"effort": "xhigh"},
            },
        )
        self.assertFalse(captured["trust_env"])

    def test_nested_responses_output_is_joined(self):
        payload = {
            "output": [
                {"content": [{"type": "output_text", "text": "first"}]},
                {"content": [{"type": "output_text", "text": {"value": "second"}}]},
            ]
        }
        self.assertEqual(model_runtime.extract_responses_text(payload), "first\nsecond")

    def test_missing_auth_fails_without_calling_provider(self):
        binding = model_runtime.ModelBinding(
            backend="responses",
            source="test",
            provider_key="cch",
            base_url="https://gateway.example/v1",
            model="gpt-5.6-sol",
            wire_api="responses",
            auth_env="SUB2API_USER_API_KEY",
        )

        def unexpected_post(*args, **kwargs):
            self.fail("provider request should not be sent without auth")

        with self.assertRaisesRegex(model_runtime.ModelConfigurationError, "SUB2API_USER_API_KEY"):
            model_runtime.execute_model(binding, "hello", post=unexpected_post, environ={})

    def test_empty_responses_output_is_an_explicit_failure(self):
        binding = model_runtime.ModelBinding(
            backend="responses",
            source="test",
            provider_key="cch",
            base_url="https://gateway.example/v1",
            model="gpt-5.6-sol",
            wire_api="responses",
            auth_env="KEY",
        )
        with self.assertRaisesRegex(model_runtime.ModelExecutionError, "no output text"):
            model_runtime.execute_model(
                binding,
                "hello",
                post=lambda *args, **kwargs: FakeResponse({"output": []}),
                environ={"KEY": "secret"},
            )

    def test_remote_http_responses_request_fails_before_sending_bearer_key(self):
        binding = model_runtime.ModelBinding(
            backend="responses",
            source="test",
            provider_key="cch",
            base_url="http://gateway.example/v1",
            model="gpt-5.6-sol",
            wire_api="responses",
            auth_env="KEY",
        )

        def unexpected_post(*_args, **_kwargs):
            self.fail("provider request should not be sent over non-loopback HTTP")

        with self.assertRaisesRegex(model_runtime.ModelConfigurationError, "non-loopback HTTP"):
            model_runtime.execute_model(
                binding,
                "hello",
                post=unexpected_post,
                environ={"KEY": "secret"},
            )

    def test_remote_http_responses_request_requires_explicit_opt_in(self):
        captured = {}
        binding = model_runtime.ModelBinding(
            backend="responses",
            source="test",
            provider_key="cch",
            base_url="http://gateway.example/v1",
            model="gpt-5.6-sol",
            wire_api="responses",
            auth_env="KEY",
        )

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured.update(kwargs)
            return FakeResponse({"output_text": "explicit insecure route"})

        reply = model_runtime.execute_model(
            binding,
            "hello",
            post=fake_post,
            environ={"KEY": "secret", "GROK_ALLOW_INSECURE_REMOTE_HTTP": "1"},
        )

        self.assertEqual(reply, "explicit insecure route")
        self.assertEqual(captured["url"], "http://gateway.example/v1/responses")

    def test_ollama_remains_an_explicit_opt_in_backend(self):
        captured = {}
        binding = model_runtime.resolve_model_binding(
            {"model_backend": "ollama", "ollama_url": "http://127.0.0.1:11434"},
            environ={},
        )

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured["json"] = kwargs["json"]
            return FakeResponse({"message": {"content": "ollama reply"}})

        reply = model_runtime.execute_model(binding, "hello", post=fake_post, environ={})
        self.assertEqual(reply, "ollama reply")
        self.assertEqual(captured["url"], "http://127.0.0.1:11434/api/chat")
        self.assertEqual(captured["json"]["model"], "lfm2.5:8b-a1b")

    def test_safe_summary_reports_auth_presence_not_secret_value(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = self.write_codex_config(directory)
            summary = model_runtime.model_runtime_summary(
                {},
                environ={"SUB2API_USER_API_KEY": "must-not-appear"},
                codex_config_path=config_path,
            )

        self.assertTrue(summary["authAvailable"])
        self.assertEqual(summary["authEnv"], "SUB2API_USER_API_KEY")
        self.assertNotIn("must-not-appear", str(summary))

    def test_cli_uses_the_explicit_codex_config(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = self.write_codex_config(directory)
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(model_runtime.__file__).resolve()),
                    "--require-auth",
                    "--codex-config",
                    str(config_path),
                ],
                cwd=Path(model_runtime.__file__).resolve().parent,
                env=dict(os.environ, SUB2API_USER_API_KEY="cli-test-secret"),
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["providerKey"], "cch")
        self.assertEqual(payload["model"], "gpt-5.6-sol")
        self.assertIn(str(config_path), payload["source"])


if __name__ == "__main__":
    unittest.main()
