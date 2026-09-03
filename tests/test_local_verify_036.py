import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERIFY = ROOT / "tools" / "verify_local_036.ps1"
PWSH = shutil.which("pwsh")
RELEASE_TARGETS = {
    ".github/workflows/ci.yml",
    ".gitignore",
    "README.md",
    "backend_server.py",
    "CHANGELOG.md",
    "config.example.json",
    "docs/current-vs-original-2026-09-01.md",
    "docs/handlers.md",
    "model_runtime.py",
    "scripts/secret_scan.py",
    "tests/test_connect_stream.py",
    "tests/test_local_launcher_036.py",
    "tests/test_local_verify_036.py",
    "tests/test_model_runtime.py",
    "tests/test_release_policy.py",
    "tests/test_renderer_patch_036.py",
    "tests/test_secret_scan.py",
    "TODOS.md",
    "tools/patch_local_routing_036.py",
    "tools/patch_renderer_036.py",
    "tools/start_grok_bot_036_local.ps1",
    "tools/verify_local_036.ps1",
    "VERSION",
}


@unittest.skipUnless(PWSH, "PowerShell 7 is required")
class LocalVerify036ContractTests(unittest.TestCase):
    def dry_run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                PWSH,
                "-NoLogo",
                "-NoProfile",
                "-File",
                str(VERIFY),
                "-DryRun",
                *args,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )

    def test_plan_uses_a_local_gateway_without_sending_a_provider_request(self):
        result = self.dry_run()
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(plan["version"], "0.36.0")
        self.assertEqual(plan["mode"], "plan-only")
        self.assertFalse(plan["localOnly"])
        self.assertEqual(plan["networkPolicy"], "loopback-gateway-with-explicit-model-provider")
        self.assertFalse(plan["startsGui"])
        self.assertFalse(plan["startsProvider"])
        self.assertFalse(plan["sendsProviderRequest"])
        self.assertFalse(plan["allowInsecureRemoteHttpProvider"])

        serialized = json.dumps(plan, ensure_ascii=False).lower()
        self.assertNotIn("api2.cursor.sh", serialized)
        self.assertNotIn("https://", serialized)

    def test_plan_covers_the_complete_local_verification_sequence(self):
        result = self.dry_run()
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        steps = {step["name"]: step for step in plan["steps"]}
        self.assertEqual(
            list(steps),
            [
                "routing_check",
                "renderer_check",
                "model_binding_check",
                "launcher_dry_run",
                "python_compile",
                "unit_tests",
                "secret_scan_tracked",
                "secret_scan_staged",
                "secret_scan_write_set",
                "git_diff_check",
            ],
        )

        routing_args = steps["routing_check"]["arguments"]
        self.assertTrue(routing_args[0].endswith("patch_local_routing_036.py"))
        self.assertIn("--check", routing_args)

        renderer_args = steps["renderer_check"]["arguments"]
        self.assertTrue(renderer_args[0].endswith("patch_renderer_036.py"))
        self.assertIn("--check", renderer_args)

        model_args = steps["model_binding_check"]["arguments"]
        self.assertTrue(model_args[0].endswith("model_runtime.py"))
        self.assertIn("--require-auth", model_args)
        self.assertIn("--codex-config", model_args)

        launcher_args = steps["launcher_dry_run"]["arguments"]
        self.assertIn("-DryRun", launcher_args)
        self.assertIn("-SkipBackendHealthCheck", launcher_args)
        self.assertIn("-NoStartBackend", launcher_args)
        self.assertNotIn("-AllowInsecureRemoteHttpProvider", launcher_args)

        self.assertEqual(
            steps["unit_tests"]["arguments"][:4],
            ["-m", "unittest", "discover", "-s"],
        )
        self.assertEqual(steps["git_diff_check"]["arguments"], ["diff", "--check"])
        self.assertTrue(steps["secret_scan_tracked"]["arguments"][0].endswith("scripts\\secret_scan.py"))
        self.assertIn("--staged", steps["secret_scan_staged"]["arguments"])
        self.assertIn("--write-set", steps["secret_scan_write_set"]["arguments"])
        write_set_args = [arg.replace("\\", "/") for arg in steps["secret_scan_write_set"]["arguments"]]
        self.assertEqual(set(write_set_args[2:]), RELEASE_TARGETS)
        self.assertNotIn("tools/patch_renderer_030.py", write_set_args)

    def test_runtime_contract_rejects_unpatched_routing(self):
        source = VERIFY.read_text(encoding="utf-8")
        self.assertIn('if ($routingText -match "(?m)^needs-patch:")', source)
        self.assertIn("did not verify both Grok Bot 0.36 main bundles", source)
        self.assertIn("SAND_HOST_GATEWAY_URL", source)
        self.assertIn("model_binding_check", source)
        self.assertNotIn("acceptance_test.py", source)

    def test_plan_records_explicit_insecure_remote_http_opt_in_without_sending_request(self):
        result = self.dry_run("-AllowInsecureRemoteHttpProvider")
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertTrue(plan["allowInsecureRemoteHttpProvider"])
        self.assertFalse(plan["sendsProviderRequest"])
        launcher = next(step for step in plan["steps"] if step["name"] == "launcher_dry_run")
        self.assertIn("-AllowInsecureRemoteHttpProvider", launcher["arguments"])

    def test_plan_threads_an_explicit_codex_config_into_the_launcher(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.toml"
            config_path.write_text('model_provider = "cch"\n', encoding="utf-8")
            result = self.dry_run("-CodexConfigPath", str(config_path))

        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(Path(plan["codexConfigPath"]), config_path.resolve())
        launcher = next(step for step in plan["steps"] if step["name"] == "launcher_dry_run")
        config_index = launcher["arguments"].index("-CodexConfigPath")
        self.assertEqual(Path(launcher["arguments"][config_index + 1]), config_path.resolve())
        model_step = next(step for step in plan["steps"] if step["name"] == "model_binding_check")
        config_index = model_step["arguments"].index("--codex-config")
        self.assertEqual(Path(model_step["arguments"][config_index + 1]), config_path.resolve())


if __name__ == "__main__":
    unittest.main()
