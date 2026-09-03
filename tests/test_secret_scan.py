import contextlib
import io
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import secret_scan


ROOT = Path(__file__).resolve().parents[1]


class SecretScanTests(unittest.TestCase):
    def make_repo(self) -> tuple[tempfile.TemporaryDirectory, Path]:
        directory = tempfile.TemporaryDirectory()
        root = Path(directory.name)
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        return directory, root

    def run_scan(self, root: Path, *args: str) -> tuple[int, str]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            return_code = secret_scan.main(list(args), root=root)
        return return_code, output.getvalue()

    def test_default_mode_keeps_scanning_tracked_worktree_only(self):
        directory, root = self.make_repo()
        self.addCleanup(directory.cleanup)
        tracked = root / "tracked.txt"
        tracked.write_text("safe\n", encoding="utf-8")
        untracked_secret = "sk-" + "untracked-secret-value"
        (root / "untracked.txt").write_text(untracked_secret + "\n", encoding="utf-8")
        subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)

        return_code, output = self.run_scan(root)

        self.assertEqual(return_code, 0, output)
        self.assertEqual(output.strip(), "secret scan clean")

    def test_staged_mode_reads_index_content_and_redacts_match(self):
        directory, root = self.make_repo()
        self.addCleanup(directory.cleanup)
        secret = "sk-" + "staged-secret-value"
        candidate = root / "candidate.txt"
        candidate.write_text(secret + "\n", encoding="utf-8")
        subprocess.run(["git", "add", "candidate.txt"], cwd=root, check=True)
        candidate.write_text("safe worktree content\n", encoding="utf-8")

        return_code, output = self.run_scan(root, "--staged")

        self.assertEqual(return_code, 1, output)
        self.assertIn("[API key] candidate.txt:1 (source=staged)", output)
        self.assertNotIn(secret, output)

    def test_write_set_scans_untracked_file_and_redacts_match(self):
        directory, root = self.make_repo()
        self.addCleanup(directory.cleanup)
        secret = "ghp_" + "123456789012345678901234567890"
        candidate = root / "candidate.txt"
        candidate.write_text(secret + "\n", encoding="utf-8")

        return_code, output = self.run_scan(
            root,
            "--write-set",
            "candidate.txt",
        )

        self.assertEqual(return_code, 1, output)
        self.assertIn("[github token] candidate.txt:1 (source=write-set)", output)
        self.assertNotIn(secret, output)

    def test_write_set_rejects_paths_outside_repository(self):
        directory, root = self.make_repo()
        self.addCleanup(directory.cleanup)
        outside = root.parent / "outside-secret-scan.txt"

        return_code, output = self.run_scan(root, "--write-set", str(outside))

        self.assertEqual(return_code, 2, output)
        self.assertIn("outside repository", output)

    def test_gitignore_covers_local_generated_artifacts(self):
        ignored = [
            ".tmp_app_candidate_036/resources/app.asar",
            ".tmp_grokbot036_candidate_user_data_v3/sand-secrets.json",
            ".tmp_backend_036_current.stdout.log",
            ".tmp_backend.diff",
            ".tmp_grokbot030_native-settings.app.asar",
            "candidate/app.asar.bak",
            "candidate/app.asar.backup",
            "candidate/app.asar.orig",
        ]
        for relative_path in ignored:
            with self.subTest(relative_path=relative_path):
                result = subprocess.run(
                    ["git", "check-ignore", "--no-index", "-q", relative_path],
                    cwd=ROOT,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, relative_path)

        result = subprocess.run(
            ["git", "check-ignore", "--no-index", "-q", "tools/example.py"],
            cwd=ROOT,
            check=False,
        )
        self.assertEqual(result.returncode, 1)


if __name__ == "__main__":
    unittest.main()
