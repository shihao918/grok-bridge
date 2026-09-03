import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


class ReleasePolicyTests(unittest.TestCase):
    def test_hosted_ci_is_manual_only(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertRegex(source, r"(?m)^\s{2}workflow_dispatch:\s*$")
        self.assertIsNone(re.search(r"(?m)^\s{2}(push|pull_request):\s*$", source))

    def test_manual_workflow_runs_the_full_local_test_pattern(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('python -m unittest discover -s tests -p "test_*.py" -v', source)
        self.assertIn("python -m py_compile model_runtime.py backend_server.py", source)


if __name__ == "__main__":
    unittest.main()
