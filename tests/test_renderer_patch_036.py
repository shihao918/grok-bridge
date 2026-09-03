import tempfile
import unittest
from pathlib import Path

from tools.patch_local_routing_036 import (
    LOCAL_ACCESS_NEW_PREFIX,
    LOCAL_ACCESS_OLD_PREFIX,
    LOCAL_FALSE_DEFAULTS,
    LOCAL_TRUE_DEFAULTS,
    SERVER_ROSTER_CAPABILITY_NEW,
    SERVER_ROSTER_CAPABILITY_OLD,
    patch_main_bundle,
)
from tools.patch_renderer_036 import (
    AGENT_COMM_BODY_NEW,
    AGENT_COMM_BODY_OLD,
    GROUP_NEW_FUNCTION,
    NETWORK_GATE_NEW,
    NETWORK_GATE_OLD,
    patch_bundle,
    patch_message_view_bundle,
)


class RendererPatch036Tests(unittest.TestCase):
    def test_patch_renders_agent_communication_body_and_is_idempotent(self):
        self.assertEqual(len(AGENT_COMM_BODY_NEW), len(AGENT_COMM_BODY_OLD))
        original = f"before:{AGENT_COMM_BODY_OLD}:after"
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory) / "chunk-view-test.js"
            bundle.write_text(original, encoding="utf-8")

            self.assertEqual(patch_message_view_bundle(bundle), "patched")
            patched = bundle.read_text(encoding="utf-8")
            self.assertIn(AGENT_COMM_BODY_NEW, patched)
            self.assertNotIn(AGENT_COMM_BODY_OLD, patched)
            self.assertEqual(patch_message_view_bundle(bundle), "already-patched")

    def test_patch_disables_grouping_and_is_idempotent(self):
        original = (
            "function mmt(t){const e=[];let n=[];const s=()=>{n=[]};"
            "for(const r of t){s(),e.push(r)}return s(),e}"
            "function hmt(t){return t}"
            + NETWORK_GATE_OLD
        )
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory) / "index-test.js"
            bundle.write_text(original, encoding="utf-8")

            self.assertEqual(patch_bundle(bundle), "patched")
            patched = bundle.read_text(encoding="utf-8")
            self.assertIn(f"{GROUP_NEW_FUNCTION}function hmt", patched)
            self.assertNotIn("function mmt(t){const e=[]", patched)
            self.assertIn(NETWORK_GATE_NEW, patched)
            self.assertNotIn(NETWORK_GATE_OLD, patched)
            self.assertEqual(patch_bundle(bundle), "already-patched")

    def test_check_reports_both_local_renderer_patches_without_writing(self):
        original = (
            "function mmt(t){const e=[];return e}"
            "function hmt(t){return t}"
            + NETWORK_GATE_OLD
        )
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory) / "index-test.js"
            bundle.write_text(original, encoding="utf-8")

            self.assertEqual(patch_bundle(bundle, check=True), "needs-patch")
            self.assertEqual(bundle.read_text(encoding="utf-8"), original)

    def test_local_routing_patch_enforces_local_defaults_and_channels(self):
        original = ",".join(
            [
                *(f"{flag}:{{client:!0,default:!0}}" for flag in LOCAL_FALSE_DEFAULTS),
                *(f"{flag}:{{client:!0,default:!1}}" for flag in LOCAL_TRUE_DEFAULTS),
                SERVER_ROSTER_CAPABILITY_OLD,
                LOCAL_ACCESS_OLD_PREFIX,
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory) / "main-app.cjs"
            bundle.write_text(original, encoding="utf-8")

            self.assertEqual(patch_main_bundle(bundle), "patched")
            patched = bundle.read_text(encoding="utf-8")
            for flag in LOCAL_FALSE_DEFAULTS:
                self.assertIn(f"{flag}:{{client:!0,default:!1}}", patched)
                self.assertNotIn(f"{flag}:{{client:!0,default:!0}}", patched)
            for flag in LOCAL_TRUE_DEFAULTS:
                self.assertIn(f"{flag}:{{client:!0,default:!0}}", patched)
                self.assertNotIn(f"{flag}:{{client:!0,default:!1}}", patched)
            self.assertIn(SERVER_ROSTER_CAPABILITY_NEW, patched)
            self.assertNotIn(SERVER_ROSTER_CAPABILITY_OLD, patched)
            self.assertIn(LOCAL_ACCESS_NEW_PREFIX, patched)
            self.assertNotIn(LOCAL_ACCESS_OLD_PREFIX, patched)
            self.assertIn('a.protocol==="http:"', LOCAL_ACCESS_NEW_PREFIX)
            self.assertIn('a.hostname==="127.0.0.1"', LOCAL_ACCESS_NEW_PREFIX)
            self.assertEqual(patch_main_bundle(bundle), "already-patched")


if __name__ == "__main__":
    unittest.main()
