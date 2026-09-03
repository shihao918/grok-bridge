"""Patch a local Grok Bot 0.36 renderer candidate for the local-only shell.

The shipped renderer folds contiguous agent communication messages into an
``agent-comm-group``.  The 0.36 summary component renders only the group's
summary and drops its individual entries.  Even when grouping is disabled, the
lazy message card still returns the communication summary before rendering the
message body.  It also blocks the complete shell when the remote first-box
bootstrap reports a connectivity failure, even when the local coordinator and
roster are healthy.  This script applies exact, idempotent replacements to both
extracted candidate renderer trees.  It never touches the installed
application or a source checkout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATE = ROOT / ".tmp_app_candidate_036"
EXPECTED_VERSION = "0.36.0"
GROUP_OLD_PREFIX = "function mmt(t){const e=[]"
GROUP_NEW_FUNCTION = "function mmt(t){return t}"
NETWORK_GATE_OLD = (
    "function mj({firstBoxPhase:t,isShowingRestoredRoster:e,"
    "isComputerRebuildLocked:n,isPrivacyBlocked:s}){return n||e||s?!1:"
    't==="connectivity-failure"||t==="grace-elapsed"}'
)
NETWORK_GATE_NEW = (
    "function mj({firstBoxPhase:t,isShowingRestoredRoster:e,"
    "isComputerRebuildLocked:n,isPrivacyBlocked:s}){return!1}"
)
AGENT_COMM_BODY_OLD = (
    "if(b(t)){let s;return e[0]!==t?(s=o.jsx(h,{entry:t}),"
    "e[0]=t,e[1]=s):s=e[1],s}"
)
AGENT_COMM_BODY_NEW = (
    "if(b(t))return o.jsxs(o.Fragment,{children:[o.jsx(h,{entry:t}),"
    "o.jsx(T,m)]});"
)


def bundle_from_index(index_html: Path) -> Path:
    html = index_html.read_text(encoding="utf-8")
    match = re.search(r'src="\./assets/(index-[^"]+\.js)"', html)
    if match is None:
        raise RuntimeError(f"renderer entry bundle not found in {index_html}")
    return index_html.parent / "assets" / match.group(1)


def candidate_bundles(candidate: Path) -> list[Path]:
    package_path = candidate / "package.json"
    if not package_path.is_file():
        raise RuntimeError(f"candidate package.json not found: {package_path}")
    package = json.loads(package_path.read_text(encoding="utf-8"))
    version = package.get("version")
    if version != EXPECTED_VERSION:
        raise RuntimeError(
            f"expected Grok Bot {EXPECTED_VERSION} candidate, found version={version!r}"
        )
    bundles = [
        bundle_from_index(candidate / "dist" / "renderer" / "index.html"),
        bundle_from_index(candidate / "payload" / "dist" / "renderer" / "index.html"),
    ]
    if len(set(bundles)) != len(bundles):
        raise RuntimeError("candidate renderer bundle paths must be distinct")
    return bundles


def message_view_bundle(renderer_root: Path) -> Path:
    assets = renderer_root / "assets"
    matches = []
    for bundle in assets.glob("chunk-view-*.js"):
        source = bundle.read_text(encoding="utf-8")
        if AGENT_COMM_BODY_OLD in source or AGENT_COMM_BODY_NEW in source:
            matches.append(bundle)
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one agent communication message view in {assets}, "
            f"found {len(matches)}"
        )
    return matches[0]


def candidate_message_view_bundles(candidate: Path) -> list[Path]:
    bundles = [
        message_view_bundle(candidate / "dist" / "renderer"),
        message_view_bundle(candidate / "payload" / "dist" / "renderer"),
    ]
    if len(set(bundles)) != len(bundles):
        raise RuntimeError("candidate message view bundle paths must be distinct")
    return bundles


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def patch_bundle(bundle: Path, *, check: bool = False) -> str:
    source = bundle.read_text(encoding="utf-8")
    group_patched_count = source.count(GROUP_NEW_FUNCTION)
    group_old_count = source.count(GROUP_OLD_PREFIX)
    if (group_old_count, group_patched_count) == (1, 0):
        group_start = source.index(GROUP_OLD_PREFIX)
        group_end = source.index("function hmt", group_start)
        old_function = source[group_start:group_end]
        if not old_function.endswith("}"):
            raise RuntimeError("mmt function boundary changed; refusing to patch")
        source = source[:group_start] + GROUP_NEW_FUNCTION + source[group_end:]
        needs_patch = True
    elif (group_old_count, group_patched_count) == (0, 1):
        needs_patch = False
    else:
        raise RuntimeError(
            f"expected exactly one mmt implementation in {bundle}, "
            f"found old={group_old_count} patched={group_patched_count}"
        )

    network_old_count = source.count(NETWORK_GATE_OLD)
    network_patched_count = source.count(NETWORK_GATE_NEW)
    if (network_old_count, network_patched_count) == (1, 0):
        source = source.replace(NETWORK_GATE_OLD, NETWORK_GATE_NEW, 1)
        needs_patch = True
    elif (network_old_count, network_patched_count) != (0, 1):
        raise RuntimeError(
            f"expected exactly one first-box network gate in {bundle}, "
            f"found old={network_old_count} patched={network_patched_count}"
        )

    if not needs_patch:
        return "already-patched"
    if check:
        return "needs-patch"
    bundle.write_text(source, encoding="utf-8", newline="\n")
    return "patched"


def patch_message_view_bundle(bundle: Path, *, check: bool = False) -> str:
    source = bundle.read_text(encoding="utf-8")
    old_count = source.count(AGENT_COMM_BODY_OLD)
    patched_count = source.count(AGENT_COMM_BODY_NEW)
    if (old_count, patched_count) == (1, 0):
        if check:
            return "needs-patch"
        source = source.replace(AGENT_COMM_BODY_OLD, AGENT_COMM_BODY_NEW, 1)
        bundle.write_text(source, encoding="utf-8", newline="\n")
        return "patched"
    if (old_count, patched_count) == (0, 1):
        return "already-patched"
    raise RuntimeError(
        f"expected exactly one agent communication body branch in {bundle}, "
        f"found old={old_count} patched={patched_count}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, default=DEFAULT_CANDIDATE)
    parser.add_argument("--bundle", type=Path, help="explicit renderer bundle path")
    parser.add_argument("--check", action="store_true", help="report state without writing")
    args = parser.parse_args()
    candidate = args.candidate.resolve()
    bundles = [args.bundle.resolve()] if args.bundle else candidate_bundles(candidate)
    for bundle in bundles:
        before = sha256(bundle)
        result = patch_bundle(bundle, check=args.check)
        after = sha256(bundle)
        print(f"{result}: {bundle}")
        print(f"sha256_before={before}")
        print(f"sha256_after={after}")
    if args.bundle is None:
        for bundle in candidate_message_view_bundles(candidate):
            before = sha256(bundle)
            result = patch_message_view_bundle(bundle, check=args.check)
            after = sha256(bundle)
            print(f"{result}: {bundle}")
            print(f"sha256_before={before}")
            print(f"sha256_after={after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
