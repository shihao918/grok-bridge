"""Enforce local-only defaults in a Grok Bot 0.36 candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATE = ROOT / ".tmp_app_candidate_036"
EXPECTED_VERSION = "0.36.0"
LOCAL_FALSE_DEFAULTS = (
    "sand_send_via_server",
    "sand_transcript_server_tail",
    "sand_roster_via_server",
)
LOCAL_TRUE_DEFAULTS = ("sand_channels",)
SERVER_ROSTER_CAPABILITY_OLD = (
    'function Gye(e,t){let r=ne(S.GrokBotService,e),n=o(async()=>Kmt({client:r,'
    'experiments:await t()}),"readCapabilities");return{isTemporalCreationEnabled:'
    'o(async()=>Fye(await n()),"isTemporalCreationEnabled"),isServerRosterEnabled:'
    'o(async()=>{let{experiments:i,capabilities:s}=await n();return '
    's?.durableIdentityEnabled??Fye({experiments:i})},"isServerRosterEnabled")}}'
    'o(Gye,"createServerAgentCapabilityReader")'
)
SERVER_ROSTER_CAPABILITY_NEW = (
    'function Gye(e,t){let r=ne(S.GrokBotService,e),n=o(async()=>Kmt({client:r,'
    'experiments:await t()}),"readCapabilities");return{isTemporalCreationEnabled:'
    'o(async()=>Fye(await n()),"isTemporalCreationEnabled"),isServerRosterEnabled:'
    'o(async()=>!1,"isServerRosterEnabled")}}'
    'o(Gye,"createServerAgentCapabilityReader")'
)
LOCAL_ACCESS_OLD_PREFIX = (
    "async function jit(e,t){let n=await ne(S.DashboardService"
)
LOCAL_ACCESS_NEW_PREFIX = (
    'async function jit(e,t){let r=process.env.SAND_HOST_GATEWAY_URL;'
    'if(typeof r=="string")try{let a=new URL(r);'
    'if(a.protocol==="http:"&&(a.hostname==="127.0.0.1"||'
    'a.hostname==="localhost"||a.hostname==="::1"||a.hostname==="[::1]"))'
    'return{state:"granted",reason:"none",privacyDisclaimerRequired:!1,'
    'purchasableTiers:[],proAndSuperGrokPlansGrantAccess:!1}}catch{}'
    "let n=await ne(S.DashboardService"
)

# Kept for compatibility with earlier local tooling.
ROUTING_DEFAULTS = LOCAL_FALSE_DEFAULTS


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def patch_main_bundle(bundle: Path, *, check: bool = False) -> str:
    source = bundle.read_text(encoding="utf-8")
    patched = False
    desired_defaults = {
        **{flag: False for flag in LOCAL_FALSE_DEFAULTS},
        **{flag: True for flag in LOCAL_TRUE_DEFAULTS},
    }
    for flag, desired in desired_defaults.items():
        desired_literal = "!0" if desired else "!1"
        wrong_literal = "!1" if desired else "!0"
        desired_pattern = f"{flag}:{{client:!0,default:{desired_literal}}}"
        wrong_pattern = f"{flag}:{{client:!0,default:{wrong_literal}}}"
        desired_count = source.count(desired_pattern)
        wrong_count = source.count(wrong_pattern)
        if wrong_count == 1 and desired_count == 0:
            source = source.replace(wrong_pattern, desired_pattern, 1)
            patched = True
            continue
        if wrong_count == 0 and desired_count == 1:
            continue
        raise RuntimeError(
            f"expected exactly one feature default for {flag} in {bundle}, "
            f"found desired={desired_count} wrong={wrong_count}"
        )

    server_roster_old_count = source.count(SERVER_ROSTER_CAPABILITY_OLD)
    server_roster_new_count = source.count(SERVER_ROSTER_CAPABILITY_NEW)
    if (server_roster_old_count, server_roster_new_count) == (1, 0):
        source = source.replace(
            SERVER_ROSTER_CAPABILITY_OLD,
            SERVER_ROSTER_CAPABILITY_NEW,
            1,
        )
        patched = True
    elif (server_roster_old_count, server_roster_new_count) != (0, 1):
        raise RuntimeError(
            f"expected exactly one server roster capability gate in {bundle}, "
            f"found old={server_roster_old_count} patched={server_roster_new_count}"
        )

    local_access_old_count = source.count(LOCAL_ACCESS_OLD_PREFIX)
    local_access_new_count = source.count(LOCAL_ACCESS_NEW_PREFIX)
    if (local_access_old_count, local_access_new_count) == (1, 0):
        source = source.replace(
            LOCAL_ACCESS_OLD_PREFIX,
            LOCAL_ACCESS_NEW_PREFIX,
            1,
        )
        patched = True
    elif (local_access_old_count, local_access_new_count) != (0, 1):
        raise RuntimeError(
            f"expected exactly one local access reader in {bundle}, "
            f"found old={local_access_old_count} patched={local_access_new_count}"
        )
    if not patched:
        return "already-patched"
    if check:
        return "needs-patch"
    bundle.write_text(source, encoding="utf-8", newline="\n")
    return "patched"


def candidate_bundles(candidate: Path) -> list[Path]:
    package_path = candidate / "package.json"
    if not package_path.is_file():
        raise RuntimeError(f"candidate package.json not found: {package_path}")
    package = json.loads(package_path.read_text(encoding="utf-8"))
    if package.get("version") != EXPECTED_VERSION:
        raise RuntimeError(
            f"expected Grok Bot {EXPECTED_VERSION} candidate, "
            f"found version={package.get('version')!r}"
        )
    bundles = [
        candidate / "dist" / "electron-main" / "main-app.cjs",
        candidate / "payload" / "dist" / "electron-main" / "main-app.cjs",
    ]
    missing = [path for path in bundles if not path.is_file()]
    if missing:
        raise RuntimeError(f"candidate main bundle not found: {missing[0]}")
    return bundles


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, default=DEFAULT_CANDIDATE)
    parser.add_argument("--check", action="store_true", help="report state without writing")
    args = parser.parse_args()
    for bundle in candidate_bundles(args.candidate.resolve()):
        before = sha256(bundle)
        result = patch_main_bundle(bundle, check=args.check)
        after = sha256(bundle)
        print(f"{result}: {bundle}")
        print(f"sha256_before={before}")
        print(f"sha256_after={after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
