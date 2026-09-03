"""Scan repository file sets for accidental secrets without echoing secret values."""

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

PATTERNS = {
    "private IP": re.compile(r"\b(?:192\.168|10\.\d+|172\.(?:1[6-9]|2\d|3[01]))\.\d+\.\d+\b"),
    "API key": re.compile(r"\bsk-[A-Za-z0-9_\-]{10,}\b"),
    "JWT": re.compile(r"\beyJ[A-Za-z0-9_\-]{20,}\.[A-Za-z0-9_\-]{10,}"),
    "github token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@(?:gmail|outlook|qq|163)\.[A-Za-z]{2,}\b"),
    "user id": re.compile(r"user_0[1-9A-Z]{10,}"),
}

SKIP_DIRS = {".git", ".venv", "state", "logs", "__pycache__", "node_modules"}
SKIP_SUFFIX = {".png", ".jpg", ".bin", ".log"}
SKIP_NAMES = {"config.json", "bridge_state.json", "codex_key.bin"}


ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ScanTarget:
    relative_path: str
    source: str
    content: bytes


def _run_git(root: Path, *args: str) -> bytes:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        operation = " ".join(args[:2])
        raise RuntimeError(f"git operation failed ({operation}): {exc}") from exc
    return result.stdout


def _decode_git_paths(payload: bytes) -> list[str]:
    return [
        raw_path.decode("utf-8", errors="surrogateescape")
        for raw_path in payload.split(b"\0")
        if raw_path
    ]


def _skip_default_path(relative_path: str) -> bool:
    rel = Path(relative_path)
    return (
        any(part in SKIP_DIRS for part in rel.parts)
        or rel.suffix.lower() in SKIP_SUFFIX
        or rel.name in SKIP_NAMES
    )


def _is_probably_binary(content: bytes) -> bool:
    return b"\0" in content[:8192]


def tracked_targets(root: Path) -> list[ScanTarget]:
    targets = []
    for relative_path in _decode_git_paths(_run_git(root, "ls-files", "-z")):
        if _skip_default_path(relative_path):
            continue
        path = root / Path(relative_path)
        if not path.is_file():
            continue
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(f"unable to read tracked file: {relative_path}: {exc}") from exc
        if not _is_probably_binary(content):
            targets.append(ScanTarget(relative_path, "tracked", content))
    return targets


def staged_targets(root: Path) -> list[ScanTarget]:
    payload = _run_git(
        root,
        "diff",
        "--cached",
        "--name-only",
        "--diff-filter=ACMR",
        "-z",
    )
    targets = []
    for relative_path in _decode_git_paths(payload):
        content = _run_git(root, "show", f":{relative_path}")
        if not _is_probably_binary(content):
            targets.append(ScanTarget(relative_path, "staged", content))
    return targets


def _resolve_write_set_path(root: Path, raw_path: str) -> tuple[Path, str]:
    root = root.resolve()
    requested = Path(raw_path)
    path = (root / requested).resolve() if not requested.is_absolute() else requested.resolve()
    try:
        relative_path = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise RuntimeError(f"write-set path is outside repository: {raw_path}") from exc
    if not path.exists():
        raise RuntimeError(f"write-set path does not exist: {relative_path}")
    return path, relative_path


def write_set_targets(root: Path, raw_paths: Sequence[str]) -> list[ScanTarget]:
    selected: dict[str, Path] = {}
    for raw_path in raw_paths:
        path, relative_path = _resolve_write_set_path(root, raw_path)
        if path.is_dir():
            for child in sorted(path.rglob("*")):
                if not child.is_file():
                    continue
                child_relative = child.relative_to(root.resolve()).as_posix()
                if not _skip_default_path(child_relative):
                    selected[child_relative] = child
        elif path.is_file():
            selected[relative_path] = path
        else:
            raise RuntimeError(f"write-set path is not a regular file or directory: {relative_path}")

    targets = []
    for relative_path, path in sorted(selected.items()):
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(f"unable to read write-set file: {relative_path}: {exc}") from exc
        if not _is_probably_binary(content):
            targets.append(ScanTarget(relative_path, "write-set", content))
    return targets


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument(
        "--staged",
        action="store_true",
        help="scan the exact content currently stored in the Git index",
    )
    inputs.add_argument(
        "--write-set",
        action="append",
        nargs="+",
        metavar="PATH",
        help="scan explicit repository-relative files or directories; may be repeated",
    )
    return parser.parse_args(argv)


def collect_targets(args: argparse.Namespace, root: Path) -> list[ScanTarget]:
    if args.staged:
        return staged_targets(root)
    if args.write_set:
        raw_paths = [path for group in args.write_set for path in group]
        return write_set_targets(root, raw_paths)
    return tracked_targets(root)


def scan_targets(targets: Sequence[ScanTarget], root: Path) -> int:
    del root  # The path boundary is enforced while targets are collected.
    hits = 0
    hit_files = set()
    for target in targets:
        text = target.content.decode("utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), 1):
            for label, pattern in PATTERNS.items():
                if pattern.search(line):
                    print(
                        f"[{label}] {target.relative_path}:{line_no} "
                        f"(source={target.source})"
                    )
                    hits += 1
                    hit_files.add((target.source, target.relative_path))
    if hits:
        print(
            f"\nFAILED: {hits} potential secret(s) found "
            f"across {len(hit_files)} file(s)"
        )
        return 1
    print("secret scan clean")
    return 0


def main(argv: Sequence[str] | None = None, *, root: Path | None = None) -> int:
    args = parse_args(argv)
    scan_root = (root or ROOT).resolve()
    try:
        targets = collect_targets(args, scan_root)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    return scan_targets(targets, scan_root)


if __name__ == "__main__":
    sys.exit(main())
