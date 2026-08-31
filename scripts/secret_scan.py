"""Scan tracked files for accidental secrets / private values. Exits 1 on any hit."""

import re
import sys
from pathlib import Path

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


ROOT = Path(__file__).resolve().parent.parent


def tracked_files() -> list[Path]:
    root = ROOT
    out = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if p.suffix.lower() in SKIP_SUFFIX:
            continue
        if p.name in {"config.json", "bridge_state.json", "codex_key.bin"}:
            continue
        out.append(p)
    return out


def main() -> int:
    hits = 0
    for p in tracked_files():
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for line_no, line in enumerate(text.splitlines(), 1):
            for label, pat in PATTERNS.items():
                m = pat.search(line)
                if m:
                    print(f"[{label}] {p.relative_to(ROOT)}:{line_no}: ...{m.group(0)[:24]}...")
                    hits += 1
    if hits:
        print(f"\nFAILED: {hits} potential secret(s) found")
        return 1
    print("secret scan clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
