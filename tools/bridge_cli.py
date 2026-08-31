"""CLI client: bridge run "task" [--engine langgraph|autogen|local|echo|grok-agent]"""
import json
import sys

import httpx


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print('usage: bridge "task" [--engine langgraph|autogen|local|echo|grok-agent]')
        sys.exit(1)
    task = args[0]
    engine = "local"
    if "--engine" in args:
        engine = args[args.index("--engine") + 1]

    base = "http://127.0.0.1:18083"
    headers = {"Content-Type": "application/json"}
    try:
        headers["x-bridge-token"] = open(
            r"C:\Users\Dean\Code\GitHub\grok-bridge\state\standalone_token.txt", encoding="utf-8"
        ).read().strip()
    except FileNotFoundError:
        pass

    r = httpx.post(f"{base}/run", headers=headers, json={"handler": engine, "task": task}, timeout=300, trust_env=False)
    d = r.json()
    if d.get("turns"):
        for t in d["turns"]:
            print(f"[{t['speaker']}] {t['text']}")
    elif d.get("reply"):
        print(d["reply"])
    else:
        print(json.dumps(d, ensure_ascii=False, indent=2)[:1500])


if __name__ == "__main__":
    main()
