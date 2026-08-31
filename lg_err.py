"""Dump run history for the latest thread (find the real LLM error)."""
import httpx

lg = httpx.Client(timeout=30, trust_env=False)
tid = "f5654b53-628f-44f4-9826-8f1e497f99c9"
hist = lg.get(f"http://127.0.0.1:2024/threads/{tid}/history?limit=5").json()
for h in hist:
    for task in h.get("tasks") or []:
        if task.get("error"):
            print("task:", task.get("name"), "| error:", str(task.get("error"))[:300])
