"""Verify the quota-test thread via history endpoint (reliable)."""
import httpx

lg = httpx.Client(timeout=30, trust_env=False)
tid = "f5654b53-628f-44f4-9826-8f1e497f99c9"
hist = lg.get(f"http://127.0.0.1:2024/threads/{tid}/history?limit=10").json()
seen = {}
for h in hist:
    for task in h.get("tasks") or []:
        if task.get("error"):
            print("ERROR in", task.get("name"), ":", str(task.get("error"))[:200])
    vals = h.get("values") or {}
    if vals.get("messages"):
        for m in vals["messages"]:
            seen[(m.get("name") or m.get("type"), str(m.get("content"))[:60])] = m

print("unique messages across history:", len(seen))
for (name, head), m in seen.items():
    print(f"[{name}] {head}")
