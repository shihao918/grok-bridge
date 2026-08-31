"""Check the latest langgraph thread's actual result."""
import httpx

lg = httpx.Client(timeout=30, trust_env=False)
r = lg.post("http://127.0.0.1:2024/threads/search", json={"limit": 3}).json()
for t in r[:3]:
    tid = t["thread_id"]
    print("thread:", tid, "| updated:", t.get("updated_at"))
    st = lg.post(f"http://127.0.0.1:2024/threads/{tid}/state", json={}).json()
    vals = st.get("values") or {}
    for m in vals.get("messages", []):
        print(f"  [{m.get('name') or m.get('type')}] {str(m.get('content'))[:100]}")
    print()
