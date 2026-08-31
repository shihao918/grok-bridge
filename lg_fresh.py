"""Fresh direct run + full state dump."""
import httpx

lg = httpx.Client(timeout=300, trust_env=False)
t = lg.post("http://127.0.0.1:2024/threads", json={}).json()
tid = t["thread_id"]
r = lg.post(
    f"http://127.0.0.1:2024/threads/{tid}/runs/wait",
    json={"assistant_id": "groupchat", "input": {"messages": [{"role": "user", "content": "fresh run check"}]}},
)
print("wait:", r.status_code)
d = r.json()
print("messages in response:", len(d.get("messages", [])))
st = lg.post(f"http://127.0.0.1:2024/threads/{tid}/state", json={}).json()
vals = st.get("values") or {}
print("state keys:", list(vals.keys()))
for k, v in vals.items():
    print(f"  {k}: {len(v) if isinstance(v, list) else v}")
for m in vals.get("messages", []):
    print(f"  [{m.get('name') or m.get('type')}] {str(m.get('content'))[:90]}")
