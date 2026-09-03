# Writing a handler

> Legacy/standalone scope: this document describes the `daemon.py` handler surface,
> not the Grok Bot 0.36 coordinator, channel lifecycle, or Codex Responses path.

A handler turns an `exec` frame's task into a result dict. Everything lives in the
`HANDLERS` dict at the bottom of `daemon.py`.

## Contract

Input: the `task` string from the request payload:

```json
{"handler": "<name>", "task": "what to do"}
```

Output: a JSON-serializable dict. The daemon wraps it as
`{"bridge": <label>, **result}` and delivers it back to the Grok cloud. Keep results
small (they travel inside one Connect frame); put long output behind a link or a file.

```python
def handle_my_engine(task: str) -> dict:
    return {
        "ok": True,
        "engine": "my-engine",
        "summary": task[:200],
        # any extra structured fields you want
    }

HANDLERS["my-engine"] = handle_my_engine
```

## Reference: langgraph handler

Talks to a LangGraph dev server (`langgraph dev`, graph registered as `groupchat`):

```python
def handle_langgraph(task: str) -> dict:
    lg = httpx.Client(timeout=240, trust_env=False)
    t = lg.post(f"{LG_DEV}/threads", json={}).json()
    r = lg.post(
        f"{LG_DEV}/threads/{t['thread_id']}/runs/wait",
        json={"assistant_id": "groupchat",
              "input": {"messages": [{"role": "user", "content": task}]}},
    )
    msgs = r.json().get("messages", [])
    return {
        "ok": bool(msgs),
        "engine": "langgraph",
        "thread_id": t["thread_id"],
        "turns": [{"speaker": m.get("name") or m.get("type"),
                   "text": str(m.get("content"))[:500]} for m in msgs],
    }
```

## Reference: autogen handler

AutoGen Studio runs over REST (session/run creation) + a WebSocket run channel:

1. `POST /api/sessions/ {user_id, team_id}` (once, cache the id)
2. `POST /api/runs/ {session_id, user_id}` → `run_id`
3. `GET /api/teams/<id>?user_id=…` → team component JSON
4. `WS /api/ws/runs/<run_id>` → send `{"type": "start", "task": …, "team_config": …}`
5. Collect events until `{"type": "completion"}`; `data.task_result.messages` carries
   the transcript.

See `handle_autogen` in `daemon.py` for the full implementation.

## Guidelines

- **Bound the runtime.** The Open-call wait window is ~1 minute; longer tasks still
  complete and get submitted, but the caller sees an "unavailable" notice instead of
  the streamed result.
- **Validate the payload.** The frame comes from whatever the cloud agent put in
  `server_message_json` — treat it as untrusted input.
- **Keep results compact.** Truncate transcripts (the built-in handlers cap turns at
  500 chars each).
- **Fail JSON.** On any exception return `{"ok": False, "error": …}` — the daemon
  already does this, but handlers can add richer diagnostics.
