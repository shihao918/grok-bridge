# UserComputer channel — protocol notes

本文只描述远程 ConnectRPC/UserComputer 通道。仓库 README 和
`docs/current-vs-original-2026-09-01.md` 中的 `/api/*` 是本地 `backend_server.py`
gateway 合同，不能与下述远程服务混读。

Observed on **Grok Bot desktop 0.30.0** (Windows). Everything below was derived from
static analysis of the shipped bundles and live traffic against `https://api2.cursor.sh`
using a personally logged-in account. Unofficial, may change at any time.

## Service

All calls are ConnectRPC (Connect protocol, JSON) against:

```
POST https://api2.cursor.sh/aiserver.v1.GrokBotService/<Method>
headers:
  authorization: Bearer <access token>
  content-type: application/json            (unary)
  content-type: application/connect+json    (server-streaming)
  connect-protocol-version: 1
  x-ghost-mode: true
```

Unary responses are plain JSON. Streaming responses use **Connect envelope framing**:
`1 byte flags + 4 bytes big-endian length + payload`, repeated.

## Machine lifecycle

```
IssueGrokBotUserComputerCredential {machine_id}
  → {credential, expires_at_ms}              # device credential (short-lived)

WatchGrokBotUserComputerRequests             # server-streaming, keeps machine "online"
  {machine_id, credential, hello: {label, local_root, capabilities{messages_op}}}
  → events: connected | notify | heartbeat

PollGrokBotUserComputerRequests
  {machine_id, credential, ack_ids[], limit}
  → {requests: [{id, frame, enqueued_at_ms}]}

SubmitGrokBotUserComputerResponses
  {machine_id, credential, frames[]: ResponseFrame}
  → {accepted_count}
```

Key insight: **the machine is only "online" while a Watch stream is open.**
Polling alone is not enough — `Open…` calls from agents fail with
"Your local machine isn't connected" unless a Watch connection exists.

## Frames

`RequestFrame` (cloud → device), one of:

| field | payload | notes |
| --- | --- | --- |
| `exec` | `{server_message_json, approval_id, authorized_by_standing, authorized_by_approval}` | "run something on the machine" |
| `upload` / `download` | file transfer with approval id | |
| `cancel` | `{op_json, approval_id}` | |
| `messagesOp` | `{request_id, client{message_json, cwd_state}}` | **reserved by the vendor**; stripped server-side from `Open…` calls made outside the app |

`ResponseFrame` (device → cloud), one of:

| field | payload |
| --- | --- |
| `client` | `{message_json, cwd_state}` |
| `control` | chunked `{data, seq, last}` |
| `file` / `file_error` | file transfer result |
| `messages_result` / `messages_error` | messagesOp result |

`Hello.capabilities = {messages_op: bool}` gates whether `messagesOp` frames are
delivered at all — but the server still strips them from third-party `Open…` calls,
so a bridge effectively handles `exec` frames only.

## Related endpoints (same service)

- `ListGrokBotUserComputers` — enrolled machines (yours shows up with a `Hello`)
- `OpenGrokBotUserComputerRequest {machine_id, frame, idempotency_key}` — cloud-side
  enqueue of a request frame
- `CancelGrokBotUserComputerRequest {machine_id, request_id}`
- `GetGrokBotRuntimeCapabilities`, `ListSandBoxes`, `GetSandBoxRunState` — box state

## Undeployed (404 as of 0.30.0)

The `PrivateWorker*` family (`RegisterPrivateWorkerPool`,
`StreamPendingPrivateWorkerRequests`, `ClaimPendingPrivateWorkerRequest`, …) exists in
the bundled protos but is **not routed on the public backend yet** — presumably gated
behind the `parallel_agent_workflow` experiment flag.

## Token storage

The desktop app persists account tokens in `sand-secrets.json`
(`cursor-accounts` → per-account `cursor-access-token` / `cursor-refresh-token`),
each wrapped in Chromium's `os_crypt` **v10** envelope: `v10` prefix + 12-byte nonce +
AES-256-GCM ciphertext+tag, with the AES key DPAPI-protected in the app's `Local State`
(`os_crypt.encrypted_key`). A same-user process can therefore derive the access token
without touching the app — which is what `bridge_common.get_grok_access_token()` does.

## Delivery semantics

- `SendGrokBotUserMessage` delivery enum: `ACCEPTED_BOX` (queued on the cloud box) or
  `ACCEPTED_TEMPORAL` (workflow engine), plus `DUPLICATE` / `REFUSED`.
- Worker hierarchy: `SetWorkerManager{worker_bc_id, manager_bc_id, spawn_kind}` with
  `ManagerSpawnKind = CREATED | ADOPTED | CREATED_SAME_VM`.
- Subagent lineage: `CloudSubagentParentSpawnKind = TASK | EVENT_SUBSCRIPTION | ADOPTED`.
