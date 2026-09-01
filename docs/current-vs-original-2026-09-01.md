# Grok Bridge 当前版本与原有版本差异

日期：2026-09-01

基线：`master` / `87669b73df6d91806bab55e24c9ba03004a63819`

候选分支：`codex/grok-bot-0.30-channel-create-20260901`

## 结论

当前工作区已经从“只提供少量沙箱/流式试验接口”的后端，扩展为可被 Grok Bot 0.30 本地桌面端读取和写入的本地桥接后端。

本轮最直接的用户可见修复是内部 Bot 群组频道创建：客户端调用 `POST /api/createGroup` 时，现在能够得到带有持久化 `agent.id` 的响应，频道会写入本地状态并出现在 GUI 左侧列表。

这不等于 Discord/Slack 外部渠道已经接通。外部 provider 的连接、刷新、断开和真实 provider 执行仍是独立工作项。

## 原有实现

基线提交 `87669b7` 中：

- `backend_server.py` 约 212 行，主要服务于沙箱端点和流式迁移实验。
- 未提供 `/api/createGroup` 路由。
- 未匹配的 API 请求会落到空对象响应，客户端拿不到 `response.agent.id`，因此显示“无法创建频道”。
- 没有当前这套 durable agent/transcript 状态恢复、群组元数据和事件广播实现。
- `daemon.py` 未单独屏蔽浏览器取消 keepalive 请求时的常见 socket 断连异常。
- `scripts/secret_scan.py` 通过递归目录扫描文件，不能严格限定 Git 已跟踪文件，Git 枚举失败也没有独立错误返回。
- 基线没有本轮新增的 `tests/test_connect_stream.py` 回归测试文件。

## 当前实现

### 1. 内部频道创建

`backend_server.py` 新增：

- `GatewayContractError`，统一报告请求合同错误。
- `_normalize_group_member_ids`，校验成员 ID 是非空列表并去除重复项。
- `_create_gateway_group`，校验频道名称、成员存在性和不允许嵌套频道。
- `POST /api/createGroup` 路由。
- durable group agent 元数据：`isGroup=true`、`memberIds`、`origin=user`、`harness=box`。
- 原子状态文件写入、`agent-upserted` 广播和基于名称/成员组合的幂等 fingerprint。
- `POST /api/setGroupMembers` 用原子持久化替换频道成员列表，并拒绝未知成员、嵌套频道和非群组目标。
- `POST /api/updateAgent` 更新 Bot/频道的名称、描述、标题和头像样式字段。
- `POST /api/deleteAgents` 批量删除用户 Bot/频道；保护合成 `bridge-agent-local`，并阻止删除仍被其他频道引用的成员。
- 群组 `sendPrompt` 会按持久化 roster 对每个成员执行一次本地模型调用，把每个结果写回群组 transcript；单成员失败会记录错误并继续其他成员。

当前实际验证过的频道：

- 名称：`123`
- ID：`f2e6e90a-e7d1-47a2-9d5c-b4c55926d149`
- 成员：`bridge-agent-local`、`6386ddf1-9307-45de-a8d4-6af10c9c3f0c`
- 重放相同请求返回同一 ID，群组数量保持 `1 -> 1`。

### 2. 后端基础能力扩展

当前 `backend_server.py` 还包含此前工作区中已经形成的候选改动：

- durable agent/transcript/acceptance 状态加载与原子持久化。
- SSE transcript 订阅和 agent 更新广播。
- 请求日志敏感字段、Bearer/JWT 和头像数据脱敏。
- 本地执行 credential/provider 描述和执行器生命周期管理。
- `/api/listAgents`、消息发送、流式事件和多个兼容响应合同。

这些扩展属于同一后端候选，但不应被压缩成“频道创建已经等于完整 provider 运行时”。

### 3. daemon 和扫描器

- `daemon.py`：捕获 `BrokenPipeError`、`ConnectionResetError`、`ConnectionAbortedError`，减少浏览器取消请求时的无意义 traceback。
- `scripts/secret_scan.py`：改用 `git ls-files -z` 获取跟踪文件，并在枚举失败时返回错误码 `2`。

### 4. 测试与工具

- 新增 `tests/test_connect_stream.py`，当前本地 `unittest` 共 33 个测试通过。
- 覆盖频道创建、成员替换、Bot/频道资料更新、批量删除、非法成员无副作用、幂等重放和群组 fan-out/部分失败语义。
- 新增 `tools/download_dmg.py`、`tools/fetch_dmg_lfs.py`、`tools/resumable_dmg.py`，属于工作区已有工具候选。

## 证据

- `python -m unittest tests.test_connect_stream`：33 tests，全部通过。
- `python -m py_compile backend_server.py daemon.py bridge_common.py local_proxy.py`：通过。
- `python scripts/secret_scan.py`：`secret scan clean`。
- `git diff --check`：通过。
- 本地 `127.0.0.1:9000/health`：HTTP 200，`{"ok": true}`。
- 本地 `POST /api/createGroup` 重放：返回原频道 ID，群组数量不增加；`setGroupMembers`、`updateAgent` 和空删除请求均已用无副作用请求验证。
- 本地 Ollama `/api/tags` 与受控 `/api/chat` canary 均 HTTP 200；canary 返回 `CANARY_OK`。历史 state 中的旧 HTTP 500 仅作为历史错误记录，不能覆盖本次 fresh 成功。
- `state/backend_transcript_state.json`：包含上述 group agent 元数据。
- Grok Bot 0.30 GUI 进程和本地 gateway 当前均在运行；已验证频道 roster/transcript 可读回和打开路径，但未做全新表单点击截图。

## 未纳入同步的内容

`.tmp_app_extract/` 共 413 个文件、约 34.5 MB，包含 Electron/renderer 构建产物和原生二进制，代码未引用该目录。它被视为本地提取缓存，不纳入 GitHub；本次同步只加入 `.gitignore` 规则，不删除本地文件。

运行时 `state/`、`logs/`、`__pycache__/` 和 `.venv/` 也继续按现有 `.gitignore` 规则排除。

## 仍然未证明或未实现

- 未实现 Discord/Slack 外部 provider 的连接、刷新、断开和真实 provider 路径。
- 全新 GUI 表单创建尚未做视觉点击验证；当前证据是 gateway 合同和已存在频道的读回/打开路径。
- GitHub Actions 是否能运行需要远端新提交后的实际 CI 结果，不能用本地测试替代。
