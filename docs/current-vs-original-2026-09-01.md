# Grok Bridge 当前版本与原有版本差异

首次记录：2026-09-01；本次更新：2026-09-03（Asia/Shanghai）

## 结论

当前工作树的目标已经从 Grok Bot 0.30 的内部频道合同修复，推进到 Grok
Bot **0.36.0 本地运行方案**：0.36 coordinator 通过进程环境变量连接
`backend_server.py`，后端默认复用当前 Codex 的 Responses provider/model
binding，并将 assistant rows 写回 transcript 供 renderer 展示。

这是一组工作树候选和本地验证合同，不是“当前正在运行”的声明，也不是
Discord/Slack provider、Hosted CI、部署、账单或未来可用性的证明。

## 身份快照

- 仓库：`C:\Users\Dean\Code\GitHub\grok-bridge`
- 分支：`codex/grok-bot-0.30-channel-create-20260901`
- grok-bridge release identity：`VERSION=0.36.0.0`
- 0.36 candidate：`.tmp_app_candidate_036`
- candidate package version：`0.36.0`
- `resources/app.asar` SHA-256：
  `98DC49AC5C69471CF77F57852E874CC4D1C0DB902BAB20C7F8FA434CCA3613F3`

分支名称仍包含 `0.30`，但名称不是版本证明；当前 candidate 的 package
identity 才是 0.36 静态身份。源码提交、GitHub PR/merge 和本地运行证据仍按
各自的实时 readback 判断，不能由这个文档替代。

## 差异矩阵

| 层级 | 原有实现 | 当前工作树 | 分类 |
| --- | --- | --- | --- |
| 桌面版本 | 0.30 频道修复阶段 | 0.36.0 candidate | `current` candidate，不是 runtime truth |
| 频道创建 | 基线缺少 `/api/createGroup`，空对象响应导致创建失败 | durable Bot/group、成员、资料、删除、channels-view 和 transcript 合同 | `current` code |
| 消息路由 | 0.30 阶段主要修复本地 `sendPrompt`/transcript 合同 | 0.36 bundled local flags + loopback coordinator + Codex Responses binding | `current` code/candidate |
| 启动 | 直接启动 EXE 不保证 coordinator 物化 | launcher 注入 loopback gateway、校验模型 binding，并验证 coordinator | `current` launcher |
| renderer | 旧 bundle 会折叠或错误呈现部分群组回复 | 0.36 renderer patch 保留逐成员 transcript rows | `current` candidate patch |
| 验收 | 分散命令和旧 `acceptance_test.py` | `verify_local_036.ps1` 统一本地代码/合同/binding 检查，不发送 provider 请求 | `current` local verification |
| provider | 兼容接口或历史云端 transport | 仍未实现 Discord/Slack 真实连接与执行 | `out of scope` |
| CI/部署 | workflow 曾监听所有 push/PR | candidate 仅保留 `workflow_dispatch`；本轮远端读回为 `disabled_manually`；走本地验收 | `current` candidate + time-varying remote snapshot |
| 运行状态 | 旧 PID、端口和 GUI 观察容易过期 | 必须同一轮 fresh probe，文档不固化“正在运行” | `unclear` until reproved |

## 原有基线与 0.30 阶段

基线 `master` / `87669b73df6d91806bab55e24c9ba03004a63819` 中：

- `backend_server.py` 主要服务沙箱端点和流式迁移实验。
- 没有 `/api/createGroup` 路由；未匹配请求落到空对象，客户端得不到
  `response.agent.id`。
- 没有当前 durable agent/transcript/acceptance 状态、群组元数据、事件广播
  和 renderer identity 兼容合同。

0.30 阶段随后补齐了内部 Bot/群组频道创建、成员替换、资料更新、批量删除、
transcript hydration、群组 fan-out、逐成员 `fromAgent` 和 acceptance 语义。这些
仍是 0.36 本地方案复用的后端基础，但它们本身不证明 0.36 coordinator 已连接。

## 当前 0.36 实现

### 1. 本地路由默认值

`tools/patch_local_routing_036.py` 校验 candidate 版本并约束两个 main bundle：

- `sand_send_via_server=false`
- `sand_roster_via_server=false`
- `sand_transcript_server_tail=false`
- `sand_channels=true`

`--check` 只读检查 candidate；统一验证器还会把 `needs-patch` 视为失败，避免
“脚本退出码为 0”被误读为 bundle 已符合本地默认值。

### 2. coordinator 启动合同

`tools/start_grok_bot_036_local.ps1`：

- 只接受 HTTP loopback gateway，默认 `http://127.0.0.1:9000`。
- 在 Grok Bot 进程创建时注入 `SAND_HOST_GATEWAY_URL`。
- 默认解析 `%USERPROFILE%\.codex\config.toml` 的当前 model/provider/base URL/
  wire API/reasoning/auth-env-name，不输出密钥值。
- 非 loopback HTTP provider 默认在构造 Authorization header 或发送 provider
  请求前失败；安全摘要只报告鉴权是否存在。HTTPS、HTTP loopback 或显式风险
  开关才允许执行。
- `-CodexConfigPath` 可绑定一份 ignored、无密钥的 tunnel 配置，保持原 provider
  identity/model/reasoning/auth-env-name，只把 base URL 指向 SSH loopback 入口。
- 正常模式下检查 `GET /model-runtime`；binding 漂移时，只重启能够证明属于
  本仓库 `backend_server.py` 的 loopback listener。
- 按需启动 backend，并等待 0.36 coordinator 连接。
- `-DryRun -SkipBackendHealthCheck -NoStartBackend` 只输出计划，不启动后端、
  GUI 或 provider。

直接双击 EXE 或在缺少环境变量的进程中重载，不能证明 coordinator 已物化。

### 3. renderer 与本地回复链

`tools/patch_renderer_036.py` 对两个 0.36 renderer bundle 应用两项本地补丁：

- 保留群组 transcript 中每个成员的独立回复；
- 在本地 candidate 中关闭依赖云端 first-box 状态的整页网络阻断，避免
  coordinator、roster 和 backend 已连通时仍被 `*.cursorvm.com` 错误页拦截。

后端的目标回复链是：

```text
/api/sendPrompt
  -> selected Responses provider/model
  -> assistant transcript commit
  -> renderer 展示
```

本轮本机只读快照解析为 `providerKey=cch`、`model=gpt-5.6-sol`、
`wireApi=responses`、`reasoningEffort=xhigh`。这组值是时点绑定的本机证据，
不是 README 稳定默认，也不证明最终 executed upstream channel 或账单归因。

路由存在、HTTP 400 参数校验、SSE 首帧、override 文件写入或 UI 显示“已发送”
都只能证明局部闸门，不能替代完整链路证据。

### 4. 统一本地发布验证

`tools/verify_local_036.ps1` 按依赖顺序执行：

1. 0.36 routing `--check`，并拒绝 `needs-patch`。
2. 0.36 renderer `--check`，校验两个 renderer bundle。
3. `model_runtime.py --require-auth`，只读确认指定 Codex 配置的 Responses binding
   与鉴权存在。
4. launcher dry-run，确认 loopback gateway 与同一 `-CodexConfigPath` model binding。
5. 相关 Python 文件 `py_compile`。
6. 当前 `tests/test_*.py` unittest discovery。
7. tracked、staged 和显式 write-set secret scan。
8. `git diff --check`。

命令：

```powershell
pwsh -NoLogo -NoProfile -File tools\verify_local_036.ps1 `
  -CodexConfigPath state\codex-tunnel-config.toml
```

只查看计划：

```powershell
pwsh -NoLogo -NoProfile -File tools\verify_local_036.ps1 -DryRun `
  -CodexConfigPath state\codex-tunnel-config.toml
```

该入口不调用 `tools/acceptance_test.py`；后者包含
`https://api2.cursor.sh`，不符合当前验收边界。GitHub Actions 不作为自动
门禁，本地测试结果也不会被表述为 Hosted CI 结果。

## 状态分类

### current

- 0.36 candidate 静态身份与本地 routing patch。
- loopback-only launcher 及 coordinator 环境注入合同。
- 当前 Codex `cch` provider identity 下的 Responses model binding 解析与
  fail-closed 执行接口。
- 本地 Bot/group、channels-view、sendPrompt、transcript 和 renderer patch
  的代码/测试面。
- 统一本地验证脚本与合同测试。
- 非 loopback HTTP fail-closed、显式风险开关和严格 backend owner 校验。

### planned

- 每次发布候选前重新生成 candidate hash、write-set 和本地验证结果。
- 需要最终 GUI 验收时，在用户可见窗口中核对 roster、频道、逐成员回复和
  重载读回。

### blocked / intentionally skipped

- GitHub Actions/Hosted CI：用户明确不采用；candidate 触发器为 manual-only；
  远端 workflow 本轮读回为 `disabled_manually`，该状态仍需发布前再次确认。
- Discord/Slack 外部 provider：不在当前本地方案范围内。

### unclear until fresh probe

- Grok Bot 0.36、backend `:9000` 和 coordinator 当前是否运行/连接。
- 一次新的 Codex Responses prompt、最终 executed upstream channel 与输出。
- GUI 当前像素、renderer 当前逐成员回复与持续订阅/重连状态。

任何旧 PID、旧端口监听、旧日志、旧 transcript 数量或旧 GUI 截图都只属于
历史证据，不能写成持续 runtime truth。

## 验收边界

本地统一验证通过可证明：

- candidate 的 0.36 routing bundle 满足已编码的本地默认值；
- launcher dry-run 绑定 loopback gateway 和 Codex Responses binding，且本次
  验证不启动 GUI/provider、也不发送 provider 请求；
- 当前 Python/测试/扫描/diff 合同通过。

它不证明（`not_proof_of`）：

- Grok Bot 或 backend 当前进程存活；
- coordinator 当前已连接；
- Codex Responses provider 已执行新请求；
- assistant row 已提交并在 GUI 渲染；
- Discord/Slack provider execution；
- 最终 executed upstream channel、billing 或持续 runtime truth。
