# dsh-harness-mcp-server

> 把 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) 的 agent 能力暴露成一个 **MCP server**，让任意 MCP 客户端（如 [Hermes](https://hermes-agent.nousresearch.com/)）都能驱动 Harness 执行真实的编码任务。

**Hermes 是大脑（pro），Harness 是胳膊（flash）—— 1+1>2。**

[![npm version](https://img.shields.io/npm/v/@chushixixin/dsh-harness-mcp-server)](https://www.npmjs.com/package/@chushixixin/dsh-harness-mcp-server)
[![license](https://img.shields.io/npm/l/@chushixixin/dsh-harness-mcp-server)](./LICENSE)

> 📖 [English](./README.md) · 中文（当前页）

## 为什么需要它

Harness 自带强大的 agent 运行时（工具、LLM、agent、会话），但它本身是一个 **Cordis 应用**，别的 agent 无法直接调用。这个插件把 Harness「由内向外」翻转：在 Harness **内部**启动一个真正的 **MCP server**（StreamableHTTP），通过 `ctx` 桥接 Harness 的核心服务 —— `ctx.agents`、`ctx.agentPresets`、`ctx.tools` —— 让外部的「大脑」能把真实工作交给 Harness 的「胳膊」。

自 **0.11.0** 起，派活的模型是 **session + turn**，不再是任务队列：`session_send` 把一条任务作为一个 turn 投喂进会话后**立即返回**——没有 taskId、没有服务端排队、没有 TTL、也没有同步等待。之后由客户端主动驱动回路：`session_status` 查权威 phase 与结构化结果，`session_tail` 拉过程明细，想让出一段等待时才用 `session_wait`。**session log 是唯一事实源**，所以进程重启也不丢状态。

```
Hermes (MCP 客户端, 大脑)
   │  session_send → { sessionId, status: "accepted" }   (HTTP, 立即返回)
   ▼
dsh-harness-mcp-server (MCP server, :8090)
   │  ctx.agents.create → 挂载 'standard' preset → agent.followup() 一个 turn
   ▼
Harness agent (flash) — 完整工具集: bash、fs、todo、web…
   │  事件流… turn/start … turn/end (session log)
   ▲
Hermes 主动轮询: session_status / session_tail (可选 session_wait)
```

## 工具

> **0.11.0 破坏性变更**：旧「任务」层已整体移除、无兼容垫片——`agent_run`、`task_inbox`、`task_result`、`task_list`、`task_wait`、`task_cancel` 均不再存在。没有 `taskId`、没有内存任务队列、没有 TTL、没有队列容量；派活统一走 `session_send` 与下列 `session_*` 工具。

| 工具 | 方向 | 用途 |
|------|-----------|---------|
| `echo` | — | 验证 MCP 连通性 |
| `harness_list_tools` | — | 列出 Harness 已注册的工具名 |
| `harness_status` | Hermes ← Harness | 运维总览：`agentPool`（size / cap / live agents）、`mcpInFlightTurns`（按会话统计 MCP 在飞 turn）与运行时 `config` |
| `model_list` | Hermes ← Harness | 列出所有已注册 provider 的模型（含已声明未激活的 provider）；`withWindow` 附加上下文窗口 |
| `preset_list` | Hermes ← Harness | 列出 **agent preset（会话预设）目录**：主词是 preset 本体（standard/code/cordis/minimal 等，来自 dsh agent-presets），并列出预设定向的其余维度——沙箱访问模式（read-only/workspace-write/danger-full-access）、审批策略（ask/never）与权限预设（捆绑，如 workspace-write = workspace-write + ask）；`modes` 给出可传给 `session_send` 的 `mode=` 规范 id |
| `workspace_list` | Hermes ← Harness | 列出工作区及其会话分组 |
| `session_send` | Hermes → Harness | **唯一的派活入口**：把一条任务作为一个 **turn** 投喂进会话并**立即返回**。参数：`sessionId` *或* `cwd`（二选一必填——`sessionId` 续接会话，`cwd` 复用/创建该目录的常驻池会话）、`message`、`context?`、`newSession?`、`title?`、`model?`、`provider?`、`preset?`、`mode?`、`sandbox?`、`approval?`。消息模板 = 记忆 `context` + 【任务】 + 「必须用一行 summary JSON 收尾」。返回 `{sessionId, cwd?, status: "accepted", inboxDepth, openTurn, hint}`——不等待、不超时、绝不阻塞 MCP 客户端 |
| `session_status` | Hermes ← Harness | **权威查询**（唯一事实源 = session log）：`live`、`source`、`cwd?`、`title?`、`agentStatus?`、`phase`（`idle`/`running`/`waiting_input`/`interrupted`）、`openTurn{turn,startedSeq}`、`lastTurn{turn,reason{kind,error?}}`、`prompts[]`、`context`（events/tokens/pressure/window/ratio；仅 live 可测，非 live 为 `null`）、`logEvents`、`changes`/`verification`/`leftovers`（从最后一个 turn 边界内的 assistant 文本提取，提不到为 `null`）、`lastText`、`note?`。末尾 `turn/start` 无对应 `turn/end` 即报 `phase: "interrupted"`——进程已不在，**不是** `running` |
| `session_tail` | Hermes ← Harness | **过程明细**：只返回**表面事件**——`user`/`assistant` 消息文本、`tool/call` 与 `tool/result` 摘要、`turn/start`/`turn/end` 边界。内部流式事件全部滤除、空文本 assistant 消息直接跳过（不再有 "(no text blocks)" 噪声）。参数：`n`（默认 5）、`sinceSeq?`、`filter?: 'all'\|'surface'\|'tools'`；每行 `{seq, type, text(≤2k), turn?}` |
| `session_wait` | Hermes ← Harness | **可选阻塞，单段 ≤240s**：`until?: 'turn-end'\|'idle'\|'input'`（默认 `turn-end`）、`timeoutMs?`（默认 60000，上限 240000）、`sinceTurn?`。事件驱动 + 500ms 兜底轮询；超时返回 `{timeout: true, status}`，落定返回 `{timeout: false, status}`，非 live 会话立即返回 `{live: false, status}` |
| `session_cancel` | Hermes → Harness | 打断会话当前的 turn（`agent.cancel({kind:'hook', reason:'harness-mcp-cancel'})`），回落 `turn/end` `reason.kind='aborted'`；`keepInbox: true` 保留未开始的排队输入（缺省清空）。非 live（重启后）会话返回 no-op——改用 `session_status` 冷读 |
| `session_list` | Hermes ← Harness | 列出可续接的会话（池/live/持久化三层）+ **上下文占用**（events/tokens/window/ratio） |
| `session_read` | Hermes ← Harness | 读会话事件流（文本/工具调用/结果），审计或续接前回顾 |
| `session_compact` | Hermes → Harness | 把会话早期历史压缩成模型摘要（需宿主加载 compaction 后端，如 dsh-compaction-basic） |
| `attach_session` | Hermes → Harness | 把会话归组到其 cwd 对应的工作区（同样受 workspaceRoots 白名单约束） |
| `rename_session` | Hermes → Harness | 给会话改名，便于会话列表归档 |

`session_send` 在 turn 入队后立即返回；结构化结果改由 `session_status` 给出（session log 是唯一事实源，因此 live 会话与重启后的冷读会话都能查）：

```json
{
  "sessionId": "...",
  "live": true,
  "source": "live",
  "cwd": "/workspace/project",
  "title": "修复失败的测试套件",
  "agentStatus": "idle",
  "phase": "idle",
  "openTurn": null,
  "lastTurn": { "turn": 3, "reason": { "kind": "completed" } },
  "prompts": [],
  "context": { "events": 142, "tokens": 18340, "pressure": 21200, "window": 128000, "ratio": 14.3 },
  "logEvents": 268,
  "changes": "改了什么",
  "verification": "怎么验证的",
  "leftovers": "遗留问题",
  "lastText": "最终回答"
}
```

`changes`/`verification`/`leftovers` 由 `parseSummary` 从**最后一个 turn 边界内**的 assistant 文本提取，agent 未按要求输出那一行 summary JSON 时为 `null`。`openTurn` 非空即表示有 turn 在飞；`prompts[]` 列出等待 `prompt_respond` 的审批/提问（此时 `phase` 为 `waiting_input`）。过程明细（消息文本、工具调用与结果）走 `session_tail`，不在 `session_status` 里。会话的生效模式（preset + 沙箱 + 审批 + 匹配的权限预设名）由 `session_list` 的 `mode` 字段报告。

### 会话模式

DSH 会话的「模式」= 三个独立旋钮 + 它们的命名捆绑：

| 类别 | 取值 | 来源 |
|------|------|------|
| Agent 预设 | `standard`（标准模式）、`code`（PTC 模式）、`cordis`（创造模式）、`minimal`（极简模式）等 | `ctx.agentPresets`（dsh agent-presets；setup 里挂载，session header 的 `agentPreset` 记录创建事实） |
| 沙箱访问模式 | `read-only` / `workspace-write` / `danger-full-access` | 会话级覆盖 = `sandbox/mode` 会话日志事件（`ctx.sandboxPolicy` 提供部署默认） |
| 审批策略 | `ask` / `never` | 会话级覆盖 = `approval/policy` 会话日志事件（`ctx.approval` 提供部署默认） |
| 权限预设（捆绑） | 如 `workspace-write` = workspace-write + ask、`danger-full-access` = danger-full-access + never | `ctx.permissionPresets` 表 |

`preset_list` 全量枚举上述内容（`only: "preset" | "sandbox" | "approval" | "permission"` 过滤，`withDetail: true` 附带更多元数据），其 `modes` 数组就是 `session_send` 的 `mode=` 可接受的规范 id 空间。创建会话时传 `mode`/`preset`/`sandbox`/`approval` 即在创建时应用该模式（指定即强制全新会话），会话从第一轮起就跑在该模式下、避免 turn 中途再提权；`session_list` 会带 mode 快照验证生效。

这打通了「客户端持久记忆 ↔ Harness 编码」的回路：记忆作为 `context` 喂给每个 turn，`session_status` 的 `changes` / `verification` / `leftovers` 可以写回客户端记忆，供下次续用。

### 派活、等待与打断

派活路径根本不阻塞 MCP 客户端，因此没有可超时的等待：

- **`session_send`** 把消息投进会话后立即返回 `{sessionId, status: "accepted", inboxDepth, openTurn}`——不等待、不超时，也没有 `timeoutMs` 参数。
- **主动轮询**用 `session_status`：便宜、随时可用且权威——有 turn 在飞时 `phase: "running"`，落定后 `"idle"`，有审批/提问待响应时 `"waiting_input"`，末尾 `turn/start` 无对应 `turn/end` 时 `"interrupted"`（持有进程已消失，绝不报 `running`）。
- **或只阻塞一小段**：`session_wait`（`until: 'turn-end' | 'idle' | 'input'`，`timeoutMs` 默认 60000、上限 240000）。超时返回 `{timeout: true, status}`，再轮询一次即可——服务端没有会丢的任务。
- **`session_cancel`** 随时打断当前 turn（会话空闲后为幂等）：`agent.cancel({kind:'hook', reason:'harness-mcp-cancel'})` 使该 turn 以 `turn/end` `reason.kind='aborted'` 收尾；传 `keepInbox: true` 保留未开始的排队输入。非 live（重启后）会话为 no-op——本来就没有在跑的东西。
- **`taskTimeoutMs`** 不再约束派活。它仍作为维护操作（如 `session_compact`）的上限存在（默认 60 分钟，`0` 关闭）。

### 会话复用 —— 由外部决定

会话**缺省按 cwd 复用**（每个 cwd 一个常驻池会话，`maxAgents` 做 LRU 上限）：每个新 turn 都追加到同一对话，省去项目上下文重载。代价是对话历史随 turn 数增长——单次调用成本上升，且无关任务共享同一上下文。调用方显式控制：

| 机制 | 行为 |
|------|------|
| `session_send` 传 `sessionId` | 精确续接该会话（多轮投喂 / 断点恢复） |
| `session_send` 传 `newSession: true` | 本次强制全新会话；旧池会话退役（dispose）但持久化保留，仍可凭其 sessionId 续接 |
| `session_send` 传 `model` / `provider` | 按次模型覆盖（对新建/resume 会话生效；池复用的会话保持原模型） |
| `session_send` 传 `mode` / `preset` / `sandbox` / `approval` | 按指定 preset/模式创建会话：`preset` 挂载 agent preset（会话预设，standard/code/cordis/minimal 等，记入 session header 的 agentPreset）；`mode` 接受 `preset_list.modes` 的任一 id——权限预设名（捆绑，如 workspace-write = workspace-write + ask）、沙箱模式、审批策略或 preset id；`sandbox`/`approval` 显式覆盖捆绑值。指定任一即**强制全新会话**（池会话无法安全套用新模式），沙箱/审批以持久会话日志事件（`sandbox/mode` / `approval/policy`）落盘，会话自第一轮起就跑在该模式下、避免 turn 中途再提权；`session_list` 带 mode 快照验证生效。续接存量 `sessionId` 时 `sandbox`/`approval` 会被拒绝（需新建）；`preset` 单独允许（resume 的 setup 里挂载） |
| 新会话传 `title`（可选） | 给新会话命名（走 sessionTitle 服务 rename）；**未传时自动按任务内容派生可读名称**（首句截断 ≤60 字符，同一 rename 路径）——新会话开箱即有名字，`session_status` 与 `session_list` 可见 `title`；复用会话不改名 |
| *(都不传)* | 缺省：复用该 cwd 的常驻池会话 |
| `session_list` | 盘点池 / live / 持久化三层会话（id、cwd、来源、标题、上下文占用），决定续接哪个 |
| `session_read` | 续接前先读该会话的转录（文本/工具调用/结果），确认它做过什么 |

也就是说**复用策略完全由外部客户端决定**：想省成本连续小改——复用（缺省）；想给大重构干净隔离——传 `newSession: true`；想跨多次调用续接长任务——传它的 `sessionId`。没有任何隐式自动轮换会意外打断你。

**忙会话保护**：LRU 淘汰与 `newSession` 都不会 dispose 正在跑 turn 的会话（`agent.status === 'idle'` 守卫）。忙会话要么被淘汰逻辑跳过（池暂时软超上限、之后补回收），要么被明确拒绝（返回 busy 提示）——正在跑的工作绝不会被悄悄掐断。

### 客户端契约

- `session_send` 上 `sessionId` **或** `cwd` **二选一必填**——`sessionId` 精确续接该会话，`cwd` 复用/创建该目录的常驻池会话。必填其一可避免误在 dsh 进程目录干活（`workspace_list` 可查可用目录）。
- `session_send` 是「投喂并接受」而非「执行并等待」：立即返回 `{sessionId, status: "accepted", inboxDepth, openTurn}`。**没有 `taskId`**，服务端不排队，也没有派活超时——长任务永远不会把你的 MCP 连接拖断。
- 回路归客户端所有：`session_status` 判断是否完成，`session_tail` 看它背后的明细。只有在「宁可阻塞一次往返、也不想轮询」时才用 `session_wait`；它最多阻塞一段 240s，然后返回 `{timeout: true, status}`。
- `phase` 由 session log 推导，而不是某个服务端状态字段：`running`（有 open turn）、`idle`（上个 turn 已收尾）、`waiting_input`（有审批/提问待响应——`prompts[]` 给出 `prompt_respond` 需要的 id）、`interrupted`（`turn/start` 始终没等到 `turn/end`，进程已不在，即没有东西在跑）。
- **结构化结果**：`changes` / `verification` / `leftovers` 在 `session_status` 上，取自最后一个 turn 的 assistant 文本。投喂模板要求 agent 以一行 JSON 收尾，例如 `{"changes":"…","verification":"…","leftovers":"…"}`——这正是 `parseSummary` 要找的内容；agent 不照做时这些字段为 `null`。
- **过程明细**：`session_tail` 只返回表面事件——消息文本、`tool/call` 与 `tool/result` 摘要、`turn/start`/`turn/end` 边界——所以 `n` 数的就是你真正想读的东西。把上次见过的 `seq` 作为 `sinceSeq` 传入，即可增量跟进正在跑的 turn。
- **冷会话同样可查**：重启后没有任何 live 会话，但 `session_status`/`session_tail` 会冷读持久化 session log，最后一个 turn、它的 summary 与事件依然看得到。`context` 仅在 live 会话可测（冷读时为 `null`）。
- 错误响应统一为 `{ "error": ... }` JSON 并带 MCP `isError` 标记，客户端可区分失败与成功。

### 审批/提问接管与 web UI 提示（notice）

MCP 拦截审批/提问后，web 会话界面会收到两条折叠提示行（`form:'notice'` 的 `user/message`：接管中 + 已响应）。**通知采用安全落点机制**：拦截（`approval/request` / 提问 provider）时只把提示入队、绝不直接写会话日志；待该工具执行完成，由 `tools/post-execute` 监听器把提示并入工具结果的 `additionalContexts`，交给 agent-loop 在 `tool/result` 之后、下个模型请求之前追加（与 `dsh-repeat-tool-reminder` / `dsh-tool-goal` 官方插件同款机制）。

- 提示行走 **DSH 原生 notice 专属呈现**：`source.form:'notice'` + `summary` 在 `additionalContexts` 路径上原样保留（web 前端 `contextForm` 的 `KNOWN_FORMS` 含 `notice`，折叠行直接展示摘要、展开为正文），与官方插件完全同款；文案按系统/状态提示撰写（`⏳` 接管中 / `✅` 已响应 / `❌` 失败，明确「审批/提问已由 MCP 接管/响应」措辞）。折叠行标题「上下文注入」是 web UI 对所有上下文行的固定命名（插件侧不可改写），但 notice 的内容语义是 notice/系统提示而非底层调用。

- 这保证 notice **从不会插进 assistant 带 `tool_calls` 的消息与其 `tool/result` 之间**——旧版（0.9.4 起）直接在拦截期 append `user/message`，若时机落在两者之间会打断模型消息序列，使下个模型请求报 `An assistant message with tool_calls must be followed by tool messages responding to each tool_call_id`（INVALID_REQUEST），会话失效。
- **已损坏的会话**（旧版代码曾把 notice 写进中间位置）：无法就地修复，直接**重新开会话**即可——`session_send` 只传 `cwd` 不传 `sessionId` 会开/复用新的池会话，或传 `newSession: true` 强制全新会话；旧会话直接忽略即可（空闲后由池 LRU 回收），不影响其他会话。本插件不重写历史日志。

### 上下文占用与压缩

既然会话随复用而增长，`session_list` 和 `session_status` 会对每个 live 会话输出**上下文占用**：

```json
"context": { "events": 142, "tokens": 18340, "pressure": 21200, "window": 128000, "ratio": 14.3 }
```

- `tokens` = 当前表面启发式 token 数，`pressure` = 最近一次请求+响应压力，均经 `ctx.tokenMeter.measure(session)` 测量（与 dsh 内部同源的固定密度定价）。
- `window` = 模型上下文窗口，经 `ctx.llm.resolveModel(agent.options.provider, agent.options.model)` 解析（按 provider:model 缓存；依次兜底 agentDefaultModel 默认选择、插件配置）。
- `ratio` = `tokens / window` 的百分比（1 位小数）——这是决定「压缩」还是「开新会话」的关键指标。
- 仅持久化行（日志未加载）、模型窗口不可解析、未加载 `tokenMeter` 服务的环境输出 `"context": null`（仅窗口未知时 window/ratio 为 null）。

当某个会话上下文过大时，直接压缩：

- **`session_compact`**（如 `{ "sessionId": "..." }`）走宿主 `ctx.compaction.compactNow`：选取一段可压缩的早期范围，替换为一段模型生成的摘要节点，返回 `compactionId`、`shadowedNodes`、`shadowedTokenCount` 与压缩前后的 `before`/`after` 占用。
- 需要宿主已加载 compaction 后端（`dsh-base` 自带的 `dsh-compaction-basic` 即可）；否则明确报错。
- 会话忙碌（正在跑 turn）返回 `busy` 分类错误；持久化会话会临时 resume、压缩后 flush 并释放。

## 安装

**要求 dsh ≥ 0.1.1-rc.2**（本版本已把 peer 依赖范围对齐到当前 Harness API，rc.6 时代的规避逻辑不再需要）。

> `@deepseek-ai/*` 全部声明为 **peerDependencies**——插件与宿主共享同一份 dsh 内部包（Cordis 服务按类身份识别，装成重复副本会导致 `inject` 失效）。请用与 profile workspace 相同的包管理器安装。

### 方式 A —— profile bundle（推荐）

```bash
cd ~/.dsh/profiles/web      # 换成你实际使用的 profile(dsh-tui / headless / web)
pnpm add @chushixixin/dsh-harness-mcp-server
```

然后在 `~/.dsh/profiles/web/package.json` 的 bundle 列表里注册：

```json
"dsh": {
  "profile": {
    "bundles": [
      "@deepseek-ai/dsh-base",
      "@deepseek-ai/dsh-web-app",
      "@chushixixin/dsh-harness-mcp-server"
    ]
  }
}
```

本包自带 `dsh.bundle.patch` 清单（`cordis.yml`），下次启动自动作为 bundle 层挂载，无需 `--patch`。

### 方式 B —— `--patch` overlay

```bash
dsh web --patch ~/.dsh/profiles/web/node_modules/@chushixixin/dsh-harness-mcp-server/cordis.yml
```

（可重复：`--patch a.yml --patch b.yml`。）

### 方式 C —— 从源码（旧式 dsh checkout 布局）

把本仓库 clone 到 Harness workspace 的 `packages/mcp/harness-mcp-server/` 下（pnpm workspace 匹配 `packages/*/*`，两级深），然后 `pnpm run build` 构建（独立 `tsc`，不再依赖 tsdown）。浏览器半区（仓库根的 `client.js`）是手写的 lazy-CJS 工厂文件，无需构建。

## 运行

```bash
export DEEPSEEK_API_KEY=...
dsh web      # 方式 A 安装后直接启动; 方式 B 记得带 --patch
```

MCP server 监听 `127.0.0.1:8090`（StreamableHTTP）。任意 MCP 客户端指向 `http://127.0.0.1:8090/mcp` 即可。

> ⚠️ **安全警告**：默认只监听 `127.0.0.1`（本机）且**未启用认证**。它暴露的是**未鉴权的远程代码执行**能力——在启用 token 之前，**不要**绑定 `0.0.0.0` 或暴露到公网/局域网（公网暴露还须加 TLS 和反向代理）。绑定非环回地址且未配 token 时，启动会打大声告警。token 可随时在 web 设置的 **MCP Server** 页开启，或经 `authToken`/`authTokens` 配置。

### Hermes 客户端配置

```bash
printf 'n\nY\n' | hermes mcp add harness_plugin --url http://127.0.0.1:8090/mcp
```

启用 token 后，客户端每个请求必须带 `Authorization: Bearer <token>` 头（在 MCP 客户端里配置该 header；Hermes 的 streamable-HTTP 服务支持自定义 header）。

## 配置

两层配置，按序解析（schema 默认 → 入口 config base → 用户层）：

- **web 设置页**（推荐）：dsh web GUI 的 **设置 → MCP Server**。运行时编辑 `host` / `port` / `authToken`——暂存式表单、逐字段重置回入口配置值、revision 设栅写入。保存即时生效：热重绑监听，不断开已建立的 MCP 会话。
- **入口配置**（cordis.yml bundle 清单或 `--patch` overlay）：全量键面，含仅入口可配的键（`provider`、`preset`、`maxAgents`、`authTokens`…）。全部可选：

| 键 | 默认 | 含义 |
|-----|---------|---------|
| `http` | `true` | 走 HTTP（StreamableHTTP） |
| `port` | `8090` | 监听端口 |
| `host` | `127.0.0.1` | 绑定地址（暴露需先加认证，见下） |
| `provider` | `deepseek-official` | 创建 agent 的后端 provider |
| `model` | *(dsh 默认)* | 创建 agent 的模型覆盖（空 = 跟随 dsh 设置） |
| `preset` | `standard` | setup 时挂载的 agent preset（写入 `meta.agentPreset`） |
| `maxAgents` | `8` | 常驻 agent 池上限（LRU 淘汰） |
| `taskTimeoutMs` | `3600000` | 维护操作（如 `session_compact`）的超时上限（60 分钟，`0` 关闭）；`session_send` 派活本身不超时 |
| `authToken` | *(无)* | Bearer token——设置后每个请求必须带 `Authorization: Bearer <token>`；也可在设置页管理（显示/隐藏 + 复制） |
| `authTokens` | *(无)* | 额外 Bearer token 列表（数组，任一命中即放行）——与 `authToken` 并存，适合给每个客户端发独立 token |
| `workspaceRoots` | *(无)* | cwd 白名单——设置后任务只能在列出的目录下运行（attach_session 的归组目标同样校验） |

> 设置页需要 dsh **web** surface：它经 web settings 文档写入，并以浏览器 bundle 形式分发。没有 settings 服务的 surface（如 headless）宿主半区照常工作，仅缺此页。若宿主行正常但设置导航里始终不出现「MCP Server」页，是部署未解析到浏览器 bundle：把已安装的包链接进宿主安装目录的 `node_modules`（或 `~/.dsh/profiles/node_modules`），例如 `ln -s ~/.dsh/profiles/web/node_modules/@chushixixin/dsh-harness-mcp-server <宿主安装>/node_modules/@chushixixin/dsh-harness-mcp-server`，然后重启。

### cordis.yml（patch 格式）

```yaml
- insert:
    - id: harness-mcp-server
      name: '@chushixixin/dsh-harness-mcp-server'
      config:
        http: true
        port: 8090
        host: 127.0.0.1        # 默认仅本机; 暴露前必须加认证
        taskTimeoutMs: 3600000
        # authToken: 'your-secret-token'     # 可选: Bearer token 认证(也可在 web 设置页管理)
        # authTokens: ['token-a', 'token-b']  # 可选: 多 token, 任一命中放行
        # workspaceRoots: ['/workspace']      # 可选: cwd 白名单
```

## 定位

它最适合当**备用工具**，而不是日常主力：日常改代码直接驱动你的主 agent 即可。当需要**上下文隔离**（大型重构会把客户端上下文撑爆）或**并行执行**互不相干的任务时，再启用它。

- agent 会话**按 cwd 复用**（避免每次调用都重新加载项目上下文——比一次性 `dsh headless` 省约 15–20 倍）。
- bash 走沙箱（`workspace-write`）：请在宿主机安装 `bubblewrap`，否则沙箱会拒绝写命令。
- 每个新的 MCP 会话拥有独立的 `McpServer` + transport（一个 MCP `McpServer` 只能连接一个 transport）。
- 续接的会话会在启动时（存量捞回）与首次使用时按 cwd（`realpath` 规范化）补挂工作区；UI 手开的会话可用 `attach_session` 手动归组。

## License

MIT
