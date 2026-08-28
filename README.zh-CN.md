# dsh-harness-mcp-server

> 把 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) 的 agent 能力暴露成一个 **MCP server**，让任意 MCP 客户端（如 [Hermes](https://hermes-agent.nousresearch.com/)）都能驱动 Harness 执行真实的编码任务。

**Hermes 是大脑（pro），Harness 是胳膊（flash）—— 1+1>2。**

[![npm version](https://img.shields.io/npm/v/@chushixixin/dsh-harness-mcp-server)](https://www.npmjs.com/package/@chushixixin/dsh-harness-mcp-server)
[![license](https://img.shields.io/npm/l/@chushixixin/dsh-harness-mcp-server)](./LICENSE)

> 📖 [English](./README.md) · 中文（当前页）

## 为什么需要它

Harness 自带强大的 agent 运行时（工具、LLM、agent、会话），但它本身是一个 **Cordis 应用**，别的 agent 无法直接调用。这个插件把 Harness「由内向外」翻转：在 Harness **内部**启动一个真正的 **MCP server**（StreamableHTTP），通过 `ctx` 桥接 Harness 的核心服务 —— `ctx.agents`、`ctx.agentPresets`、`ctx.tools` —— 让外部的「大脑」能把真实工作交给 Harness 的「胳膊」。

```
Hermes (MCP 客户端, 大脑)
   │  agent_run / task_inbox (HTTP)
   ▼
dsh-harness-mcp-server (MCP server, :8090)
   │  ctx.agents.create → 挂载 'standard' preset
   ▼
Harness agent (flash) — 完整工具集: bash、fs、todo、web…
```

## 工具

| 工具 | 方向 | 用途 |
|------|-----------|---------|
| `echo` | — | 验证 MCP 连通性 |
| `harness_list_tools` | — | 列出 Harness 已注册的工具名 |
| `harness_status` | Hermes ← Harness | 运维总览：队列 / agent 池 / live 会话 / 运行时配置 |
| `model_list` | Hermes ← Harness | 列出所有已注册 provider 的模型（含已声明未激活的 provider）；`withWindow` 附加上下文窗口 |
| `workspace_list` | Hermes ← Harness | 列出工作区及其会话分组 |
| `agent_run` | Hermes → Harness | 同步执行任务，返回结构化结果（传 `timeoutMs` 可把长任务自动转异步） |
| `task_inbox` | Hermes → Harness | 把结构化任务（任务 + 记忆上下文 + cwd）推入异步队列（支持 per-task `timeoutMs`） |
| `task_result` | Hermes ← Harness | 取回队列任务的结构化结果 |
| `task_wait` | Hermes ← Harness | 服务端阻塞等待任务完成（一次往返替代多次轮询） |
| `task_list` | Hermes ← Harness | 列出最近任务（状态/cwd/时间/错误/会话上下文/进行中进度），可按 sessionId 过滤 |
| `task_cancel` | Hermes → Harness | 打断运行中的任务（与超时保护共用 cancel 路径） |
| `session_list` | Hermes ← Harness | 列出可续接的会话（池/live/持久化三层）+ **上下文占用**（events/tokens/window/ratio） |
| `session_read` | Hermes ← Harness | 读会话事件流（文本/工具调用/结果），审计或续接前回顾 |
| `session_close` | Hermes → Harness | 显式退役池会话（持久化保留，仍可凭 sessionId 续接） |
| `session_compact` | Hermes → Harness | 把会话早期历史压缩成模型摘要（需宿主加载 compaction 后端，如 dsh-compaction-basic） |
| `attach_session` | Hermes → Harness | 把会话归组到其 cwd 对应的工作区（同样受 workspaceRoots 白名单约束） |
| `rename_session` | Hermes → Harness | 给会话改名，便于会话列表归档 |

每个任务结果都是**结构化**的：

```json
{
  "sessionId": "...",
  "assistantText": "最终回答",
  "toolCalls": [{ "name": "bash", "args": "..." }],
  "toolResults": ["命令输出"],
  "changes": "改了什么",
  "verification": "怎么验证的",
  "leftovers": "遗留问题",
  "timeout": false
}
```

这打通了「客户端持久记忆 ↔ Harness 编码」的回路：记忆作为 `context` 喂给每个任务，结果（`changes` / `verification` / `leftovers`）可以写回客户端记忆，供下次续用。

### 超时与打断

agent 循环一旦卡死，任务会无限阻塞 MCP 客户端。有两道保护：

- **超时**（`taskTimeoutMs`，默认 60 分钟，`0` 关闭）：到点自动 `cancel` agent，回收部分输出，`result.timeout` 置为 `true`，`leftovers` 提示可用 `sessionId` 续接。
- **`task_cancel`**：随时按 `taskId` 打断 `queued`/`running` 任务（已结束任务为幂等 no-op）。

### 会话复用 —— 由外部决定

会话**缺省按 cwd 复用**（每个 cwd 一个常驻池会话，`maxAgents` 做 LRU 上限）：每次新任务都追加到同一对话，省去项目上下文重载。代价是对话历史随任务数增长——单次调用成本上升，且无关任务共享同一上下文。调用方显式控制：

| 机制 | 行为 |
|------|------|
| `agent_run` / `task_inbox` 传 `sessionId` | 精确续接该会话（多轮投喂 / 断点恢复） |
| `agent_run` / `task_inbox` 传 `newSession: true` | 本次强制全新会话；旧池会话退役（dispose）但持久化保留，仍可凭其 sessionId 续接 |
| `agent_run` / `task_inbox` 传 `model` / `provider` | 按次模型覆盖（对新建/resume 会话生效；池复用的会话保持原模型） |
| *(都不传)* | 缺省：复用该 cwd 的常驻池会话 |
| `session_list` | 盘点池 / live / 持久化三层会话（id、cwd、来源、标题、上下文占用），决定续接哪个 |
| `session_read` | 续接前先读该会话的转录（文本/工具调用/结果），确认它做过什么 |
| `session_close` | 显式退役池会话（释放 live 句柄；持久化保留可续接） |

也就是说**复用策略完全由外部客户端决定**：想省成本连续小改——复用（缺省）；想给大重构干净隔离——传 `newSession: true`；想跨多次调用续接长任务——传它的 `sessionId`。没有任何隐式自动轮换会意外打断你。

**忙会话保护**：LRU 淘汰、`session_close`、`newSession` 都不会 dispose 正在跑任务的会话（`agent.status === 'idle'` 守卫）。忙会话要么被淘汰逻辑跳过（池暂时软超上限、之后补回收），要么被明确拒绝（返回 busy 提示）——正在跑的工作绝不会被悄悄掐断。

### 客户端契约

- 未续接会话（无 `sessionId`）时 **`cwd` 必填**——避免误在 dsh 进程目录干活（`workspace_list` 可查可用目录）。
- 所有任务（同步/异步）都注册进队列并回填真实 `taskId`，可被 `task_result`/`task_wait` 查询、`task_cancel` 取消。**任何状态的任务都可取消**：agent 已就绪的走 cancel 钩子；仍在 cwd 锁上等待（agent 未启动）的任务会被标记 `cancelled` 并在启动前中止（两种情况 `task_cancel` 都返回 `cancelled: true`）。
- `task_inbox` 支持 per-task `timeoutMs`（覆盖全局 `taskTimeoutMs`；`0` 关闭）。
- `task_list` 支持 `sessionId` 过滤，查看单个会话的任务列表。
- `agent_run` 传 `timeoutMs`（默认 `taskTimeoutMs` 60 分钟）：超过等待窗口返回 `{ "status": "async", "taskId": ... }` 并在后台继续执行——建议设得比客户端 HTTP 超时小（如 `120000`），避免长任务被断连。
- **进度汇报**：`task_wait`、`task_result`、转异步响应、`task_list` 的运行中行都带 `progress` 字段，客户端可实时汇报"正在执行到哪一步"：

  ```json
  "progress": { "status": "running", "events": 42, "toolCalls": 3,
                "currentTool": { "name": "bash", "args": "npm test -- --filter x" },
                "lastText": "正在跑失败的测试套件…" }
  ```

  `events`/`toolCalls` 只统计本任务从起点开始的日志增量；`currentTool` 是最近一次进行中的工具调用；`lastText` 是 agent 最近可见的文本。
- 错误响应统一为 `{ "error": ... }` JSON 并带 MCP `isError` 标记，客户端可区分失败与成功。

### 审批/提问接管与 web UI 提示（notice）

MCP 拦截审批/提问后，web 会话界面会收到两条折叠提示行（`form:'notice'` 的 `user/message`：接管中 + 已响应）。**通知采用安全落点机制**：拦截（`approval/request` / 提问 provider）时只把提示入队、绝不直接写会话日志；待该工具执行完成，由 `tools/post-execute` 监听器把提示并入工具结果的 `additionalContexts`，交给 agent-loop 在 `tool/result` 之后、下个模型请求之前追加（与 `dsh-repeat-tool-reminder` / `dsh-tool-goal` 官方插件同款机制）。

- 这保证 notice **从不会插进 assistant 带 `tool_calls` 的消息与其 `tool/result` 之间**——旧版（0.9.4 起）直接在拦截期 append `user/message`，若时机落在两者之间会打断模型消息序列，使下个模型请求报 `An assistant message with tool_calls must be followed by tool messages responding to each tool_call_id`（INVALID_REQUEST），会话失效。
- **已损坏的会话**（旧版代码曾把 notice 写进中间位置）：无法就地修复，直接**重新开会话**即可——`agent_run` 不带 `sessionId` 会开/复用新会话，或传 `newSession: true` 强制全新会话；旧会话可忽略或 `session_close` 退役，不影响其他会话。本插件不重写历史日志。

### 上下文占用与压缩

既然会话随复用而增长，`session_list` 和 `task_list` 会对每个 live 会话行输出**上下文占用**：

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
- 会话忙碌（正在跑任务）返回 `busy` 分类错误；持久化会话会临时 resume、压缩后 flush 并释放。

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

把本仓库 clone 到 Harness workspace 的 `packages/mcp/harness-mcp-server/` 下（pnpm workspace 匹配 `packages/*/*`，两级深），然后 `pnpm run build` 构建（独立 `tsc`，不再依赖 tsdown）。

## 运行

```bash
export DEEPSEEK_API_KEY=...
dsh web      # 方式 A 安装后直接启动; 方式 B 记得带 --patch
```

MCP server 监听 `127.0.0.1:8090`（StreamableHTTP）。任意 MCP 客户端指向 `http://127.0.0.1:8090/mcp` 即可。

> ⚠️ **安全警告**：默认只监听 `127.0.0.1`（本机）。它暴露的是**未鉴权的远程代码执行**能力——在没有加认证、TLS 和反向代理之前，**不要**绑定 `0.0.0.0` 或暴露到公网/局域网。

### Hermes 客户端配置

```bash
printf 'n\nY\n' | hermes mcp add harness_plugin --url http://127.0.0.1:8090/mcp
```

## 配置

patch 条目的 `config` 全部可选：

| 键 | 默认 | 含义 |
|-----|---------|---------|
| `http` | `true` | 走 HTTP（StreamableHTTP） |
| `port` | `8090` | 监听端口 |
| `host` | `127.0.0.1` | 绑定地址（暴露需先加认证，见下） |
| `provider` | `deepseek-official` | 创建 agent 的后端 provider |
| `model` | *(dsh 默认)* | 创建 agent 的模型覆盖（空 = 跟随 dsh 设置） |
| `preset` | `standard` | setup 时挂载的 agent preset（写入 `meta.agentPreset`） |
| `maxQueue` | `100` | 异步队列容量（排队 + 执行中） |
| `taskTtlMs` | `3600000` | 已完成任务在队列中的保留时长（60 分钟，与 taskTimeoutMs 对齐） |
| `maxAgents` | `8` | 常驻 agent 池上限（LRU 淘汰） |
| `taskTimeoutMs` | `3600000` | 单任务超时；超时自动 cancel 并回收部分结果（60 分钟，`0` 关闭） |
| `authToken` | *(无)* | Bearer token——设置后每个请求必须带 `Authorization: Bearer <token>` |
| `workspaceRoots` | *(无)* | cwd 白名单——设置后任务只能在列出的目录下运行（attach_session 的归组目标同样校验） |

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
        # authToken: 'your-secret-token'     # 可选: Bearer token 认证
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
