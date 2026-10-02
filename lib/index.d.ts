/**
 * dsh-harness-mcp-server — 在 Harness 内部启动 MCP server, 暴露 Harness 能力给 Hermes(大脑)。
 *
 * 适配 dsh >= 0.2.0-rc.1(rc.6 的 agent ctx 丢 scope 问题已在上游修复)。
 *
 * 架构(v0.11.0): 「任务」层已降维为 **session + turn** —— 不再有 taskId/任务队列/内存态任务结果。
 *  - 派活 = 往一个会话投喂一个 turn: session_send 组装 message 后 agent.followup() 立即返回(不等待/不超时阻塞)。
 *  - 查询 = 主动去查: session_status / session_tail 以 **session log 为唯一事实源**。
 *    live 会话读内存日志 + turnBoundaryProjection; 非 live(重启后)冷读持久化日志, 因此重启不丢状态。
 *  - 结构化产物(changes/verification/leftovers)保留: 投喂模板要求 agent 输出一行 summary JSON,
 *    session_status 从「最后一个 turn 边界内的 assistant 文本」parseSummary 提取。
 *
 * 工具集:
 *   - echo                : 验证 MCP server 连通
 *   - harness_list_tools  : 列出 Harness 工具注册表
 *   - harness_status      : 系统水位总览(agent 池/live 会话/运行时配置)
 *   - model_list          : 列出 provider 的模型目录, 供按任务选模型
 *   - preset_list         : 列出 agent preset(会话预设)目录, 并列出与预设定向相关的其余维度(沙箱访问模式 / 审批策略 / 权限预设)
 *   - workspace_list      : 列出工作区及其会话分组
 *   - session_send        : 【派活入口】把一条任务作为一个 turn 投喂进会话, 立即返回(不阻塞)
 *   - session_status      : 【主动查询】phase/openTurn/lastTurn/prompts/context/summary, 以 session log 为准
 *   - session_tail        : 【过程明细】按需拉取表面事件(消息文本/工具调用与结果/turn 边界)
 *   - session_wait        : 【可选阻塞】单段 ≤240s 等 turn-end / idle / input; 超时返回 {timeout:true}
 *   - session_cancel      : 打断会话当前回合(替代旧 task_cancel)
 *   - session_list        : 列出可续接的会话(池/live/持久化三层)+ 上下文占用, 供外部决定续接哪个 sessionId
 *   - session_read        : 读会话事件流(文本/工具调用/结果), 审计或续接前回顾
 *   - session_compact     : 把会话早期历史压缩成一段模型摘要(需宿主加载 compaction 后端, 如 dsh-compaction-basic)
 *   - pending_prompts     : 列出等待输入的弹窗(审批/提问)——MCP 调用方对 DSH 弹窗不再盲目
 *   - prompt_respond      : 响应弹窗(审批 approve/deny, 提问自由文本), 解除 agent 阻塞继续
 *   - session_set_model   : 给指定会话切换模型(改 agent.options.model, 下个 turn 生效)
 *   - session_inject      : 向指定会话的 agent 队列插入补充指令(steering), 不打断当前工具执行
 *   - attach_session      : 把会话归组到其 cwd 对应的工作区(手动补给站)
 *   - rename_session      : 给已有会话改名
 *
 * 会话模式: DSH 会话的「模式」= agent 预设(standard/code/cordis/minimal 等, 来自 dsh-agent-preset-registry,
 * 经 ctx.agentPresets.mount 挂载, meta.agentPreset 记入 session header)+ 沙箱访问模式(read-only /
 * workspace-write / danger-full-access, 会话级覆盖 = sandbox/mode 日志事件)+ 审批策略(ask / never,
 * 覆盖 = approval/policy 日志事件)。权限预设(ctx.permissionPresets)把沙箱+审批捆绑命名(如
 * workspace-write = workspace-write + ask)。session_send 传 preset/mode/sandbox/approval 可在
 * 创建会话时应用(指定即强制全新会话, 避免后续再提权); preset_list 列出可用 preset 及其定向的其余维度。
 *
 * 上下文占用: session_list 与 session_status(仅 live)经 ctx.tokenMeter.measure(session) 输出事件数与
 * 启发式 token 数(固定密度定价, 与 dsh token-meter 同源), 并经 ctx.llm.resolveModelInfo 解析模型
 * contextWindow 得占用比 ratio=tokens/window(百分比); tokenMeter 缺失时整个 context 为 null,
 * 窗口不可解析时 window/ratio 为 null。非 live 会话没有 Session 对象可供计量, context 为 null。
 *
 * 会话复用策略(外部显式控制): 缺省按 cwd 复用常驻池会话(省上下文加载, 但历史随任务数增长);
 * 外部可传 newSession:true 强制全新会话(旧会话退役但持久化保留), 或传 sessionId 精确续接, 或用
 * session_list 自行盘点(常驻池按 LRU 自动淘汰, 退役只由池策略决定) —— 是否复用完全由调用方决定。
 *
 * 客户端契约要点:
 *  - session_send 立即返回 {sessionId, inboxDepth, openTurn}; 之后用 session_status 主动查询,
 *    或用 session_wait 可选阻塞一段(≤240s)。没有 taskId, 也没有服务端排队与 TTL。
 *  - 取消语义: session_cancel → agent.cancel({kind:'hook',reason:'harness-mcp-cancel'}), 回落 turn/end
 *    reason.kind='aborted'(keepInbox=true 时保留未开始的排队输入)。
 *  - 错误响应统一 {error:...} JSON + isError 标记。
 *  - 忙会话保护: LRU 淘汰与 newSession 都不会 dispose 正在跑 turn 的 agent(池软超上限, 任务落定后再回收)。
 *
 * sessionId 续接: 指定 sessionId 时按 本进程池 → live 会话(UI 手开)→ 持久化 resume 三级接管,
 * 前两者都找不到才报错, 所以进程重启前/UI 手开的会话也能续接。
 * 工作区分组: cwd 先 realpath 规范化再 `workspaceRegistry.resolveByPath ?? create` + attachSession;
 * 启动时对存量未分组会话补挂一次(存量捞回)。
 *
 * 回路: Hermes 记忆 →(context)→ session_send → Harness agent 执行一个 turn → 结果落 session log
 *       → session_status/session_tail 主动查询 → Hermes 持久化
 */
import type { Context } from '@deepseek-ai/cordis';
import type { ContextFormed } from '@deepseek-ai/dsh-llm';
import schemastery from '@deepseek-ai/schemastery';
declare module '@deepseek-ai/dsh-llm' {
    interface MessageSourceMap {
        'harness-mcp-server': {
            kind: 'harness-mcp-server';
        } & ContextFormed;
    }
}
/** Cordis 插件名 */
export declare const name = "harness-mcp-server";
/** 插件版本(与 package.json 同步; MCP initialize 时上报) */
export declare const VERSION = "0.13.0";
/**
 * 声明依赖的核心服务。
 * workspaceRegistry/sessionPersistence/sessions 是续接/归组三个增量用到的服务——
 * 漏声明会在真实启动时拿不到服务(本插件曾经踩过, 务必与代码里的 ctx.get 对齐)。
 */
export declare const inject: string[];
/**
 * 插件配置 schema(dsh 0.2.0 形态): 命名空间 = profile 条目 id(`harness-mcp-server`)。
 * 只有 `.volatile()` 字段会进自动生成的设置表单, 并可在设置页热改; 其余字段仍只从入口 config 读。
 * 类型 `Config` 由 schema 推导(`Schemastery.TypeT`), 供 apply 签名等内部使用。
 */
export declare const Config: schemastery<Schemastery.ObjectS<NoInfer<{
    http: schemastery<boolean, boolean, "defined">;
    port: schemastery<number, number, "volatile-defined">;
    host: schemastery<string, string, "volatile-defined">;
    provider: schemastery<string, string, "defined">;
    model: schemastery<string, string, "defined">;
    preset: schemastery<string, string, "defined">;
    maxAgents: schemastery<number, number, "defined">;
    taskTimeoutMs: schemastery<number, number, "defined">;
    authToken: schemastery<string, string, "volatile-defined">;
    authTokens: schemastery<string[], string[], "defined">;
    workspaceRoots: schemastery<string[], string[], "defined">;
}>>, Schemastery.ObjectT<NoInfer<{
    http: schemastery<boolean, boolean, "defined">;
    port: schemastery<number, number, "volatile-defined">;
    host: schemastery<string, string, "volatile-defined">;
    provider: schemastery<string, string, "defined">;
    model: schemastery<string, string, "defined">;
    preset: schemastery<string, string, "defined">;
    maxAgents: schemastery<number, number, "defined">;
    taskTimeoutMs: schemastery<number, number, "defined">;
    authToken: schemastery<string, string, "volatile-defined">;
    authTokens: schemastery<string[], string[], "defined">;
    workspaceRoots: schemastery<string[], string[], "defined">;
}>>, "plain">;
/** 插件配置类型: 从上面的 schema 推导(volatile 字段为 `Volatile<T>`, 经 `.get()` 读取)。 */
export type Config = Schemastery.TypeT<typeof Config>;
/**
 * 插件入口: 启动 MCP server(StreamableHTTP, 跨网), 通过 ctx 桥接 Harness 能力。
 */
export declare function apply(ctx: Context, config?: Partial<Config>): Promise<void>;
