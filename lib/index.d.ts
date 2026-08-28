/**
 * dsh-harness-mcp-server — 在 Harness 内部启动 MCP server, 暴露 Harness 能力给 Hermes(大脑)。
 *
 * 适配 dsh >= 0.1.1-rc.2(rc.6 的 agent ctx 丢 scope 问题已在上游修复)。
 *
 * 工具集:
 *   - echo                : 验证 MCP server 连通
 *   - harness_list_tools  : 列出 Harness 工具注册表
 *   - harness_status      : 系统水位总览(队列/agent 池/live 会话/运行时配置)
 *   - model_list          : 列出 provider 的模型目录, 供按任务选模型
 *   - mode_list           : 列出会话模式目录(agent preset / 沙箱访问模式 / 审批策略 / 权限预设), 供按任务选模式
 *   - workspace_list      : 列出工作区及其会话分组
 *   - agent_run           : 同步执行任务(改代码/分析/跑命令), 返回结构化结果
 *   - task_inbox          : Hermes push 结构化任务(任务+记忆上下文)到 Harness 队列, 异步执行, 返回 taskId
 *   - task_result         : 取回任务的结构化结果(changes/verification/leftovers)
 *   - task_list           : 列出最近任务(状态/目录/时间)+ 会话上下文占用, 便于批量轮询
 *   - task_cancel         : 打断一个 running 任务(超时保护也走同一条 cancel 路径)
 *   - session_list        : 列出可续接的会话(池/live/持久化三层)+ 上下文占用, 供外部决定续接哪个 sessionId
 *   - session_read        : 读会话事件流(文本/工具调用/结果), 审计或续接前回顾
 *   - session_close       : 显式退役一个池会话(持久化保留, 可凭 sessionId 续接)
 *   - session_compact     : 把会话早期历史压缩成一段模型摘要(需宿主加载 compaction 后端, 如 dsh-compaction-basic)
 *   - pending_prompts     : 列出等待输入的弹窗(审批/提问)——MCP 调用方对 DSH 弹窗不再盲目
 *   - prompt_respond      : 响应弹窗(审批 approve/deny, 提问自由文本), 解除 agent 阻塞继续
 *   - session_set_model   : 给指定会话切换模型(改 agent.options.model, 下个 turn 生效)
 *   - session_inject      : 向指定会话的 agent 队列插入补充指令(steering), 不打断当前工具执行
 *   - attach_session      : 把会话归组到其 cwd 对应的工作区(手动补给站)
 *   - rename_session      : 给已有会话改名
 *
 * 会话模式: DSH 会话的「模式」= agent 预设(standard/code/cordis/minimal 等, 来自 dsh agent-presets,
 * 经 ctx.agentPresets.mount 挂载, meta.agentPreset 记入 session header)+ 沙箱访问模式(read-only /
 * workspace-write / danger-full-access, 会话级覆盖 = sandbox/mode 日志事件)+ 审批策略(ask / never,
 * 覆盖 = approval/policy 日志事件)。权限预设(ctx.permissionPresets)把沙箱+审批捆绑命名(如
 * workspace-write = workspace-write + ask)。agent_run/task_inbox 传 preset/mode/sandbox/approval 可在
 * 创建会话时应用模式(指定即强制全新会话, 避免后续再提权); 结果带 mode 快照验证生效, mode_list 列出可用模式。
 *
 * 上下文占用: session_list/task_list 与任务 result/progress(agent_run/task_inbox/task_result/task_wait)
 * 经 ctx.tokenMeter.measure(session) 输出事件数与启发式 token 数(固定密度定价, 与 dsh token-meter 同源),
 * 并经 ctx.llm.resolveModelInfo 解析模型 contextWindow 得占用比 ratio=tokens/window(百分比);
 * tokenMeter 缺失时整个 context 为 null, 窗口不可解析时 window/ratio 为 null。
 *
 * 会话复用策略(外部显式控制): 缺省按 cwd 复用常驻池会话(省上下文加载, 但历史随任务数增长);
 * 外部可传 newSession:true 强制全新会话(旧会话退役但持久化保留), 或传 sessionId 精确续接, 或用
 * session_list/session_close 自行盘点与退役 —— 是否复用完全由调用方决定。
 *
 * 客户端契约要点:
 *  - agent_run 同步执行, 传 timeoutMs 超时自动转异步(返回 taskId, 用 task_result/task_wait/task_cancel 跟进);
 *    所有任务(含同步)都注册进队列并回填真实 taskId, 均可查可取消。
 *  - 进度汇报: task_wait/task_result/转异步响应/未完成任务行带 progress 字段
 *    {status, events, toolCalls, currentTool:{name,args}, lastText}, 客户端可据此实时汇报"正在执行到哪一步"。
 *  - 取消语义: agent 已就绪的任务走 cancel 钩子; 等锁/排队中的任务(task_cancel 置 cancelled)
 *    在锁释放后执行前由 shouldAbort 检查中止 —— 任何状态的任务都可取消。
 *  - 错误响应统一 {error:...} JSON + isError 标记。
 *  - 忙会话保护: LRU 淘汰 / session_close / newSession 都不会 dispose 正在跑任务的 agent(软上限/拒绝/摘除)。
 *
 * sessionId 续接: 指定 sessionId 时按 本进程池 → live 会话(UI 手开)→ 持久化 resume 三级接管,
 * 前两者都找不到才报错, 所以进程重启前/UI 手开的会话也能续接。
 * 工作区分组: cwd 先 realpath 规范化再 `workspaceRegistry.resolveByPath ?? create` + attachSession;
 * 启动时对存量未分组会话补挂一次(存量捞回)。
 *
 * 回路: Hermes 记忆 →(context)→ task_inbox → Harness agent 执行 → 结果进队列 → task_result → Hermes 持久化
 */
import type { Context } from '@deepseek-ai/cordis';
/** Cordis 插件名 */
export declare const name = "harness-mcp-server";
/** 插件版本(与 package.json 同步; MCP initialize 时上报) */
export declare const VERSION = "0.9.7";
/**
 * 声明依赖的核心服务。
 * workspaceRegistry/sessionPersistence/sessions 是续接/归组三个增量用到的服务——
 * 漏声明会在真实启动时拿不到服务(本插件曾经踩过, 务必与代码里的 ctx.get 对齐)。
 */
export declare const inject: string[];
/** 插件配置 */
export interface Config {
    http?: boolean;
    port?: number;
    host?: string;
    /** 后端 provider(默认 deepseek-official) */
    provider?: string;
    /** 执行任务的模型(默认 deepseek-v4-flash) */
    model?: string;
    /** 挂载的 agent preset(默认 standard) */
    preset?: string;
    /** 任务队列容量上限(默认 100) */
    maxQueue?: number;
    /** 已完成任务保留毫秒数(默认 60 分钟, 对齐 taskTimeoutMs, 避免异步工作流丢结果) */
    taskTtlMs?: number;
    /** 常驻 agent 会话上限(默认 8, LRU 淘汰) */
    maxAgents?: number;
    /** 单任务超时毫秒数, 超时自动 cancel 并回收部分输出(默认 60 分钟; 0 = 不限制) */
    taskTimeoutMs?: number;
    /** Bearer token 认证(设置后所有请求必须带 Authorization: Bearer <token>) */
    authToken?: string;
    /** cwd 白名单(设置后 agent 只能在列出的目录下干活) */
    workspaceRoots?: string[];
}
/**
 * 插件入口: 启动 MCP server(StreamableHTTP, 跨网), 通过 ctx 桥接 Harness 能力。
 */
export declare function apply(ctx: Context, config?: Config): Promise<void>;
