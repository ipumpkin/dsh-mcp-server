/**
 * dsh-harness-mcp-server — 在 Harness 内部启动 MCP server, 暴露 Harness 能力给 Hermes(大脑)。
 *
 * 适配 dsh >= 0.1.1-rc.2(rc.6 的 agent ctx 丢 scope 问题已在上游修复)。
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
 *   - mode_list           : 列出会话模式目录(agent preset / 沙箱访问模式 / 审批策略 / 权限预设)
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
 * 会话模式: DSH 会话的「模式」= agent 预设(standard/code/cordis/minimal 等, 来自 dsh agent-presets,
 * 经 ctx.agentPresets.mount 挂载, meta.agentPreset 记入 session header)+ 沙箱访问模式(read-only /
 * workspace-write / danger-full-access, 会话级覆盖 = sandbox/mode 日志事件)+ 审批策略(ask / never,
 * 覆盖 = approval/policy 日志事件)。权限预设(ctx.permissionPresets)把沙箱+审批捆绑命名(如
 * workspace-write = workspace-write + ask)。session_send 传 preset/mode/sandbox/approval 可在
 * 创建会话时应用模式(指定即强制全新会话, 避免后续再提权); mode_list 列出可用模式。
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

// ── Context 声明合并: 让 ctx.tools / ctx.llm / ctx.agents 有类型 ──
import type {} from '@deepseek-ai/dsh-tools'
import type {} from '@deepseek-ai/dsh-llm'
import type {} from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-agent-presets'

import type { Context } from '@deepseek-ai/cordis'
import type { Agent, AgentHandle } from '@deepseek-ai/dsh-agent'
import { z } from 'zod'
import { boundContextSummary, createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionHeader } from '@deepseek-ai/dsh-session'
import { scopeOf } from '@deepseek-ai/dsh-scope'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { randomUUID, timingSafeEqual } from 'node:crypto'
import schemastery from '@deepseek-ai/schemastery'
import { readdir, readFile, realpath } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { homedir } from 'node:os'
import http from 'node:http'
import { join, resolve } from 'node:path'

/** Cordis 插件名 */
export const name = 'harness-mcp-server'

/** 插件版本(与 package.json 同步; MCP initialize 时上报) */
export const VERSION = "0.11.0"

/**
 * 声明依赖的核心服务。
 * workspaceRegistry/sessionPersistence/sessions 是续接/归组三个增量用到的服务——
 * 漏声明会在真实启动时拿不到服务(本插件曾经踩过, 务必与代码里的 ctx.get 对齐)。
 */
export const inject = ['tools', 'llm', 'agents', 'agentPresets', 'workspaceRegistry', 'sessionPersistence', 'sessions']

/** 插件配置 */
export interface Config {
  http?: boolean
  port?: number
  host?: string
  /** 后端 provider(默认 deepseek-official) */
  provider?: string
  /** 执行任务的模型(默认 deepseek-v4-flash) */
  model?: string
  /** 挂载的 agent preset(默认 standard) */
  preset?: string
  /** 常驻 agent 会话上限(默认 8, LRU 淘汰) */
  maxAgents?: number
  /** 单次维护操作(如 session_compact)的超时毫秒数(默认 60 分钟; 0 = 不限制) */
  taskTimeoutMs?: number
  /** Bearer token 认证(设置后所有请求必须带 Authorization: Bearer <token>) */
  authToken?: string
  /** Bearer token 列表(任一命中即放行; 与 authToken 并存, 适合多客户端各自持一个 token) */
  authTokens?: string[]
  /** cwd 白名单(设置后 agent 只能在列出的目录下干活) */
  workspaceRoots?: string[]
}

/** 运行时配置(apply 时从 config 初始化, 提供安全默认值) */
const runtimeConfig = {
  provider: 'deepseek-official',
  // 空字符串 = 不覆盖 model, 跟随 dsh 的用户/默认设置; 显式配置则覆盖
  model: '',
  preset: 'standard',
  maxAgents: 8,
  taskTimeoutMs: 60 * 60 * 1000,
  authToken: '',
  authTokens: [] as string[],
  workspaceRoots: [] as string[],
}

/**
 * Bearer token 校验(常时时间比较, 防时序侧信道逐字节猜 token)。
 * 有效 token 集 = authToken + authTokens; 空集 = 未启用认证(直接放行)。
 * header 须为 `Bearer <token>`; timingSafeEqual 要求等长输入,
 * 先比长度(长度不同直接 false, 不泄漏 token 内容), 等长才进常时比较。
 */
function bearerTokenOk(header: string | undefined): boolean {
  const tokens = [
    ...(runtimeConfig.authToken ? [runtimeConfig.authToken] : []),
    ...runtimeConfig.authTokens,
  ]
  if (tokens.length === 0) return true
  if (typeof header !== 'string' || !header.startsWith('Bearer ')) return false
  const given = Buffer.from(header.slice('Bearer '.length), 'utf8')
  return tokens.some((t) => {
    const expected = Buffer.from(t, 'utf8')
    return expected.length === given.length && timingSafeEqual(expected, given)
  })
}

// ── settings 命名空间: web 设置面板可配置的持久化子集(生效值 = schema 默认 → 入口 config base → 用户层) ──

/** settings 命名空间名(web 设置卡片以它为键配对) */
const SETTINGS_NAMESPACE = 'harness-mcp-server'

/** 设置界面可编辑的持久化字段(入口 config 里 taskTimeoutMs 等其余字段不进 settings) */
interface HarnessMcpSettings {
  host: string
  port: number
  authToken: string
}

/** schemastery schema: 也是设置面板渲染与 wire 校验的依据 */
const HarnessMcpSettingsSchema = schemastery.object({
  host: schemastery.string().default('127.0.0.1'),
  port: schemastery.number().default(8090),
  authToken: schemastery.string().default(''),
})

/** ctx.settings 的结构化最小面(避免绑定宿主具体实现类型) */
interface SettingsProviderLike {
  register(
    ns: string,
    schema: unknown,
    options?: {
      base?: Partial<HarnessMcpSettings>
      applies?: 'live' | 'restart'
      validate?: (value: HarnessMcpSettings) => void
    },
  ): SettingsScopeLike
}

/** register 返回的命名空间 scope 的结构化最小面 */
interface SettingsScopeLike {
  get(): HarnessMcpSettings
  watch(callback: (next: HarnessMcpSettings, prev: HarnessMcpSettings) => void | Promise<void>): () => void
}

// ── 会话「模式」词汇: agent 预设 + 沙箱访问模式 + 审批策略 ──
// 与 dsh-sandbox 的 SandboxMode 对齐(会话级覆盖以 sandbox/mode 日志事件为唯一存储)
const SANDBOX_MODES = ['read-only', 'workspace-write', 'danger-full-access'] as const
/** 沙箱访问模式的中文简述(与 dsh-sandbox 词汇一致) */
const SANDBOX_MODE_DESCRIPTIONS: Record<string, string> = {
  'read-only': '只读: 仅允许必要 sink(/dev/null 等), 禁止一切文件写入',
  'workspace-write': '工作区可写: 允许写会话 cwd(工作区根)与后端定义的临时区',
  'danger-full-access': '完全访问: 绕过沙箱文件约束(危险, 建议仅在可信环境)',
}
// 与 dsh-user-approval 的 ApprovalPolicy 对齐(会话级覆盖以 approval/policy 日志事件为唯一存储)
const APPROVAL_POLICIES = ['ask', 'never'] as const
/** 审批策略的中文简述(与 dsh-user-approval 词汇一致) */
const APPROVAL_POLICY_DESCRIPTIONS: Record<string, string> = {
  ask: '每次受限操作弹窗询问审批(交给应答链; 无应答者时 fail-closed)',
  never: '永不询问: 自动拒绝每个审批请求(CI/无人值守的确定性姿态)',
}

/** 工具回调统一返回 MCP text content */
function out(content: string) {
  return { content: [{ type: 'text' as const, text: content }] }
}

/** 错误响应: 结构化 JSON 文本 + isError 标记(MCP 客户端可据此识别失败, 不写回记忆) */
function err(content: string) {
  return { content: [{ type: 'text' as const, text: content }], isError: true as const }
}

/**
 * 从任务内容派生一个可读的会话标题(新建会话未显式传 title 时使用, 走 sessionTitle 服务的 rename)。
 * 背景: DSH 原生的自动命名只对 source.kind === 'user' 的消息触发(collectSessionTitleMessages 过滤),
 * 而本插件投喂的全是 plugin 来源消息, 所以 MCP 新建会话永远得不到名字, session_list 里一串空名。
 * 这里与 dsh-session-title 的 deterministic fallback 同思路: 清控制字符/转义、归一空白、
 * 取首句(句读/换行截断), 超长截断 —— 保证每个新会话开箱即有可读名称。
 */
function deriveSessionTitle(text: string, maxChars = 60): string {
  const cleaned = String(text ?? '')
    .replace(/[\u001B\u009B]/g, '')
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!cleaned) return ''
  const firstSentence = cleaned.split(/[。！？!?\n]/, 1)[0] ?? cleaned
  const sentence = firstSentence.trim()
  if (sentence.length <= maxChars) return sentence
  return `${sentence.slice(0, maxChars - 1)}…`
}

/** sessionTitle 服务的只读视图(可选依赖; 未加载时返回 undefined) */
interface SessionTitleView {
  get?: (s: unknown) => { title?: string } | undefined
  rename?: (s: unknown, t: string) => unknown
}

/** 读会话当前标题快照(sessionTitle 服务; 缺失/尚无标题返回 undefined) */
function sessionTitleOf(ctx: Context, session: unknown): string | undefined {
  const st = ctx.get('sessionTitle') as SessionTitleView | undefined
  return st?.get?.(session)?.title
}

/** 给会话命名(sessionTitle 服务 rename; 失败仅告警, 不阻断任务)。返回实际生效的标题(可能 undefined) */
function renameSessionSafe(ctx: Context, session: unknown, title: string): string | undefined {
  try {
    const st = ctx.get('sessionTitle') as SessionTitleView | undefined
    if (!st?.rename) return undefined
    const snapshot = st.rename(session, title) as { title?: string } | undefined
    return snapshot?.title ?? title
  } catch (e) {
    console.warn('[harness-mcp-server] session title set failed:', String(e))
    return undefined
  }
}

/** 工作区视图(ctx.get('workspaceRegistry')): 可选依赖, headless/无 workspace 插件的环境自动跳过 */
interface WorkspaceView {
  id: string
  path: string
  sessionIds: readonly SessionId[]
  attachSession?: (sessionId: SessionId) => Promise<void>
}
interface WorkspaceRegistryView {
  create?: (path: string) => Promise<WorkspaceView>
  resolveByPath?: (path: string) => Promise<WorkspaceView | undefined>
  list?: () => WorkspaceView[]
}

/**
 * cwd realpath 规范化: 解析符号链接与 .. 段, 使 cwd 能与 workspace.path(存储时为 realpath 规范化值)
 * 精确比对——这是官方 attachSession 强校验通过的前提。目录不存在时回退 resolve 结果, 由调用方告警不阻断。
 */
async function canonicalCwd(raw: string): Promise<string> {
  try {
    return await realpath(raw)
  } catch {
    return resolve(raw)
  }
}

/** 官方 session.create RPC 同款姿势: resolveByPath ?? create, 幂等; 无 workspaceRegistry 时返回 undefined */
async function ensureWorkspace(ctx: Context, canonical: string): Promise<WorkspaceView | undefined> {
  const registry = ctx.get('workspaceRegistry') as WorkspaceRegistryView | undefined
  if (!registry) return undefined
  return (await registry.resolveByPath?.(canonical)) ?? (await registry.create?.(canonical))
}

/** 把会话挂名到其 cwd 对应的工作区。attachSession 内部强校验 realpath(header.cwd) 精确等于 workspace.path,
 *  所以 canonical 必须是 header.cwd 的 realpath 规范化值。失败告警不阻断任务(分组是锦上添花)。 */
async function attachToWorkspace(ctx: Context, canonical: string, sessionId: SessionId): Promise<void> {
  try {
    const ws = await ensureWorkspace(ctx, canonical)
    if (ws?.attachSession) await ws.attachSession(sessionId)
  } catch (e) {
    console.warn('[harness-mcp-server] workspace attach failed:', (e as Error)?.message ?? e)
  }
}

/** 按会话 header 的 cwd(realpath 规范化后)补挂工作区; header 无 cwd 时静默跳过 */
async function attachSessionCwd(ctx: Context, sessionId: SessionId, cwd: string | undefined): Promise<void> {
  if (cwd === undefined) return
  await attachToWorkspace(ctx, await canonicalCwd(cwd), sessionId)
}

/** 常驻 agent 会话(按 cwd 复用, 省 token: 避免每次全量加载项目上下文) */
const liveAgents = new Map<string, { sessionId: SessionId; handle: AgentHandle }>()

/** sessionId → cwd 索引(支持按 session 续接: 指定 sessionId 时定位到对应 cwd 的常驻会话) */
const sessionToCwd = new Map<string, string>()

/** 每个 cwd 的串行执行锁(防同一 agent 会话被并发 followup 冲突) */
const agentLocks = new Map<string, Promise<unknown>>()

/** getAgent 的返回: handle 恒有 .agent; resume 出来的独占句柄带 disposeAfter 标记, 任务结束后应 flush+dispose */
interface ResolvedAgent {
  sessionId: SessionId
  handle: AgentHandle
  /** true = 本插件 resume 出来的独占句柄; false/缺省 = 常驻池会话或 live 接管(生命周期归池/owner) */
  disposeAfter?: boolean
}

/** 获取(或创建)指定 cwd 的常驻 agent 会话; 传 sessionId 时接管指定会话; 传 title 时给新会话命名。
 *  fresh=true 且未传 sessionId 时: 跳过池命中, 先退役该 cwd 的旧池会话(dispose, 持久化保留), 再新建 ——
 *  这是外部客户端显式控制「是否复用会话」的入口(session_send 的 newSession 参数)。
 *  modelOpts 提供按次调用的 provider/model 覆盖(只对新建/resume 的会话生效; 池命中的复用会话保持原模型)。
 *  modeOpts 提供按次的会话模式(agent preset + 沙箱访问模式 + 审批策略):
 *   - 指定模式且未传 sessionId 时恒强制全新会话 —— 池里复用的存量会话无法安全套用新模式(避免后续再提权的前提是
 *     会话从一开始就跑在该模式下; 新建会话应用 sandbox/approval 走官方会话日志事件, 作为持久覆盖)。
 *   - 传 sessionId 时: preset 允许(在 resume 的 setup 里挂载该 preset); sandbox/approval 拒绝(不可改写存量会话历史)。 */
async function getAgent(
  ctx: Context,
  cwd: string,
  sessionId?: string,
  title?: string,
  fresh?: boolean,
  modelOpts?: { provider?: string; model?: string },
  modeOpts?: { preset?: string; sandbox?: string; approval?: string },
): Promise<ResolvedAgent> {
  const modeRequested = modeOpts !== undefined && (modeOpts.preset !== undefined || modeOpts.sandbox !== undefined || modeOpts.approval !== undefined)
  if (modeRequested && sessionId !== undefined && (modeOpts.sandbox !== undefined || modeOpts.approval !== undefined)) {
    // 防御(工具层已前置校验): 存量会话的历史不能改写
    throw new Error('mode/sandbox/approval only apply when creating a new session; pass newSession:true or omit sessionId (preset alone is allowed when resuming)')
  }
  if (modeRequested && sessionId === undefined) fresh = true // 指定模式 = 强制全新会话(池会话无法安全套用新模式)
  // 恒解析生效模型(显式覆盖 → 插件配置 → agentDefaultModel): 预设 persona 引用 {{model}},
  // agent.options.model 缺失会让 prompt 组装抛 "has no value for this assembly" 并空跑本轮。
  // 指定 sessionId 时优先采用 session_set_model 记录的会话级覆盖(resume 后依然生效)。
  const sessionOverride = sessionId !== undefined ? sessionModelOverrides.get(sessionId) : undefined
  const agentOptions = sessionOverride
    ? { provider: sessionOverride.provider ?? modelOpts?.provider ?? runtimeConfig.provider, model: sessionOverride.model }
    : resolveAgentModel(ctx, modelOpts)
  // 指定 sessionId: 接管已有会话(长任务分多轮投喂 / 中断后恢复 / UI 手开的会话)
  if (sessionId) {
    // 先看本进程常驻池(指定 sessionId 时定位到对应 cwd 的常驻会话; 命中 LRU 移到末尾, 保留上游语义)
    const targetCwd = sessionToCwd.get(sessionId)
    if (targetCwd !== undefined) {
      const existing = liveAgents.get(targetCwd)
      if (existing) {
        liveAgents.delete(targetCwd)
        liveAgents.set(targetCwd, existing)
        mcpSessionIds.add(sessionId)
        return existing
      }
    }
    const sid = SessionId(sessionId)
    // 不在常驻池: 看 live(UI 手开的、别的插件持有的会话), 直接接管、不持有 dispose(归其 owner)
    const live = ctx.agents.get(sid)
    if (live) {
      // live 会话也补挂工作区(幂等): 用户手开的会话若尚未归组, 这里一并挂名
      await attachSessionCwd(ctx, sid, live.session.header.cwd)
      mcpSessionIds.add(sessionId) // 被 MCP 接管即视为 MCP 驱动(审批转达调用方)
      // no-op dispose 兜底: 只有 disposeAfter 为 true 的句柄才由调用方(投喂路径)负责释放
      return { sessionId: sid, handle: { agent: live, dispose: () => Promise.resolve() }, disposeAfter: false }
    }
    // live 也没有: 从持久化会话存储 resume 并接管(进程重启前的会话、LRU 淘汰后被释放的会话)
    const resumePreset = modeOpts?.preset ?? runtimeConfig.preset
    let handle: AgentHandle
    try {
      handle = await ctx.agents.resume({
        resumeSessionId: sid,
        agentOptions,
        setup: async (agentCtx) => {
          // dsh 0.1.1-rc.2 起已修复 rc.6 的 agent ctx 丢 scope 问题(agent-loop 会 createScope);
          // 保留检测以兼容更旧版本: 无 scope 时跳过挂载(降级为无工具 agent), 不让 resume 整体崩溃。
          if (scopeOf(agentCtx) === undefined) {
            console.warn('[harness-mcp-server] agent ctx unscoped (old dsh bug); preset mount skipped — upgrade dsh >= 0.1.1-rc.2 for full tool support')
            return
          }
          await ctx.agentPresets.mount(agentCtx, resumePreset)
        },
      })
    } catch (e) {
      // 恢复失败返回明确错误(沿用上游错误风格): 不在常驻池、不是 live、持久化里也没有(或 resume 失败)
      throw new Error(`session not found for resume: ${sessionId} (not live and not persisted; ${(e as Error)?.message ?? e})`)
    }
    await attachSessionCwd(ctx, sid, handle.agent.session.header.cwd)
    mcpSessionIds.add(sessionId)
    sessionPresetApplied.set(sessionId, resumePreset)
    return { sessionId: sid, handle, disposeAfter: true }
  }
  // 显式全新会话: 跳过池命中 —— 先退役该 cwd 的旧池会话(保留持久化, 可凭 sessionId 续接)。
  // 旧会话正在跑任务时不 dispose(不掐任务), 仅从池摘除; 其任务结束后 agent 仍 live, 可凭 sessionId 接管(等其空闲后由 LRU 回收)。
  if (fresh) {
    const old = liveAgents.get(cwd)
    if (old) {
      liveAgents.delete(cwd)
      sessionToCwd.delete(String(old.sessionId))
      const status = (old.handle.agent as unknown as { status?: string }).status
      if (status === 'idle') {
        try { await old.handle.dispose() } catch { /* 退役失败不阻断新建 */ }
      }
    }
    return createPoolAgent(ctx, cwd, title, agentOptions, modeOpts)
  }
  const existing = liveAgents.get(cwd)
  if (existing) {
    // LRU: 命中则移到末尾(最近使用)
    liveAgents.delete(cwd)
    liveAgents.set(cwd, existing)
    // 自愈: 幂等补挂(已在花名册则 no-op; 首次挂名失败的池会话在此被捞回)
    await attachToWorkspace(ctx, await canonicalCwd(cwd), existing.sessionId)
    return existing
  }
  return createPoolAgent(ctx, cwd, title, agentOptions, modeOpts)
}

/** 新建一个 cwd 的常驻池会话: LRU 淘汰(只淘汰 idle 的) → agents.create(挂 preset) → 入池 → 工作区分组 → 可选命名。
 *  modeOpts 提供按次的会话模式: preset 写进 meta.agentPreset(官方创建事实)并在 setup 挂载;
 *  sandbox/approval 以官方会话日志事件(sandbox/mode / approval/policy)落为持久覆盖 —— 会话自创建起就跑在该模式下。 */
async function createPoolAgent(ctx: Context, cwd: string, title?: string, agentOptions?: { provider?: string; model?: string }, modeOpts?: { preset?: string; sandbox?: string; approval?: string }): Promise<ResolvedAgent> {
  // LRU 淘汰: 超过上限时逐出最久未用的会话 —— 只淘汰 idle 的(agent.status === 'idle');
  // 最旧一批都在忙时**不掐任务**, 允许池暂时超上限(软上限), 等任务落定后由下次淘汰回收。
  while (liveAgents.size >= runtimeConfig.maxAgents) {
    let victimKey: string | undefined
    for (const [key, rec] of liveAgents) {
      const status = (rec.handle.agent as unknown as { status?: string }).status
      if (status === 'idle') { victimKey = key; break }
    }
    if (victimKey === undefined) break
    const old = liveAgents.get(victimKey)
    liveAgents.delete(victimKey)
    if (old) {
      sessionToCwd.delete(String(old.sessionId))
      try { await old.handle.dispose() } catch { /* 忽略 */ }
    }
  }
  const newSessionId = SessionId(randomUUID())
  // cwd 先 realpath 规范化: session header 的 cwd 与 workspace.path 必须精确相等,
  // 否则 attachSession 强校验 reject(只会 create 注册而 UI 仍落未分组)
  const canonical = await canonicalCwd(cwd)
  const presetId = modeOpts?.preset ?? runtimeConfig.preset
  const handle = await ctx.agents.create({
    sessionId: newSessionId,
    // meta.agentPreset 自 dsh 0.1.1-rc.2 起是官方字段(session header 记录/预置选择器消费);
    // 但 preset 仍需在 setup 里显式 mount —— agentPresets 不做自动挂载, 只对未挂载 agent 告警。
    meta: { cwd: canonical, agentPreset: presetId },
    agentOptions,
    setup: async (agentCtx) => {
      // 关键: 通过 setup 挂载 preset(含 bash/fs/todo/web 等完整工具)。
      // rc.6 的 agent-loop 曾把 setup 收到的 agent ctx 弄丢 scope tag(挂载会抛
      // 'refusing to compose an unscoped context'); 0.1.1-rc.2 已修复。
      // 这里保留检测以兼容更旧版本: 无 scope 时跳过挂载(降级为无工具 agent), 避免 session_send 整体失败。
      if (scopeOf(agentCtx) === undefined) {
        console.warn('[harness-mcp-server] agent ctx unscoped (old dsh bug); preset mount skipped — upgrade dsh >= 0.1.1-rc.2 for full tool support')
        return
      }
      await ctx.agentPresets.mount(agentCtx, presetId)
    },
  })
  sessionPresetApplied.set(String(newSessionId), presetId)
  // sandbox/approval 应用: 以官方会话日志事件落为持久覆盖(与 dsh-sandbox-policy / dsh-user-approval 的
  // setSandboxMode / setApprovalPolicy 同一表示 —— 事件即状态, 回放即恢复)。会话自创建起就跑在该模式下,
  // 后续 bash/fs 等受限调用按此模式执行, 避免任务中途再提权。
  try {
    const sess = handle.agent.session as { append?: (type: string, data: unknown) => unknown }
    if (modeOpts?.sandbox !== undefined && sess.append) sess.append('sandbox/mode', { mode: modeOpts.sandbox })
    if (modeOpts?.approval !== undefined && sess.append) sess.append('approval/policy', { policy: modeOpts.approval })
  } catch (e) {
    console.warn('[harness-mcp-server] mode application to new session failed:', String(e))
  }
  const rec = { sessionId: newSessionId, handle }
  liveAgents.set(cwd, rec)
  sessionToCwd.set(String(newSessionId), cwd)
  mcpSessionIds.add(String(newSessionId))

  // 分组: 把会话归属到 cwd 对应的工作区(resolveByPath ?? create + attachSession; 可选依赖; headless 环境自动跳过)
  void (async () => {
    try {
      const ws = await ensureWorkspace(ctx, canonical)
      if (ws?.attachSession) await ws.attachSession(newSessionId)
    } catch (e) {
      console.warn('[harness-mcp-server] workspace attach failed:', String(e))
    }
  })()

  // title 命名(可选): 创建会话后立即命名(走 sessionTitle 服务的 rename; 显式 title 或
  // 由任务内容自动派生的名称都走同一条路径, 使新会话开箱即有名字, session_list 可见)
  if (title) {
    renameSessionSafe(ctx, handle.agent.session, title)
  }

  return rec
}

/** 同一 cwd 串行执行, 避免并发 followup 同一会话 */
async function withLock<T>(cwd: string, fn: () => Promise<T>): Promise<T> {
  const prev = agentLocks.get(cwd) ?? Promise.resolve()
  const next = prev.then(fn, fn)
  agentLocks.set(cwd, next.catch(() => {}))
  return next
}

/** 超时哨兵: 区分维护操作(如 session_compact)的「到点中止」与真实异常 */
const TASK_TIMEOUT = Symbol('task-timeout')

/** 从 agent 最终回答里解析 changes/verification/leftovers(从后往前找候选, 更可靠) */
function parseSummary(assistantText: string): { changes: string; verification: string; leftovers: string } {
  const empty = { changes: '', verification: '', leftovers: '' }
  // 收集所有 {...} 候选(agent 被要求输出一行 summary JSON)
  const candidates: string[] = []
  const re = /\{[\s\S]*?\}/g
  let m: RegExpExecArray | null
  while ((m = re.exec(assistantText)) !== null) {
    candidates.push(m[0])
  }
  // 从后往前: 最后出现的候选最可能是最终 summary, 逐个尝试解析
  for (let i = candidates.length - 1; i >= 0; i--) {
    try {
      const obj = JSON.parse(candidates[i] as string) as Record<string, unknown>
      const s = (v: unknown) => (typeof v === 'string' ? v : '')
      const changes = s(obj.changes) || s(obj.改动)
      const verification = s(obj.verification) || s(obj.验证)
      const leftovers = s(obj.leftovers) || s(obj.遗留) || s(obj.leftover)
      // 只要含任一 summary 字段就采纳, 否则继续尝试更早的候选
      if (changes || verification || leftovers) {
        return { changes, verification, leftovers }
      }
    } catch {
      // 非合法 JSON, 继续尝试下一个候选
    }
  }
  return empty
}

/** ctx.tokenMeter 的只读视图(可选服务; 未加载时返回 null) */
interface TokenMeterView {
  measure?: (session: unknown) => {
    logRevision?: number
    surfaceTokens?: number
    totalTokens?: number
  }
}

/** (provider:model) → 上下文窗口 token 数缓存; 解析失败缓存 null(不反复查询) */
const modelWindowCache = new Map<string, number | null>()

/** 经 ctx.llm.resolveModelInfo 解析某 provider/model 的上下文窗口; 不可解析返回 null。
 *  注意 dsh-llm 的 LlmRuntime 服务只暴露 resolveModelInfo(适配器层的 resolveModel 是 LlmAdapter 方法,
 *  服务上不存在) —— 之前的 resolveModel 调用恒 undefined, 导致 window/ratio 恒 null。 */
async function modelWindowOf(ctx: Context, provider: string | undefined, model: string | undefined): Promise<number | null> {
  if (!provider || !model) return null
  const key = `${provider}:${model}`
  const cached = modelWindowCache.get(key)
  if (cached !== undefined) return cached
  let window: number | null = null
  try {
    const llm = ctx.get('llm') as { resolveModelInfo?: (p: string, m: string, s?: AbortSignal) => Promise<{ context?: { contextWindow?: number } }> } | undefined
    const info = await llm?.resolveModelInfo?.(provider, model)
    window = info?.context?.contextWindow ?? null
  } catch {
    window = null
  }
  modelWindowCache.set(key, window)
  return window
}

/** 会话生效的 provider/model: agent.options 优先, 其次 agentDefaultModel 默认选择, 否则插件配置 */
function agentModelOf(ctx: Context, agent: { options?: { provider?: string; model?: string } } | undefined): { provider?: string; model?: string } {
  if (agent?.options?.model) return { provider: agent.options.provider, model: agent.options.model }
  const def = (ctx.get('agentDefaultModel') as { currentSelection?: () => { provider?: string; model?: string } } | undefined)?.currentSelection?.()
  if (def?.model) return def
  return { provider: runtimeConfig.provider, model: runtimeConfig.model || undefined }
}

/** 会话上下文占用快照(session_list / session_status 通用形状) */
interface ContextUsage {
  events: number
  tokens: number
  pressure: number
  window: number | null
  ratio: number | null
}

/** 完整上下文占用: 事件数 + 表面 token 数 + 最近请求压力 + 模型窗口 + 占用比(百分比, 1 位小数);
 *  tokenMeter 缺失返回 null; 窗口不可解析时 window/ratio 为 null。 */
async function contextUsage(
  ctx: Context,
  session: unknown,
  agent?: { options?: { provider?: string; model?: string } },
): Promise<ContextUsage | null> {
  try {
    const m = (ctx.get('tokenMeter') as TokenMeterView | undefined)?.measure?.(session)
    if (!m) return null
    const events = m.logRevision ?? 0
    const tokens = m.surfaceTokens ?? 0
    const pressure = m.totalTokens ?? 0
    const { provider, model } = agentModelOf(ctx, agent)
    const window = await modelWindowOf(ctx, provider, model)
    const ratio = window && window > 0 ? Math.round((tokens / window) * 1000) / 10 : null
    return { events, tokens, pressure, window, ratio }
  } catch {
    return null
  }
}

/** 按 sessionId 找 live agent(池优先, 其次 ctx.agents; 都不是返回 undefined) */
function liveAgentFor(ctx: Context, sessionId: string | undefined): { session: unknown; options?: { provider?: string; model?: string } } | undefined {
  if (!sessionId) return undefined
  const cwd = sessionToCwd.get(sessionId)
  const pooled = cwd !== undefined ? liveAgents.get(cwd) : undefined
  if (pooled) return pooled.handle.agent
  return ctx.agents.get(SessionId(sessionId))
}

/** 等待输入的弹窗(审批/提问)一行: session_status.prompts 与 pending_prompts 共用的形状 */
interface PromptRow {
  type: 'approval' | 'question'
  id: string
  toolName?: string
  reason?: string
  questions?: unknown
  note?: string
}

/** 收集某会话当前挂起的弹窗: 审批(本插件应答链挂起) + 提问(本插件接管 / 本插件未接管时 GUI 应答链
 *  挂起的 ask_user_question)。session 缺省(非 live 冷读路径)时只能看本插件的挂起表。 */
function promptsFor(sessionId: string, session: unknown | undefined): PromptRow[] {
  const prompts: PromptRow[] = []
  for (const pa of pendingApprovals.values()) {
    if (pa.agentId === sessionId) {
      prompts.push({ type: 'approval', id: pa.promptId, toolName: pa.toolName, ...(pa.reason !== undefined ? { reason: pa.reason } : {}) })
    }
  }
  for (const pq of pendingQuestions.values()) {
    if (pq.agentId === sessionId) prompts.push({ type: 'question', id: pq.promptId, questions: pq.questions })
  }
  if (!questionsProviderOurs && session !== undefined) {
    const detected = detectPendingAskUser(session)
    if (detected) {
      prompts.push({ type: 'question', id: detected.id, questions: detected.questions, note: 'not claimed by MCP (MCP only claims questions asked while it drives the session); answer it in the DSH web UI' })
    }
  }
  return prompts
}

/** 从任意事件/消息对象递归收集文本(容错遍历; 供结果提取与 session_read 复用) */
function extractText(obj: unknown, out: string[]): void {
  if (Array.isArray(obj)) { obj.forEach((x) => extractText(x, out)); return }
  if (obj && typeof obj === 'object') {
    const rec = obj as Record<string, unknown>
    if (typeof rec.text === 'string' && rec.text.trim()) out.push(rec.text)
    if (typeof rec.content === 'string' && rec.content.trim()) out.push(rec.content)
    for (const v of Object.values(rec)) extractText(v, out)
  }
}

/** cwd 白名单校验: 配置了 workspaceRoots 时只允许在列出的目录下干活(防路径穿越); 未配置恒放行 */
function cwdAllowed(workdir: string): boolean {
  if (runtimeConfig.workspaceRoots.length === 0) return true
  return runtimeConfig.workspaceRoots.some((root) => {
    const r = resolve(root)
    return workdir === r || workdir.startsWith(r + '/')
  })
}

/** 解析 agent 生效的 provider/model: 显式覆盖 → 插件配置 → agentDefaultModel 默认选择。
 *  必须恒有 model: 预设 persona 模板(如 standard 的 "powered by the {{model}} model")引用 {{model}} 变量,
 *  该变量取自 agent.options.model —— 缺失时 prompt 组装抛
 *  `prompt variable "{{model}}" has no value for this assembly (section "deployment:persona")`, 本轮空跑。 */
function resolveAgentModel(ctx: Context, modelOpts?: { provider?: string; model?: string }): { provider: string; model?: string } {
  const explicit = modelOpts?.model ?? runtimeConfig.model
  if (explicit) return { provider: modelOpts?.provider ?? runtimeConfig.provider, model: explicit }
  const def = (ctx.get('agentDefaultModel') as { currentSelection?: () => { provider?: string; model?: string } } | undefined)?.currentSelection?.()
  const provider = modelOpts?.provider ?? def?.provider ?? runtimeConfig.provider
  const model = def?.model
  if (!model) {
    console.warn('[harness-mcp-server] no model resolved (agentDefaultModel service missing?); persona {{model}} may fail to assemble')
  }
  return { provider, model }
}

/** 待响应的提问 prompt(仅当本插件持有提问应答权 questionsProviderOurs 时产生; 否则提问走 web GUI 应答链) */
interface PendingQuestion {
  promptId: string
  agentId: string
  questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }>
  resolve: (answer: { answers: Array<{ id: string; selected: string[]; custom?: string }> }) => void
  reject: (e: Error) => void
}

/** MCP 驱动的会话 id 集(创建/接管即标记): 标识「该会话由 MCP 创建或接管过」(历史事实, 不随任务结束消失) */
const mcpSessionIds = new Set<string>()
/** sessionId → 尚未落定的 MCP turn 数(mcpBusySessionIds 的等价物): 「该会话当前是否有 MCP 在飞 turn」的
 *  精确判据。session_send 投喂时 +1, 该会话每个 turn/end 事件 -1(减到 0 即摘除计数与观测器)。
 *  审批应答者必须同时满足「在 mcpSessionIds 且计数 > 0」才接管: 单看 mcpSessionIds 只能证明历史接管过,
 *  用户经 web UI 直接向该会话发消息触发的审批也会被误接管, 而 Hermes 并无任务在等 → 两端都收不到, 死锁。 */
const mcpPendingTurns = new Map<string, number>()
/** sessionId → turn/end 观测器的 disposer(每个有在飞 turn 的会话只挂一个, 计数归零即摘除) */
const mcpTurnWatchers = new Map<string, () => void>()

/** 该会话是否有 MCP 在飞 turn(审批/提问接管判据的第二半) */
function mcpTurnInFlight(sessionId: string): boolean {
  return (mcpPendingTurns.get(sessionId) ?? 0) > 0
}

/** 投喂后登记一个 MCP 在飞 turn(+1); 首次登记时挂一个「该会话下一次 turn/end 就 -1」的观测器 */
function markMcpTurn(ctx: Context, sessionId: string): void {
  mcpPendingTurns.set(sessionId, (mcpPendingTurns.get(sessionId) ?? 0) + 1)
  if (mcpTurnWatchers.has(sessionId)) return
  const off = onSessionEvent(ctx, sessionId, (event) => {
    if ((event as { type?: string })?.type !== 'turn/end') return
    const left = (mcpPendingTurns.get(sessionId) ?? 0) - 1
    if (left > 0) { mcpPendingTurns.set(sessionId, left); return }
    mcpPendingTurns.delete(sessionId)
    mcpTurnWatchers.delete(sessionId)
    off?.()
  })
  if (off === undefined) {
    // 观测器挂不上(宿主事件不可用): 退化为「投喂即认为有一个在飞 turn」, 由下一次 session_send 覆盖
    return
  }
  mcpTurnWatchers.set(sessionId, off)
}

/** 撤销一个 MCP 在飞 turn(投喂失败/主动取消时用; 减到 0 即摘除) */
function unmarkMcpTurn(sessionId: string): void {
  const left = (mcpPendingTurns.get(sessionId) ?? 0) - 1
  if (left > 0) { mcpPendingTurns.set(sessionId, left); return }
  mcpPendingTurns.delete(sessionId)
  const off = mcpTurnWatchers.get(sessionId)
  if (off !== undefined) { mcpTurnWatchers.delete(sessionId); try { off() } catch { /* ignore */ } }
}
/** 审批决策结果(DSH 词汇表的调用方可控子集; 'unavailable' 仅由 fail-closed 产生) */
type ApprovalOutcomeValue = 'allowed-once' | 'rejected' | 'cancelled'
/** ctx.approval 'approval/request' 请求的只读视图(鸭子类型, 避免引入 dsh-user-approval 依赖) */
interface ApprovalRequestView {
  agent: { id: unknown; session: unknown }
  toolName: string
  callId?: string
  reason?: string
  signal?: AbortSignal
}
/** 待响应的审批 prompt(promptId = 审计事件 approval/asked 的 id) */
const pendingApprovals = new Map<string, {
  promptId: string
  agentId: string
  toolName: string
  reason?: string
  resolve: (outcome: ApprovalOutcomeValue) => void
}>()
/** 待响应的提问 prompt */
const pendingQuestions = new Map<string, PendingQuestion>()
/** 本插件是否持有提问应答权(true = 提问由 MCP 接管, prompt_respond 可答; false = 交给 web GUI 应答链)。
 *  dsh-user-questions 两版宿主各有取得方式(旧版 registerProvider / 新版 waterfall listener), 详见 apply()。 */
let questionsProviderOurs = false
/** 会话级模型覆盖(sessionId → {provider?, model}): session_set_model 记录, resume 时同样生效 */
const sessionModelOverrides = new Map<string, { provider?: string; model: string }>()

// ── 会话「模式」应用/读取: preset(agentPresets) + sandbox/approval(会话日志事件) ──

/** 应用到会话的 agent preset 登记(sessionId → preset id): 创建/接管时记录, 供结果/会话列表回读生效 preset */
const sessionPresetApplied = new Map<string, string>()

/** ctx.agentPresets 的只读视图(鸭子类型, 避免硬依赖 dsh-agent-presets 内部类型) */
interface AgentPresetsView {
  list?: () => Promise<Array<{ id: string; name?: string; description?: string; trust?: string; order?: number; path?: string; broken?: string }>>
  resolve?: (id: string) => Promise<{ id: string; name?: string; description?: string; trust?: string; order?: number; path?: string; broken?: string } | undefined>
  defaultId?: string | (() => string)
}
/** ctx.permissionPresets 的只读视图(鸭子类型; dsh-permission-presets 服务可选) */
interface PermissionPresetsView {
  names?: readonly string[]
  resolve?: (name: string) => { sandbox?: string; approval?: string; name?: string; description?: string } | undefined
  defaultPreset?: string
}
/** ctx.sandboxPolicy / ctx.approval 的只读视图(鸭子类型; 服务可选) */
interface SandboxPolicyView {
  defaultMode?: string
  workspaceRoot?: string
}
interface ApprovalServiceView {
  config?: { policy?: string }
}

/** 从会话事件流折出生效沙箱模式(最后一条 sandbox/mode 事件; 无覆盖回退部署默认) */
function effectiveSandboxModeOf(session: unknown, fallback: string): string {
  const events = (session as { events?: Array<{ type?: string; data?: { mode?: string } }> }).events ?? []
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]
    if (e?.type === 'sandbox/mode' && e.data?.mode) return e.data.mode
  }
  return fallback
}

/** 从会话事件流折出生效审批策略(最后一条 approval/policy 事件; 无覆盖回退部署默认) */
function effectiveApprovalPolicyOf(session: unknown, fallback: string): string {
  const events = (session as { events?: Array<{ type?: string; data?: { policy?: string } }> }).events ?? []
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]
    if (e?.type === 'approval/policy' && e.data?.policy) return e.data.policy
  }
  return fallback
}

/** 会话生效的 preset: 本插件创建/接管时登记优先, 其次 session header 的 agentPreset(创建事实), 否则插件配置默认 */
function effectivePresetOf(sessionId: string, header?: { agentPreset?: string }): string {
  return sessionPresetApplied.get(sessionId) ?? header?.agentPreset ?? runtimeConfig.preset
}

/** 按 (sandbox, approval) 对匹配已命名的权限预设(permissionPresets 服务不可用时返回 undefined) */
function permissionPresetNameOf(ctx: Context, sandbox: string, approval: string): string | undefined {
  const pp = ctx.get('permissionPresets') as PermissionPresetsView | undefined
  if (!pp?.resolve) return undefined
  for (const name of pp.names ?? []) {
    const spec = pp.resolve(name)
    if (spec && spec.sandbox === sandbox && spec.approval === approval) return name
  }
  return undefined
}

/** 部署默认模式(服务缺省时给安全回退): {preset, sandbox, approval, permissionPreset} */
function deploymentModeDefaults(ctx: Context): { preset: string; sandbox: string; approval: string; permissionPreset?: string } {
  const ap = ctx.get('agentPresets') as AgentPresetsView | undefined
  const preset = typeof ap?.defaultId === 'function' ? ap.defaultId() : (ap?.defaultId ?? runtimeConfig.preset)
  const sandbox = (ctx.get('sandboxPolicy') as SandboxPolicyView | undefined)?.defaultMode ?? 'read-only'
  const approval = (ctx.get('approval') as ApprovalServiceView | undefined)?.config?.policy ?? 'ask'
  const permissionPreset = permissionPresetNameOf(ctx, sandbox, approval)
  return { preset, sandbox, approval, ...(permissionPreset !== undefined ? { permissionPreset } : {}) }
}

/** 会话当前的生效模式快照(结果回读/会话列表共用): preset + 折出的 sandbox/approval + 匹配的权限预设名 */
function sessionModeOf(ctx: Context, sessionId: string, session: unknown, header?: { agentPreset?: string }): {
  preset: string
  sandbox: string
  approval: string
  permissionPreset?: string
} {
  const defaults = deploymentModeDefaults(ctx)
  const sandbox = effectiveSandboxModeOf(session, defaults.sandbox)
  const approval = effectiveApprovalPolicyOf(session, defaults.approval)
  const permissionPreset = permissionPresetNameOf(ctx, sandbox, approval)
  return {
    preset: effectivePresetOf(sessionId, header),
    sandbox,
    approval,
    ...(permissionPreset !== undefined ? { permissionPreset } : {}),
  }
}

/** 把调用方传入的 mode/preset/sandbox/approval 解析成规范模式(校验 + 消歧), 失败抛错。
 *  mode 的解析顺序: 权限预设名(捆绑 sandbox+approval) → 沙箱模式 → 审批策略 → agent preset id。
 *  显式 sandbox/approval 覆盖 mode 捆绑里的对应值。 */
async function resolveModeRequest(ctx: Context, input: { mode?: string; preset?: string; sandbox?: string; approval?: string }): Promise<{
  preset?: string
  sandbox?: string
  approval?: string
}> {
  const out: { preset?: string; sandbox?: string; approval?: string } = {}
  const agentPresets = ctx.get('agentPresets') as AgentPresetsView | undefined
  const permissionPresets = ctx.get('permissionPresets') as PermissionPresetsView | undefined

  // mode: 命名模式消歧(权限预设名优先 —— 'workspace-write' 同时是沙箱模式与权限预设名, 捆绑更具体)
  if (input.mode !== undefined) {
    const m = input.mode
    let matched = false
    if (permissionPresets?.resolve) {
      try {
        const spec = permissionPresets.resolve(m)
        if (spec) {
          out.sandbox = spec.sandbox
          out.approval = spec.approval
          matched = true
        }
      } catch { /* 非权限预设名, 继续尝试其它类别 */ }
    }
    if (!matched && (SANDBOX_MODES as readonly string[]).includes(m)) {
      out.sandbox = m
      matched = true
    }
    if (!matched && (APPROVAL_POLICIES as readonly string[]).includes(m)) {
      out.approval = m
      matched = true
    }
    if (!matched && agentPresets?.resolve) {
      try {
        await agentPresets.resolve(m)
        out.preset = m
        matched = true
      } catch { /* 非 preset id */ }
    }
    if (!matched) {
      const available = [
        ...(permissionPresets?.names ?? []),
        ...SANDBOX_MODES,
        ...APPROVAL_POLICIES,
        ...(await listPresetIds(ctx)),
      ]
      throw new Error(`unknown mode: ${m} (available: ${[...new Set(available)].join(', ') || 'none'})`)
    }
  }

  if (input.preset !== undefined) {
    // 校验 preset id(agentPresets.resolve 可用时); 未知抛错并附可用清单
    if (agentPresets?.resolve) {
      try {
        await agentPresets.resolve(input.preset)
      } catch (e) {
        const avail = (e as { available?: readonly string[] }).available
        const suffix = avail !== undefined && avail.length > 0 ? ` (available: ${avail.join(', ')})` : ''
        throw new Error(`unknown preset: ${input.preset}${suffix}`)
      }
    }
    out.preset = input.preset
  }

  if (input.sandbox !== undefined) {
    if (!(SANDBOX_MODES as readonly string[]).includes(input.sandbox)) {
      throw new Error(`invalid sandbox mode: ${input.sandbox} (must be one of ${SANDBOX_MODES.join(', ')})`)
    }
    out.sandbox = input.sandbox
  }
  if (input.approval !== undefined) {
    if (!(APPROVAL_POLICIES as readonly string[]).includes(input.approval)) {
      throw new Error(`invalid approval policy: ${input.approval} (must be one of ${APPROVAL_POLICIES.join(', ')})`)
    }
    out.approval = input.approval
  }
  return out
}

/** agentPresets.list 的 id 清单(服务/方法缺失时返回空; 供报错提示与 mode_list 汇总) */
async function listPresetIds(ctx: Context): Promise<string[]> {
  const agentPresets = ctx.get('agentPresets') as AgentPresetsView | undefined
  try {
    return (await agentPresets?.list?.())?.map((p) => p.id) ?? []
  } catch {
    return []
  }
}

/** 从审批请求的会话事件里取审计 id(倒查最近一条匹配 callId 的 approval/asked, 与 web GUI 应答者同款); 找不到时合成兜底 id */
function approvalPromptIdOf(req: { agent: { session: unknown }; toolName: string; callId?: string }): string {
  const events = ((req.agent.session as unknown as { events?: Array<{ type?: string; data?: { id?: string; callId?: string } }> }).events ?? [])
  const decided = new Set<string>()
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]
    if (e?.type === 'approval/decided') decided.add(e.data?.id as string)
    else if (e?.type === 'approval/asked') {
      if (decided.has(e.data?.id as string)) continue
      if ((req.callId ?? null) !== (e.data?.callId ?? null)) continue
      if (e.data?.id) return String(e.data.id)
    }
  }
  return `approval-${req.toolName}-${Date.now()}`
}

/** 检测挂起的 ask_user_question 工具调用(本插件未接管该提问时, 这是感知它的唯一途径):
 *  倒查最后一条 ask_user_question 的 tool/call, 其后没有 tool/result 即为挂起。 */
function detectPendingAskUser(session: unknown): { id: string; questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }> } | undefined {
  const log = (session as { log?: unknown[] }).log ?? []
  let callIdx = -1
  for (let i = log.length - 1; i >= 0; i--) {
    const e = log[i] as { type?: string; data?: { name?: string } }
    if (e.type === 'tool/call' && e.data?.name === 'ask_user_question') { callIdx = i; break }
  }
  if (callIdx < 0) return undefined
  for (let i = callIdx + 1; i < log.length; i++) {
    if ((log[i] as { type?: string }).type === 'tool/result') return undefined
  }
  const args = (log[callIdx] as { data?: { arguments?: string } }).data?.arguments
  let questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }> = []
  try {
    const parsed = JSON.parse(args ?? '{}') as { questions?: Array<{ id?: string; question?: string; detail?: string; options?: { label?: string }[] }> }
    questions = (parsed.questions ?? []).map((q) => ({
      id: String(q.id ?? ''),
      question: String(q.question ?? ''),
      ...(q.detail !== undefined ? { detail: q.detail } : {}),
      ...(q.options !== undefined ? { options: (q.options ?? []).map((o) => ({ label: o.label ?? '' })) } : {}),
    }))
  } catch { /* 参数不可解析时仅报挂起, 不带原文 */ }
  return { id: `ask-${callIdx}`, questions }
}

/** 待安全落点的 notice(审批/提问被 MCP 接管/响应时产生), 由 tools/post-execute 在工具完成后统一投递 */
interface PendingNotice {
  text: string
  summary: string
}

/**
 * 按 agent id 挂起的 notice 队列: 响应类提示(✅/❌)只入队, 绝不直接写会话日志。
 * 修复回归: 旧版 appendPromptNotice 在 approval/request 拦截期直接 append user/message,
 * 若时机落在 assistant 带 tool_calls 的消息与其 tool/result 之间, 会打断消息序列,
 * 使下个模型请求报 'An assistant message with tool_calls must be followed by tool
 * messages responding to each tool_call_id'(INVALID_REQUEST), 会话失效。
 * (拦截类 ⏳ 提示现已走 notifyPromptIntercepted 的挂起期即时投递, 不再经过本队列;
 * 本队列仍承接 ✅/❌ 响应提示, 并作为 ⏳ 即时投递失败时的兜底。)
 */
const pendingNotices = new Map<string, PendingNotice[]>()

/** 从 agent 鸭子类型取稳定 id(与 mcpSessionIds / 审批应答者同款身份解析) */
function agentIdOf(agent: unknown): string | undefined {
  const a = agent as { id?: unknown; session?: { id?: unknown } } | undefined
  if (a === undefined) return undefined
  const id = a.id ?? a.session?.id
  return id === undefined ? undefined : String(id)
}

/**
 * 构造一条 form:'notice' 的 plugin 来源 user/message(web UI 折叠提示行专属呈现, 与官方插件同款)。
 *
 * 呈现契约(已对照 DSH web 前端 0.1.1-rc.2 源码 + 实际运行 GUI 的 client 包确认):
 *  - dsh-agent-loop 把 additionalContexts 里的消息原样 append 为 user/message(source 含 form/summary),
 *    即 form:'notice' 在 additionalContexts 路径上**会被保留**——因此无需 exec.deferContext 等替代方案;
 *  - dsh-client-runtime 的 contextForm(source) 读 source.form, KNOWN_FORMS 含 'notice'
 *    (dsh-client-ui-conversation 的 contextBody 也实现 case 'notice' → NoticeBody + 折叠行 summary),
 *    所以 notice 走的是 DSH 原生 notice 专属呈现, 与 dsh-repeat-tool-reminder / dsh-tool-goal 完全同款;
 *  - 折叠行标题「上下文注入」(message.contextInjection) 是 UI 对所有非 recall 上下文行的固定命名,
 *    插件侧无法改写; 因此这里的文案按「系统/状态提示」撰写(带 ⏳/✅ 与明确的
 *    「审批/提问已由 MCP 接管/响应」措辞), 让折叠行与展开体读起来是 notice/系统提示而非底层调用。
 */
function noticeUserMessage(text: string, summary: string) {
  return createUserMessage({
    content: [{ type: 'text', text }],
    source: {
      kind: 'plugin',
      plugin: 'harness-mcp-server',
      form: 'notice' as const,
      summary: boundContextSummary(summary),
    },
  })
}

/** 【2】web UI 提示(安全落点版): 审批/提问被 MCP 拦截/响应时只把提示入队, 不写会话日志。
 *  工具完成后由 tools/post-execute 监听器把这些 notice 并入该工具结果的 additionalContexts,
 *  交给 agent-loop 在 tool/result 之后、下个模型请求之前追加(官方 dsh-repeat-tool-reminder /
 *  dsh-tool-goal 同款机制)—— 从不在 assistant(tool_calls) 与其 tool/result 之间插入
 *  user/message, 因此不破坏模型消息序列。 */
function queuePromptNotice(agent: unknown, text: string, summary: string): void {
  const agentId = agentIdOf(agent)
  if (agentId === undefined) {
    console.warn('[harness-mcp-server] prompt notice skipped (agent has no id):', summary)
    return
  }
  const list = pendingNotices.get(agentId) ?? []
  list.push({ text, summary })
  pendingNotices.set(agentId, list)
}

/** tools/post-execute 安全投递: 该 agent 有挂起 notice 时, 把它们并入 downstream decision 的
 *  additionalContexts(不改动 decision 本身); 没有则原样放行。 */
function flushPromptNotices(agent: unknown, downstream: { kind?: string; additionalContexts?: unknown[] }): { kind?: string; additionalContexts?: unknown[] } {
  const agentId = agentIdOf(agent)
  if (agentId === undefined) return downstream
  const notices = pendingNotices.get(agentId)
  if (notices === undefined || notices.length === 0) return downstream
  pendingNotices.delete(agentId)
  const contexts = notices.map((n) => noticeUserMessage(n.text, n.summary))
  return { ...downstream, additionalContexts: [...(downstream.additionalContexts ?? []), ...contexts] }
}

/**
 * 【3】挂起期即时投递: 审批/提问被 MCP 拦截的 ⏳ 提示, 拦截当下立即追加到该 agent 的
 * next-step inbox, 让 web UI 在用户响应(prompt_respond)之前就能看到, 不再等响应后才随
 * tools/post-execute flush 落地。
 *
 * 落点选型(对照 DSH 0.1.1-rc.2 核心源码逐一实证; 两份候选方案均被否决, 理由如下):
 *  - 方案A-2(拦截期直接 session.append user/message)被否决 —— 拦截时机恒处于
 *    「assistant(tool_calls) 已落日志、其 tool/result 未回」窗口: dsh-agent-loop 的 startCall
 *    先 appendToolCall 再 prepare/dispatch, 而 approval/request 在工具执行内触发
 *    (dsh-tools resolveAskDecision → approval.request → approval/request waterfall)。
 *    此窗口内直插 user/message 会进入 surface(deriveMessages 按日志序投影), 下个模型请求即报
 *    INVALID_REQUEST(0.9.4 回归)。因此「窗口判断」在拦截回调里恒为不安全, 直接 append 无一例外。
 *  - 方案A-1(inbox.append('next-step', form:'notice' 插件消息))消息序列安全, 但挂起期不可见:
 *    inbox 只在下一个 step 边界被消费(dsh-agent-loop preStep → inbox.claim), 且宿主
 *    dsh-host-apiproxy 的 queueItems 投影只把 source.kind === 'user' 的 next-step 项标为
 *    placement 'steering', 其余(含 plugin 来源)标为 'context' —— 而 web UI 对 placement
 *    'context' 的队列行没有任何渲染(只渲染 'steering' → PendingSteeringBubble、
 *    'queued' → QueueDock), form:'notice' 的折叠行要等 claim 落日志后才出现,
 *    与现有 flush 路径同时机, 等于白做。
 *  - 实际采用: inbox.append('next-step', source { kind: 'user' }) —— 即 web GUI「steering」
 *    的官方同款形状: 宿主在 agent/inbox/spliced 事件上即时 broadcast session/queue
 *    (placement 'steering'), web UI 当场渲染 PendingSteeringBubble(挂起期立即可见);
 *    用户响应后 agent-loop 在下个 step 边界 claim 该消息, 追加为 user/message —— 落点在
 *    全部 tool/result 之后(与官方 steering 消息同位), 模型消息序列合法; UI 气泡随 durable
 *    user/message 落地而退役为 transcript 内的 steering 行。对模型而言与现有 flush 路径
 *    完全同位同角色(user-role 文本, 下一步边界送达), 不新增模型语义。
 *
 * inbox 不可用/append 抛错时退回 queuePromptNotice(挂起 pendingNotices, 仍由
 * tools/post-execute 统一 flush), 原有兜底机制保持不变。响应后的 ✅/❌ 提示不走本函数:
 * settle 时同样处于 tool/result 未回窗口(直接写日志同样非法), 且 ✅ 没有「挂起期」诉求,
 * 维持既有入队 + flush 路径。
 */
function notifyPromptIntercepted(agent: unknown, text: string, summary: string): void {
  const inbox = (agent as { inbox?: { append?: (t: 'next-turn' | 'next-step', m: unknown) => void } } | undefined)?.inbox
  if (inbox?.append) {
    try {
      inbox.append('next-step', createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } }) as never)
      return
    } catch (e) {
      console.warn('[harness-mcp-server] prompt steering append failed, falling back to queued notice:', (e as Error)?.message ?? String(e))
    }
  }
  queuePromptNotice(agent, text, summary)
}

// ── turn 事实源: session log 折叠 + 冷读持久化日志 ──
// 「任务」的进度/结果不再由内存态 taskQueue 记录, 而是完全由 session log 推导:
//   live 会话 → 读内存日志(并经 turnBoundaryProjection 校正 turn 边界)
//   非 live(重启后/已退役) → 冷读持久化日志(官方 sessionPersistence 优先, zstd 落盘兜底), 因此重启不丢状态。

/** 事件的最小只读视图(事件是宿主 live 数据, 只取需要的叶子字段, 不整体序列化) */
interface EventView {
  type?: string
  seq?: number
  data?: Record<string, unknown>
}

/** turn 状态折叠: session log 为唯一事实源, live 与冷读共用同一折叠(保证重启前后语义一致) */
interface TurnState {
  /** 末尾 turn/start 无对应 turn/end 时 = 在飞 turn(进程已不在 = interrupted); null = 无在飞 turn */
  openTurn: { turn: number; startedSeq: number | null } | null
  /** 最近一次**已闭合**的 turn(其 reason 即本轮结束原因) */
  lastTurn: { turn: number; reason: { kind: string; error?: { code?: string; message?: string } } } | null
  /** 最后一个 turn 边界内的 assistant 文本(供 parseSummary 提取 changes/verification/leftovers) */
  lastTurnAssistantText: string
  /** 最近一条 assistant 文本(不限 turn) */
  lastText: string
}

/** 取一条消息事件的文本块(非文本块/空文本返回 '', 从根上消除 session_read 的 '(no text blocks)' 噪声) */
function assistantTextOf(event: unknown): string {
  const content = (event as { data?: { message?: { content?: Array<{ type?: string; text?: string }> } } })?.data?.message?.content
  if (!Array.isArray(content)) return ''
  return content
    .filter((c) => c.type === 'text' && typeof c.text === 'string' && c.text !== '')
    .map((c) => c.text as string)
    .join('\n')
}

/** 折叠事件流 → turn 边界 + 最后一个 turn 内的 assistant 文本(单遍 O(n)) */
function foldTurnState(events: readonly unknown[]): TurnState {
  const state: TurnState = { openTurn: null, lastTurn: null, lastTurnAssistantText: '', lastText: '' }
  let currentTurnText: string[] = []
  let sawTurnStart = false
  for (const raw of events) {
    const e = raw as EventView
    const type = e?.type
    if (type === 'turn/start') {
      const turn = Number((e.data as { turn?: unknown } | undefined)?.turn ?? 0)
      state.openTurn = { turn, startedSeq: typeof e.seq === 'number' ? e.seq : null }
      currentTurnText = []
      sawTurnStart = true
    } else if (type === 'turn/end') {
      const d = e.data as { turn?: unknown; reason?: { kind?: unknown; error?: { code?: string; message?: string } } } | undefined
      const turn = Number(d?.turn ?? state.openTurn?.turn ?? 0)
      if (state.openTurn !== null && state.openTurn.turn === turn) state.openTurn = null
      const error = d?.reason?.error
      state.lastTurn = {
        turn,
        reason: {
          kind: String(d?.reason?.kind ?? 'unknown'),
          ...(error !== undefined && error !== null
            ? { error: { ...(error.code !== undefined ? { code: error.code } : {}), ...(error.message !== undefined ? { message: error.message } : {}) } }
            : {}),
        },
      }
      state.lastTurnAssistantText = currentTurnText.join('\n')
    } else if (type === 'assistant/message') {
      const text = assistantTextOf(e)
      if (text) { currentTurnText.push(text); state.lastText = text }
    }
  }
  // 尚未闭合的在飞 turn: 其部分文本也算「最后一个 turn 边界内」(调用方据 phase 判断是否已落定)
  if (sawTurnStart && state.openTurn !== null) state.lastTurnAssistantText = currentTurnText.join('\n')
  return state
}

/** 宿主 turnBoundaryProjection 的只读视图(agent-loop 注册; 服务缺失时 undefined) */
interface TurnBoundaryView {
  openTurnStartSeq?: number | null
  lastTurn?: number
}

/** 取宿主投影里的 turn 边界事实(权威; 日志被尾部窗口截断时用它补齐 openTurn) */
function turnBoundaryOf(ctx: Context, session: unknown): TurnBoundaryView | undefined {
  try {
    const sp = ctx.get('sessionProjections') as { stateOf?: (s: unknown, key: string) => unknown } | undefined
    const state = sp?.stateOf?.(session, 'turnBoundary')
    return state === undefined || state === null ? undefined : (state as TurnBoundaryView)
  } catch {
    return undefined
  }
}

/** live 会话的 turn 状态: 日志折叠为主, turnBoundaryProjection 校正(投影说在飞但窗口里看不到 start 时补齐) */
function liveTurnState(ctx: Context, session: unknown): TurnState {
  const state = foldTurnState((session as { log?: readonly unknown[] })?.log ?? [])
  const boundary = turnBoundaryOf(ctx, session)
  if (boundary !== undefined && boundary.openTurnStartSeq !== null && boundary.openTurnStartSeq !== undefined) {
    state.openTurn = state.openTurn ?? { turn: boundary.lastTurn ?? 0, startedSeq: boundary.openTurnStartSeq }
  }
  return state
}

// ── 冷读: 非 live 会话从持久化日志推导 turn 状态(重启不丢) ──

/** zstd CLI(child_process; 落盘兜底解压用) */
const execFileAsync = promisify(execFile)

/** DSH_HOME(默认 ~/.dsh) */
function dshHome(): string {
  return process.env.DSH_HOME ?? join(homedir(), '.dsh')
}

/** 持久化 read 句柄的只读视图(官方 API; 鸭子类型, 避免硬依赖内部类型) */
interface ColdHandle {
  header?: { cwd?: string; agentPreset?: string; createdAt?: number }
  read(offset?: number, length?: number): Promise<{ events: readonly unknown[] }>
  close(): Promise<void>
}

/** ctx.sessionPersistence 的只读视图(鸭子类型) */
interface PersistenceView {
  stat?: (id: unknown, options?: unknown) => Promise<{ eventCount?: number } | undefined>
  open?: (id: unknown, access: 'read', options?: unknown) => Promise<ColdHandle>
  list?: (options?: unknown) => Promise<Array<{ id: unknown; cwd?: string; agentPreset?: string }>>
}

/** 冷读结果 */
interface ColdLog {
  /** 事件窗口(可能是尾部切片; windowStart > 0 时非全量) */
  events: unknown[]
  /** 日志总事件数 */
  total: number
  /** 本次读取覆盖的起始 seq(>0 表示只读了尾部窗口) */
  windowStart: number
  source: 'persistence' | 'file'
  path?: string
  header?: { cwd?: string; agentPreset?: string; createdAt?: number }
}

/** 窗口内是否已包含一个完整的 turn 边界(最后一段 turn 可判定) */
function windowHasTurnBoundary(events: readonly unknown[]): boolean {
  let lastStart = -1
  let lastEnd = -1
  for (let i = 0; i < events.length; i++) {
    const t = (events[i] as EventView)?.type
    if (t === 'turn/start') lastStart = i
    else if (t === 'turn/end') lastEnd = i
  }
  if (lastStart < 0) return false
  if (lastEnd > lastStart) return true
  return (events[0] as EventView | undefined)?.type === 'turn/start'
}

/**
 * 冷读一个非 live 会话的事件流(尾部窗口):
 *  ① 官方 ctx.sessionPersistence.open(id, 'read') —— 校验过的连续事件前缀, 跨进程安全, 与格式版本无关;
 *  ② 兜底: 直接解压 ~/.dsh/sessions/<项目键>/<sessionId>/session.vN.jsonl.zstd(zstd CLI)——
 *     官方读句柄不可用时仍能拿到事实(撕裂尾行由 JSON.parse 逐行容错跳过)。
 *
 * 注(实测 dsh 0.1.5-rc.2 + dsh-session-persistence-jsonl): stat() 不提供 eventCount, 拿不到总长时
 * 退化走 read(0) 全量读 + 尾部切片; 后端若提供 eventCount 则直接按窗口 seek(少读很多)。两者结果一致。
 */
async function readColdLog(ctx: Context, sessionId: string, minEvents: number): Promise<ColdLog | undefined> {
  const CHUNK = 2000
  const MAX = 30000
  const persistence = ctx.get('sessionPersistence') as PersistenceView | undefined
  if (persistence?.open) {
    let handle: ColdHandle | undefined
    try {
      handle = await persistence.open(SessionId(sessionId), 'read')
    } catch {
      handle = undefined // 持久化里没有该会话(或读取被拒) → 落盘兜底
    }
    if (handle !== undefined) {
      try {
        let total: number | undefined
        try { total = (await persistence.stat?.(SessionId(sessionId)))?.eventCount } catch { /* 元数据缺失: 退化为全量读 */ }
        let events: readonly unknown[] = []
        let windowStart = 0
        if (typeof total === 'number' && total > 0) {
          let take = Math.max(CHUNK, Math.min(minEvents, MAX))
          for (;;) {
            windowStart = Math.max(0, total - take)
            events = (await handle.read(windowStart, total - windowStart)).events
            if (windowStart === 0 || events.length >= MAX) break
            if (events.length >= minEvents && windowHasTurnBoundary(events)) break
            if (take >= MAX) break
            take = Math.min(MAX, take * 2)
          }
        } else {
          events = (await handle.read(0)).events
          windowStart = 0
          total = events.length
        }
        return {
          events: [...events],
          total: typeof total === 'number' ? total : events.length,
          windowStart,
          source: 'persistence',
          ...(handle.header !== undefined ? { header: handle.header } : {}),
        }
      } catch (e) {
        console.warn('[harness-mcp-server] persisted log read via sessionPersistence failed, falling back to disk:', (e as Error)?.message ?? e)
      } finally {
        try { await handle.close() } catch { /* ignore */ }
      }
    }
  }
  return readColdLogFromDisk(sessionId, MAX)
}

/** 落盘兜底: 扫 ~/.dsh/sessions/<项目键>/<sessionId>/ 下的 session.vN.jsonl(.zstd) 并解出事件 */
async function readColdLogFromDisk(sessionId: string, maxEvents: number): Promise<ColdLog | undefined> {
  const root = join(dshHome(), 'sessions')
  let projects: string[]
  try { projects = await readdir(root) } catch { return undefined }
  for (const project of projects) {
    const dir = join(root, project, sessionId)
    let files: string[]
    try { files = await readdir(dir) } catch { continue } // 该会话不在这个项目目录下
    const logs = files.filter((f) => /^session(\.v\d+)?\.jsonl(\.zstd)?$/.test(f)).sort()
    const name = logs[logs.length - 1]
    if (name === undefined) continue
    const path = join(dir, name)
    let text: string
    try {
      if (name.endsWith('.zstd')) {
        const { stdout } = await execFileAsync('zstd', ['-dc', '--', path], { maxBuffer: 512 * 1024 * 1024 })
        text = stdout
      } else {
        text = await readFile(path, 'utf8')
      }
    } catch (e) {
      console.warn('[harness-mcp-server] cold log read failed:', path, (e as Error)?.message ?? e)
      continue
    }
    const all: unknown[] = []
    let header: ColdLog['header']
    for (const line of text.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        const row = JSON.parse(trimmed) as { type?: string; cwd?: string; agentPreset?: string; createdAt?: number }
        if (row.type === 'session') {
          header = {
            ...(row.cwd !== undefined ? { cwd: row.cwd } : {}),
            ...(row.agentPreset !== undefined ? { agentPreset: row.agentPreset } : {}),
            ...(row.createdAt !== undefined ? { createdAt: row.createdAt } : {}),
          }
          continue
        }
        all.push(row)
      } catch { /* 撕裂尾行/非法行: 跳过 */ }
    }
    const windowStart = Math.max(0, all.length - maxEvents)
    return {
      events: all.slice(windowStart),
      total: all.length,
      windowStart,
      source: 'file',
      path,
      ...(header !== undefined ? { header } : {}),
    }
  }
  return undefined
}

/** 非 live 会话的 header(cwd/preset): 冷读日志头部优先已有, 这里补持久化 list 兜底 */
async function persistedHeaderOf(ctx: Context, sessionId: string): Promise<{ cwd?: string; agentPreset?: string } | undefined> {
  try {
    const persistence = ctx.get('sessionPersistence') as PersistenceView | undefined
    for (const h of (await persistence?.list?.()) ?? []) {
      if (String(h.id) === sessionId) {
        return { ...(h.cwd !== undefined ? { cwd: h.cwd } : {}), ...(h.agentPreset !== undefined ? { agentPreset: h.agentPreset } : {}) }
      }
    }
  } catch { /* ignore */ }
  return undefined
}

/** 订阅某会话的已提交事件(只读叶子字段, 不搬运 live 对象); 宿主事件不可用时返回 undefined */
function onSessionEvent(ctx: Context, sessionId: string, listener: (event: unknown) => void): (() => void) | undefined {
  try {
    const on = ctx.on as unknown as (name: string, fn: (session: unknown, event: unknown) => void) => unknown
    const disposer = on.call(ctx, 'session/event', (session: unknown, event: unknown) => {
      if (String((session as { id?: unknown } | undefined)?.id) !== sessionId) return
      listener(event)
    })
    return typeof disposer === 'function' ? (disposer as () => void) : undefined
  } catch (e) {
    console.warn('[harness-mcp-server] session/event subscription unavailable:', (e as Error)?.message ?? String(e))
    return undefined
  }
}

// ── 派活: 任务文本组装 + 投喂后登记 ──

/** 组装完整任务文本: 记忆上下文 + 任务 + 结构化输出要求(照搬旧 executeTask 的模板) */
function composeTaskMessage(context: string, task: string): string {
  return [
    context ? `【记忆/上下文(供参考, 来自 Hermes 大脑)】\n${context}\n` : '',
    `【任务】\n${task}\n`,
    `【完成后必须】用一行 JSON 总结(不要 markdown 代码块包裹, 直接输出这一行):`,
    `{"changes":"改了什么","verification":"怎么验证的","leftovers":"遗留问题"}`,
  ].filter(Boolean).join('\n')
}

/** 临时 resume 句柄的释放上限(观测不到 turn/end 时的兜底, 防句柄泄漏) */
const DETACHED_RELEASE_MAX_MS = 60 * 60 * 1000

/**
 * 投喂后登记: 该会话「下一个 turn/end」落定时释放临时 resume 句柄(flush + dispose)。
 * 与旧 executeTask 的 disposeAfter 语义对齐 —— 区别只在于**投喂方不再持有等待**, 释放改由事件驱动。
 * 只在非驻池(resume 出来的独占句柄)上调用; 驻池句柄永不 dispose。
 */
function releaseAfterTurn(ctx: Context, sessionId: string, handle: AgentHandle): void {
  let done = false
  let ourTurn: number | null = null
  let timer: ReturnType<typeof setTimeout> | undefined
  const finish = async (): Promise<void> => {
    if (done) return
    done = true
    if (timer !== undefined) clearTimeout(timer)
    off?.()
    try {
      await (ctx.get('sessions') as { flush?: (s: unknown) => Promise<unknown> } | undefined)?.flush?.(handle.agent.session)
    } catch { /* flush 失败不阻断释放 */ }
    try { await handle.dispose() } catch { /* 释放失败不影响调用方 */ }
  }
  const off = onSessionEvent(ctx, sessionId, (event) => {
    const e = event as EventView
    if (e.type === 'turn/start') {
      if (ourTurn === null) ourTurn = Number((e.data as { turn?: unknown } | undefined)?.turn ?? 0)
    } else if (e.type === 'turn/end' && ourTurn !== null && Number((e.data as { turn?: unknown } | undefined)?.turn ?? -1) === ourTurn) {
      void finish()
    }
  })
  timer = setTimeout(() => void finish(), DETACHED_RELEASE_MAX_MS)
  if (typeof timer === 'object' && timer !== null) (timer as { unref?: () => void }).unref?.()
}

// ── 主动查询: session_status 组装(供 session_status / session_wait 共用) ──

/** session_status 的返回体(也是 session_wait 的 status 字段) */
interface SessionStatusBody {
  sessionId: string
  live: boolean
  source: 'live' | 'persisted'
  cwd?: string
  title?: string
  agentStatus?: string
  phase: 'idle' | 'running' | 'waiting_input' | 'interrupted'
  openTurn: { turn: number; startedSeq: number | null } | null
  lastTurn: TurnState['lastTurn']
  prompts: PromptRow[]
  context: ContextUsage | null
  logEvents: number
  changes: string | null
  verification: string | null
  leftovers: string | null
  lastText: string
  note?: string
}

/** 组装会话状态: live 直读内存日志; 非 live 冷读持久化日志(末尾 turn/start 无 turn/end = interrupted) */
async function buildSessionStatus(ctx: Context, sessionId: string): Promise<SessionStatusBody | undefined> {
  const agent = liveAgentFor(ctx, sessionId) as Agent | undefined
  const session = agent?.session as unknown
  let state: TurnState
  let prompts: PromptRow[]
  let context: ContextUsage | null
  let logEvents: number
  let source: 'live' | 'persisted'
  let cwd: string | undefined
  let title: string | undefined
  let note: string | undefined

  if (agent !== undefined && session !== undefined) {
    const log = (session as { log?: readonly unknown[] }).log ?? []
    state = liveTurnState(ctx, session)
    prompts = promptsFor(sessionId, session)
    logEvents = log.length
    source = 'live'
    cwd = (session as { header?: { cwd?: string } }).header?.cwd
    title = sessionTitleOf(ctx, session)
    context = await contextUsage(ctx, session, agent)
  } else {
    const cold = await readColdLog(ctx, sessionId, 200)
    if (cold === undefined) return undefined
    state = foldTurnState(cold.events)
    prompts = promptsFor(sessionId, undefined)
    logEvents = cold.total
    source = 'persisted'
    const header = cold.header ?? await persistedHeaderOf(ctx, sessionId)
    cwd = header?.cwd
    note = cold.windowStart > 0
      ? `cold read from the persisted session log (tail window: seq ${cold.windowStart}..${cold.total - 1} of ${cold.total}, source=${cold.source})`
      : `cold read from the persisted session log (source=${cold.source}); the process that owned this session is gone, so nothing is running — an unbalanced tail turn/start is reported as interrupted`
    // 非 live 没有 Session 对象可供 tokenMeter 计量(挂起弹窗也随进程消失), context 恒 null
    context = null
  }

  const phase: SessionStatusBody['phase'] = source === 'live'
    ? (prompts.length > 0 ? 'waiting_input' : (state.openTurn !== null || agent?.status === 'running' ? 'running' : 'idle'))
    : (state.openTurn !== null ? 'interrupted' : 'idle')

  const summary = state.lastTurnAssistantText ? parseSummary(state.lastTurnAssistantText) : { changes: '', verification: '', leftovers: '' }
  const hasSummary = summary.changes !== '' || summary.verification !== '' || summary.leftovers !== ''
  return {
    sessionId,
    live: source === 'live',
    source,
    ...(cwd !== undefined ? { cwd } : {}),
    ...(title !== undefined ? { title } : {}),
    ...(source === 'live' && agent?.status !== undefined ? { agentStatus: agent.status } : {}),
    phase,
    openTurn: state.openTurn,
    lastTurn: state.lastTurn,
    prompts,
    context,
    logEvents,
    changes: hasSummary ? summary.changes : null,
    verification: hasSummary ? summary.verification : null,
    leftovers: hasSummary ? summary.leftovers : null,
    lastText: state.lastText,
    ...(note !== undefined ? { note } : {}),
  }
}

/** session_wait 单段等待上限(MCP 客户端 HTTP 超时通常更短, 这里给足但封顶) */
const MAX_WAIT_MS = 240000

/** 等会话落定(事件驱动 + 500ms 兜底轮询): true = 目标已达成, false = 超时 */
function waitForSettle(
  ctx: Context,
  sessionId: string,
  agent: Agent,
  until: 'turn-end' | 'idle' | 'input',
  capMs: number,
  minTurn: number,
): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    let done = false
    let off: (() => void) | undefined
    let timer: ReturnType<typeof setTimeout> | undefined
    let interval: ReturnType<typeof setInterval> | undefined
    const finish = (settled: boolean): void => {
      if (done) return
      done = true
      if (timer !== undefined) clearTimeout(timer)
      if (interval !== undefined) clearInterval(interval)
      off?.()
      resolve(settled)
    }
    const check = (): void => {
      if (done) return
      const state = liveTurnState(ctx, agent.session)
      const prompts = promptsFor(sessionId, agent.session)
      if (until === 'input') { if (prompts.length > 0) finish(true); return }
      if (until === 'idle') {
        if (state.openTurn === null && prompts.length === 0 && (agent.status ?? 'idle') === 'idle') finish(true)
        return
      }
      if (state.openTurn === null && state.lastTurn !== null && state.lastTurn.turn >= minTurn) finish(true)
    }
    off = onSessionEvent(ctx, sessionId, () => check())
    interval = setInterval(check, 500)
    if (typeof interval === 'object' && interval !== null) (interval as { unref?: () => void }).unref?.()
    timer = setTimeout(() => finish(false), capMs)
    if (typeof timer === 'object' && timer !== null) (timer as { unref?: () => void }).unref?.()
    check()
  })
}

/** 找会话 header: live 优先, 其次持久化 list(轻量元数据扫描, 不加载整日志) */
async function findSessionHeader(ctx: Context, sessionId: SessionId): Promise<SessionHeader | undefined> {
  const sessions = ctx.get('sessions') as { get?: (id: SessionId) => { header: SessionHeader } | undefined } | undefined
  const live = sessions?.get?.(sessionId)
  if (live !== undefined) return live.header
  const persistence = ctx.get('sessionPersistence') as { list?: () => Promise<SessionHeader[]> } | undefined
  for (const header of (await persistence?.list?.()) ?? []) {
    if (header.id === sessionId) return header
  }
  return undefined
}

/**
 * 存量捞回: 启动时把现存未分组的会话补挂到已注册工作区。
 * 条件: header.cwd 的 realpath 等于某已注册 workspace.path, 且该 sessionId 不在其花名册里。
 * 只补挂到"已注册"工作区, 不新建(避免把无关目录刷成新工作区); 单会话失败不影响其余。
 */
async function reattachOrphanSessions(ctx: Context): Promise<{ attached: number; failed: number }> {
  const registry = ctx.get('workspaceRegistry') as WorkspaceRegistryView | undefined
  const byPath = new Map<string, WorkspaceView>()
  for (const ws of registry?.list?.() ?? []) byPath.set(ws.path, ws)
  if (byPath.size === 0) return { attached: 0, failed: 0 }

  // live + 持久化 header 合并(live 优先), 按 id 去重
  const headers = new Map<string, SessionHeader>()
  const sessions = ctx.get('sessions') as { list?: () => { header: SessionHeader }[] } | undefined
  for (const session of sessions?.list?.() ?? []) headers.set(session.header.id, session.header)
  const persistence = ctx.get('sessionPersistence') as { list?: () => Promise<SessionHeader[]> } | undefined
  for (const header of (await persistence?.list?.()) ?? []) {
    if (!headers.has(header.id)) headers.set(header.id, header)
  }

  let attached = 0
  let failed = 0
  for (const header of headers.values()) {
    if (header.cwd === undefined) continue
    const canonical = await canonicalCwd(header.cwd)
    const ws = byPath.get(canonical)
    if (ws === undefined || !ws.attachSession) continue
    if (ws.sessionIds.includes(header.id)) continue
    try {
      await ws.attachSession(header.id)
      attached++
      console.log(`[harness-mcp-server] 存量捞回: session ${header.id} -> workspace ${ws.path}`)
    } catch (e) {
      failed++
      console.warn(`[harness-mcp-server] 存量捞回失败 session ${header.id}:`, (e as Error)?.message ?? e)
    }
  }
  return { attached, failed }
}

/** 在给定 McpServer 上注册工具 */
function registerTools(mcp: McpServer, ctx: Context): void {
  mcp.tool('echo', '回显输入, 验证 MCP server 连通', { text: z.string() }, async ({ text }) => {
    return out(`收到: ${text} @ ${Date.now()}`)
  })

  mcp.tool('harness_list_tools', '列出 Harness 当前注册的所有工具名', {}, async () => {
    const tools = ctx.tools as unknown as { keys?: () => Iterable<string> } | null
    const names = tools && typeof tools.keys === 'function' ? Array.from(tools.keys()) : []
    return out(JSON.stringify(names))
  })

  // 运维总览: agent 池/live 会话/运行时配置 —— 外部客户端一眼看清系统水位(任务层已降维为 session+turn)
  mcp.tool(
    'harness_status',
    '系统水位总览: agent 常驻池、live 会话数、MCP 在飞 turn(按会话)、运行时配置。',
    {},
    async () => {
      const liveCount = ((ctx.agents as unknown as { list?: () => unknown[] }).list?.() ?? []).length
      const inFlight = [...mcpPendingTurns.entries()].map(([sessionId, turns]) => ({ sessionId, turns }))
      return out(JSON.stringify({
        uptimeSec: Math.round(process.uptime()),
        agentPool: { size: liveAgents.size, max: runtimeConfig.maxAgents, liveAgents: liveCount },
        mcpInFlightTurns: { sessions: inFlight.length, total: inFlight.reduce((n, r) => n + r.turns, 0), detail: inFlight },
        config: {
          provider: runtimeConfig.provider,
          model: runtimeConfig.model || '(dsh default)',
          preset: runtimeConfig.preset,
          maxAgents: runtimeConfig.maxAgents,
          taskTimeoutMs: runtimeConfig.taskTimeoutMs,
        },
      }, null, 2))
    },
  )

  // 模型目录: 缺省枚举所有已注册 provider 的模型(listProviders), 并补上已声明但未激活的配置 provider;
  // 传 provider 只列该 provider; withWindow=true 时逐模型解析上下文窗口(多一次 llm 查询)
  mcp.tool(
    'model_list',
    '列出可用模型目录: 缺省枚举所有已注册 provider 的模型(listProviders), 并补上已声明但未激活的配置 provider(active:false); 传 provider 只列该 provider; withWindow=true 时逐模型解析 contextWindow(可能较慢)。',
    {
      provider: z.string().optional().describe('只列出该 provider 路由的模型(缺省: 全部已注册 provider)'),
      withWindow: z.boolean().optional().describe('true = 逐模型解析 contextWindow'),
    },
    async ({ provider, withWindow }) => {
      const llm = ctx.get('llm') as {
        listProviders?: () => { id: string; name?: string }[]
        listModels?: (p: string) => Promise<{ id: string; name?: string; description?: string; inputModalities?: readonly string[] }[]>
        listConfigurableProviders?: () => { provider: string; displayName?: string; declared?: boolean }[]
      } | undefined
      if (!llm?.listProviders) return err(JSON.stringify({ error: 'llm service unavailable' }))
      const registered = llm.listProviders?.() ?? []
      const directory = llm.listConfigurableProviders?.() ?? []

      const rows: Record<string, unknown>[] = []
      const seen = new Set<string>()
      // 指定 provider 时只列它(未注册 → 报错行); 否则遍历全部已注册路由
      const targets = provider ? [{ id: provider, name: provider }] : registered
      for (const p of targets) {
        seen.add(p.id)
        try {
          const models = (await llm.listModels?.(p.id)) ?? []
          const listed = await Promise.all(models.map(async (m) => {
            const row: Record<string, unknown> = { id: m.id, name: m.name, description: m.description, inputModalities: m.inputModalities }
            if (withWindow) row.contextWindow = await modelWindowOf(ctx, p.id, m.id)
            return row
          }))
          rows.push({ provider: p.id, providerName: p.name, active: true, total: listed.length, models: listed })
        } catch (e) {
          rows.push({ provider: p.id, providerName: p.name, active: true, error: (e as Error)?.message ?? String(e) })
        }
      }
      // 补全目录(仅在枚举全部时): 已声明但未注册(未激活/未配置)的 provider 也列出, 客户端可见"全部可能配置"
      if (!provider) {
        for (const cp of directory) {
          if (seen.has(cp.provider)) continue
          seen.add(cp.provider)
          rows.push({
            provider: cp.provider,
            providerName: cp.displayName,
            active: false,
            total: 0,
            models: [],
            note: 'declared but not active (configure the provider to activate)',
          })
        }
      }
      return out(JSON.stringify({ total: rows.length, providers: rows }, null, 2))
    },
  )

  // 模式目录: 会话「模式」= agent preset(standard/code/cordis 等) + 沙箱访问模式(read-only/workspace-write/
  // danger-full-access) + 审批策略(ask/never) + 权限预设(捆绑沙箱+审批, 如 workspace-write = workspace-write+ask)。
  // modes 汇总给出可传给 session_send 的 mode= 规范 id(按类别), 与 model_list 的枚举姿势一致:
  // presets 经 ctx.agentPresets.list() 实时枚举, 沙箱/审批词汇固定, 默认值经 ctx.sandboxPolicy / ctx.approval /
  // ctx.permissionPresets(服务缺省时给安全回退)。withDetail=true 附带更多元数据与部署默认。
  mcp.tool(
    'mode_list',
    '列出可用会话模式: agent preset(standard/code/cordis/minimal 等, 来自 dsh agent-presets) + 沙箱访问模式(read-only/workspace-write/danger-full-access) + 审批策略(ask/never) + 权限预设(捆绑沙箱+审批, 如 workspace-write = workspace-write + ask)。modes 汇总给出可传给 session_send 的 mode= 规范 id; 传 only 只列某一类。',
    {
      only: z.enum(['preset', 'sandbox', 'approval', 'permission']).optional().describe('只列出某一类(preset/sandbox/approval/permission); 缺省列全部'),
      withDetail: z.boolean().optional().describe('true = 附带详细字段(preset 路径/顺序/损坏原因, 部署默认, 每个 mode 的语义/适用场景)'),
    },
    async ({ only, withDetail }) => {
      const detail = withDetail === true
      const agentPresets = ctx.get('agentPresets') as AgentPresetsView | undefined
      const permissionPresets = ctx.get('permissionPresets') as PermissionPresetsView | undefined
      const sandboxPolicy = ctx.get('sandboxPolicy') as SandboxPolicyView | undefined
      const approvalService = ctx.get('approval') as ApprovalServiceView | undefined
      const defaults = deploymentModeDefaults(ctx)

      // 1) agent presets(实时枚举; 服务缺失 → 空并注明)
      const presets: Record<string, unknown>[] = []
      if (agentPresets?.list) {
        try {
          const list = await agentPresets.list()
          for (const p of list) {
            const row: Record<string, unknown> = {
              id: p.id,
              name: p.name ?? p.id,
              description: p.description,
              trust: p.trust,
              order: p.order,
              default: p.id === defaults.preset,
              ...(p.broken !== undefined ? { broken: p.broken } : {}),
            }
            if (detail) {
              row.path = p.path
              row.kind = 'preset'
            }
            presets.push(row)
          }
        } catch (e) {
          presets.push({ error: `agentPresets.list failed: ${(e as Error)?.message ?? String(e)}` })
        }
      } else {
        presets.push({ note: 'agentPresets service unavailable (no presets enumerated)' })
      }

      // 2) 沙箱访问模式(固定词汇 + 部署默认标注)
      const sandboxModes: Record<string, unknown>[] = SANDBOX_MODES.map((m) => ({
        id: m,
        description: SANDBOX_MODE_DESCRIPTIONS[m] ?? '',
        default: m === defaults.sandbox,
        ...(detail ? { kind: 'sandbox', workspaceRoot: sandboxPolicy?.workspaceRoot } : {}),
      }))

      // 3) 审批策略(固定词汇 + 部署默认标注)
      const approvalPolicies: Record<string, unknown>[] = APPROVAL_POLICIES.map((p) => ({
        id: p,
        description: APPROVAL_POLICY_DESCRIPTIONS[p] ?? '',
        default: p === defaults.approval,
        ...(detail ? { kind: 'approval' } : {}),
      }))

      // 4) 权限预设(捆绑沙箱+审批; 服务缺失 → 空)
      const permissionPresetsList: Record<string, unknown>[] = []
      if (permissionPresets?.resolve) {
        for (const name of permissionPresets.names ?? []) {
          try {
            const spec = permissionPresets.resolve(name)
            if (!spec) continue
            permissionPresetsList.push({
              id: name,
              name: spec.name ?? name,
              description: spec.description,
              sandbox: spec.sandbox,
              approval: spec.approval,
              default: name === (defaults.permissionPreset ?? permissionPresets.defaultPreset),
              ...(detail ? { kind: 'permission' } : {}),
            })
          } catch { /* 单条解析失败跳过 */ }
        }
      }

      // 5) modes 汇总: 传给 session_send mode= 的规范 id(按类别去重, 与类别行同序)
      const modes: Record<string, unknown>[] = []
      const seen = new Set<string>()
      for (const p of permissionPresetsList) {
        const id = String(p.id)
        if (seen.has(id)) continue
        seen.add(id)
        modes.push({ id, kind: 'permission', sandbox: p.sandbox, approval: p.approval, name: p.name })
      }
      for (const s of sandboxModes) {
        const id = String(s.id)
        if (seen.has(id)) continue
        seen.add(id)
        modes.push({ id, kind: 'sandbox', description: s.description })
      }
      for (const a of approvalPolicies) {
        const id = String(a.id)
        if (seen.has(id)) continue
        seen.add(id)
        modes.push({ id, kind: 'approval', description: a.description })
      }
      for (const pr of presets) {
        if (pr.id === undefined) continue
        const id = String(pr.id)
        if (seen.has(id)) continue
        seen.add(id)
        modes.push({ id, kind: 'preset', name: pr.name ?? id, description: pr.description })
      }

      const categories = ['preset', 'sandbox', 'approval', 'permission']
      const body: Record<string, unknown> = { total: 4, categories }
      if (only === undefined || only === 'preset') body.presets = presets
      if (only === undefined || only === 'sandbox') body.sandboxModes = sandboxModes
      if (only === undefined || only === 'approval') body.approvalPolicies = approvalPolicies
      if (only === undefined || only === 'permission') body.permissionPresets = permissionPresetsList
      if (only === undefined) {
        body.modes = modes
        body.deployment = {
          defaultPreset: defaults.preset,
          defaultSandboxMode: defaults.sandbox,
          defaultApprovalPolicy: defaults.approval,
          ...(defaults.permissionPreset !== undefined ? { defaultPermissionPreset: defaults.permissionPreset } : {}),
        }
      }
      return out(JSON.stringify(body, null, 2))
    },
  )

  // 工作区分组视图: 对齐 UI 侧 dsh-workspace, 便于按项目维度管理会话
  mcp.tool(
    'workspace_list',
    '列出工作区及其会话分组(dsh-workspace 的花名册), 便于按项目维度管理; workspaceRegistry 未加载时报错。',
    {},
    async () => {
      const registry = ctx.get('workspaceRegistry') as { list?: () => { id?: string; path?: string; title?: string; sessionIds?: readonly string[] }[] } | undefined
      const list = registry?.list?.() ?? []
      const workspaces = list.map((w) => ({
        id: w.id, path: w.path, title: w.title,
        sessionCount: w.sessionIds?.length ?? 0,
        sessionIds: (w.sessionIds ?? []).slice(0, 100),
      }))
      return out(JSON.stringify({ total: workspaces.length, workspaces }, null, 2))
    },
  )

  // 读会话事件流: 审计或续接前回顾 Harness 到底做了什么
  mcp.tool(
    'session_read',
    '读会话的事件流(文本/工具调用/结果), 审计或续接前回顾。池/live 会话直读; 持久化会话临时 resume 读取后 flush 并释放。',
    {
      sessionId: z.string().describe('要读取的会话 id(池/live/持久化均可)'),
      limit: z.number().int().min(1).max(500).optional().describe('最多返回最近事件数(默认 100)'),
    },
    async ({ sessionId, limit }) => {
      let resolved: ResolvedAgent
      try {
        resolved = await getAgent(ctx, '', sessionId)
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      const agent = resolved.handle.agent
      try {
        const log = ((agent.session as unknown as { log?: unknown[] }).log ?? [])
        // limit 按「表面事件」计: 真实 dsh 日志里 assistant/chunk、reasoning-chunks、step/* 等流式/内部事件
        // 占绝对多数且稀疏夹杂表面事件, 直接 slice 原始日志会让 limit 失效(最后 N 条原始日志常只含 1 条表面事件)。
        // 因此先收集表面事件下标, 再取最近 N 条格式化。
        const surfaceTypes = new Set(['user/message', 'assistant/message', 'tool/call', 'tool/result'])
        const max = limit ?? 100
        const surfaceIdx: number[] = []
        for (let i = 0; i < log.length; i++) {
          const t = (log[i] as { type?: string })?.type
          if (t !== undefined && surfaceTypes.has(t)) surfaceIdx.push(i)
        }
        const start = Math.max(0, surfaceIdx.length - max)
        const events: { seq?: number; type?: string; text?: string }[] = []
        for (let k = start; k < surfaceIdx.length; k++) {
          const ev = log[surfaceIdx[k] as number]
          const e = ev as { seq?: number; type?: string; data?: { message?: { content?: { type?: string; text?: string }[] }; name?: string; arguments?: string } }
          const type = e.type
          if (type === 'user/message' || type === 'assistant/message') {
            const content = e.data?.message?.content
            const text = (content ?? []).filter((c) => c.type === 'text' && c.text).map((c) => c.text).join('\n').slice(0, 4000)
            events.push({ seq: e.seq, type, text: text || '(no text blocks)' })
          } else if (type === 'tool/call') {
            events.push({ seq: e.seq, type, text: `${e.data?.name ?? '?'}(${String(e.data?.arguments ?? '').slice(0, 2000)})` })
          } else if (type === 'tool/result') {
            const texts: string[] = []
            extractText(e.data ?? ev, texts)
            events.push({ seq: e.seq, type, text: texts.join('\n').slice(0, 3000) || '(empty result)' })
          }
        }
        return out(JSON.stringify({ sessionId, total: surfaceIdx.length, logEvents: log.length, returned: events.length, events }, null, 2))
      } finally {
        if (resolved.disposeAfter) {
          try { await (ctx.get('sessions') as { flush?: (s: unknown) => Promise<unknown> } | undefined)?.flush?.(agent.session) } catch { /* ignore */ }
          try { await resolved.handle.dispose() } catch { /* ignore */ }
        }
      }
    },
  )

  // 【派活入口】把一个 turn 投喂进会话: resume-or-create agent → 组装 message → followup → **立即返回**。
  // 不再有 taskId/任务队列/超时阻塞: 之后用 session_status 主动查询(session log 为唯一事实源),
  // 或用 session_wait 可选阻塞一段, 或用 session_cancel 打断。
  mcp.tool(
    'session_send',
    '把一条任务作为一个 turn 投喂进会话并立即返回(不等待、不超时阻塞)。传 sessionId 续接已有会话, 或传 cwd 复用/创建该目录的常驻会话(newSession:true 强制全新会话)。消息模板 = 记忆上下文 + 【任务】 + 「完成后必须输出一行 summary JSON」。返回 {sessionId, inboxDepth, openTurn}; 之后用 session_status 查询 phase/lastTurn 与结构化结果(changes/verification/leftovers), 用 session_tail 拉过程明细, 用 session_cancel 打断。',
    {
      sessionId: z.string().optional().describe('续接已有会话的 sessionId(与 cwd 二选一, 优先)'),
      cwd: z.string().optional().describe('工作目录(不传 sessionId 时必填): 复用该目录的常驻会话, 没有则新建; 可用 workspace_list 查看'),
      message: z.string().describe('任务内容(自然语言)'),
      context: z.string().optional().describe('Hermes 记忆/上下文, 随任务注入给 agent 参考'),
      newSession: z.boolean().optional().describe('true = 强制全新会话(跳过该 cwd 的池复用; 旧会话退役但持久化保留, 仍可凭 sessionId 续接)'),
      title: z.string().optional().describe('新会话的标题(仅新建时生效; 缺省按任务内容自动派生)'),
      model: z.string().optional().describe('本次使用的模型 id(对新建/resume 会话生效; 池复用的会话保持原模型)'),
      provider: z.string().optional().describe('本次使用的 provider 路由(默认 deepseek-official)'),
      preset: z.string().optional().describe('agent preset id(standard/code/cordis/minimal 等): 新建时挂载并写进 session header; resume 存量会话时也允许(在 setup 挂载该 preset)'),
      mode: z.string().optional().describe('会话模式(仅新建会话时应用; 指定即强制全新会话)。取值见 mode_list 的 modes: 权限预设名(如 workspace-write = 沙箱 workspace-write + 审批 ask) 或沙箱模式(read-only/workspace-write/danger-full-access) 或审批策略(ask/never) 或 agent preset id'),
      sandbox: z.enum(['read-only', 'workspace-write', 'danger-full-access']).optional().describe('沙箱访问模式(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
      approval: z.enum(['ask', 'never']).optional().describe('审批策略(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
    },
    async ({ sessionId, cwd, message, context, newSession, title, model, provider, preset, mode, sandbox, approval }) => {
      if (!sessionId && !cwd) {
        return err(JSON.stringify({ error: 'sessionId or cwd is required: pass sessionId to continue a session, or cwd to reuse/create the pool session for that directory (see workspace_list)' }))
      }
      // 模式解析与前置校验: 非法 mode/preset/sandbox/approval 立即报错(不投喂); sandbox/approval 不可用于续接存量会话
      let modeRes: { preset?: string; sandbox?: string; approval?: string }
      try {
        modeRes = await resolveModeRequest(ctx, { mode, preset, sandbox, approval })
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      if (sessionId !== undefined && (modeRes.sandbox !== undefined || modeRes.approval !== undefined)) {
        return err(JSON.stringify({ error: 'mode/sandbox/approval only apply when creating a new session; pass newSession:true or omit sessionId (preset alone is allowed when resuming)' }))
      }
      // cwd 规范化: 只在显式给 cwd 时校验白名单(续接路径的目录由会话 header 决定); 新建必须有 cwd
      let workdir = ''
      if (cwd !== undefined && cwd !== '') {
        workdir = await canonicalCwd(resolve(cwd))
        if (!cwdAllowed(workdir)) {
          return err(JSON.stringify({ error: `cwd not allowed (outside workspaceRoots): ${workdir}` }))
        }
      }
      const modeRequested = modeRes.preset !== undefined || modeRes.sandbox !== undefined || modeRes.approval !== undefined
      const effectiveTitle = title ?? deriveSessionTitle(message || context || '')
      const lockKey = sessionId !== undefined ? `session:${sessionId}` : workdir
      // 串行锁只覆盖「解析/创建 agent + 投喂」这一小段, 投喂后立即释放(不再持有到 whenIdle)
      return withLock(lockKey, async () => {
        let resolved: ResolvedAgent
        try {
          resolved = await getAgent(
            ctx, workdir, sessionId, effectiveTitle,
            newSession === true || (modeRequested && sessionId === undefined),
            { provider, model }, modeRes,
          )
        } catch (e) {
          return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
        }
        const agent = resolved.handle.agent
        const sid = String(resolved.sessionId)
        // 登记「MCP 在飞 turn」(审批/提问接管判据): 该会话 turn/end 落定时自动 -1
        markMcpTurn(ctx, sid)
        try {
          agent.followup(createUserMessage({
            content: [{ type: 'text', text: composeTaskMessage(context ?? '', message) }],
            source: { kind: 'plugin', plugin: 'harness-mcp-server' },
          }))
        } catch (e) {
          unmarkMcpTurn(sid)
          if (resolved.disposeAfter) { try { await resolved.handle.dispose() } catch { /* ignore */ } }
          return err(JSON.stringify({ error: `send failed: ${(e as Error)?.message ?? String(e)}` }))
        }
        // 非驻池 resume 句柄: 本轮 turn 结束后 flush + dispose(不驻池, 也不阻塞本次调用)
        if (resolved.disposeAfter) releaseAfterTurn(ctx, sid, resolved.handle)
        const inbox = (agent as unknown as { inbox?: { nextTurn?: readonly unknown[]; nextStep?: readonly unknown[] } }).inbox
        const state = liveTurnState(ctx, agent.session)
        return out(JSON.stringify({
          sessionId: sid,
          ...(workdir !== '' ? { cwd: workdir } : {}),
          status: 'accepted',
          inboxDepth: (inbox?.nextTurn?.length ?? 0) + (inbox?.nextStep?.length ?? 0),
          openTurn: state.openTurn,
          ...(resolved.disposeAfter ? { note: 'resumed into a temporary handle; it is flushed and released once this turn settles' } : {}),
          hint: 'poll session_status (or block with session_wait) for phase/lastTurn and the changes/verification/leftovers summary',
        }, null, 2))
      })
    },
  )

  // 【主动查询】turn 状态以 session log 为唯一事实源: live 读内存日志 + turnBoundaryProjection;
  // 非 live(重启后/已退役)冷读持久化日志 —— 末尾 turn/start 无对应 turn/end 报 interrupted(进程不在了, 不是 running)。
  mcp.tool(
    'session_status',
    '主动查询会话/turn 状态(唯一事实源 = session log, 重启不丢): phase(idle/running/waiting_input/interrupted)、openTurn{turn,startedSeq}、lastTurn{turn,reason{kind,error?}}、prompts[](待响应的审批/提问)、context(events/tokens/pressure/window/ratio; 仅 live 可测, 非 live 为 null)、changes/verification/leftovers(从最后一个 turn 边界内的 assistant 文本 parseSummary 提取, 提不到为 null)、lastText。',
    { sessionId: z.string().describe('会话 id(session_send 返回的 sessionId; live/persisted 均可)') },
    async ({ sessionId }) => {
      const status = await buildSessionStatus(ctx, sessionId)
      if (status === undefined) {
        return err(JSON.stringify({ error: `session not found: ${sessionId} (not live in this process and not persisted under ${join(dshHome(), 'sessions')})` }))
      }
      return out(JSON.stringify(status, null, 2))
    },
  )

  // 【过程明细】按需拉取表面事件(修掉 session_read 对 LLM 不友好的两点: '(no text blocks)' 噪声 + 98% 内部事件)
  mcp.tool(
    'session_tail',
    '按需拉取会话最近的过程明细(只含表面事件, 内部流式事件全部滤除): user/assistant 消息文本、tool/call 与 tool/result 摘要、turn/start|end 边界。每条 {seq,type,text(≤2k),turn}。n = 返回条数(默认 5), sinceSeq = 只看该 seq 之后的事件, filter = all|surface|tools(surface = 消息文本与 turn 边界, tools = 工具调用与结果)。空文本的 assistant 消息直接跳过(不再出现 "(no text blocks)")。',
    {
      sessionId: z.string().describe('会话 id(live/persisted 均可)'),
      n: z.number().int().min(1).max(200).optional().describe('最多返回多少条表面事件(默认 5)'),
      sinceSeq: z.number().int().min(0).optional().describe('只返回 seq 大于该值的事件'),
      filter: z.enum(['all', 'surface', 'tools']).optional().describe('事件类别过滤(默认 all)'),
    },
    async ({ sessionId, n, sinceSeq, filter }) => {
      const count = n ?? 5
      const mode = filter ?? 'all'
      const agent = liveAgentFor(ctx, sessionId) as Agent | undefined
      let events: readonly unknown[]
      let source: 'live' | 'persisted'
      let total: number
      if (agent !== undefined) {
        events = (agent.session as unknown as { log?: readonly unknown[] }).log ?? []
        source = 'live'
        total = events.length
      } else {
        const cold = await readColdLog(ctx, sessionId, Math.max(2000, count * 50))
        if (cold === undefined) {
          return err(JSON.stringify({ error: `session not found: ${sessionId} (not live in this process and not persisted)` }))
        }
        events = cold.events
        source = 'persisted'
        total = cold.total
      }
      const types = mode === 'tools'
        ? new Set(['tool/call', 'tool/result'])
        : mode === 'surface'
          ? new Set(['user/message', 'assistant/message', 'turn/start', 'turn/end'])
          : new Set(['user/message', 'assistant/message', 'tool/call', 'tool/result', 'turn/start', 'turn/end'])
      const rows: Array<{ seq?: number; type: string; text: string; turn?: number }> = []
      let currentTurn: number | undefined
      for (const raw of events) {
        const e = raw as EventView
        const type = e.type
        if (type === undefined) continue
        if (type === 'turn/start') currentTurn = Number((e.data as { turn?: unknown } | undefined)?.turn ?? 0)
        if (!types.has(type)) continue
        if (sinceSeq !== undefined && typeof e.seq === 'number' && e.seq <= sinceSeq) continue
        let text = ''
        if (type === 'user/message' || type === 'assistant/message') {
          text = assistantTextOf(e)
          if (text === '') continue // 空文本消息不返回(修 '(no text blocks)' 噪声)
        } else if (type === 'tool/call') {
          const d = e.data as { name?: string; arguments?: unknown; input?: unknown } | undefined
          const args = d?.arguments !== undefined ? String(d.arguments) : JSON.stringify(d?.input ?? null) ?? ''
          text = `${d?.name ?? '?'}(${args.slice(0, 2000)})`
        } else if (type === 'tool/result') {
          const texts: string[] = []
          extractText(e.data ?? raw, texts)
          text = texts.join('\n').slice(0, 2000) || '(empty result)'
        } else if (type === 'turn/start') {
          text = `turn ${currentTurn ?? 0} start`
        } else if (type === 'turn/end') {
          const d = e.data as { turn?: unknown; reason?: { kind?: unknown } } | undefined
          text = `turn ${Number(d?.turn ?? currentTurn ?? 0)} end (${String(d?.reason?.kind ?? 'unknown')})`
        }
        rows.push({
          ...(typeof e.seq === 'number' ? { seq: e.seq } : {}),
          type,
          text: text.slice(0, 2000),
          ...(currentTurn !== undefined ? { turn: currentTurn } : {}),
        })
      }
      const tail = rows.slice(Math.max(0, rows.length - count))
      return out(JSON.stringify({
        sessionId, source, logEvents: total, surfaceEvents: rows.length, returned: tail.length,
        ...(sinceSeq !== undefined ? { sinceSeq } : {}),
        ...(filter !== undefined ? { filter: mode } : {}),
        events: tail,
      }, null, 2))
    },
  )

  // 【可选阻塞】单段 ≤240s 等 turn-end / idle / input; 非 live 立即返回 session_status; 超时返回 {timeout:true, status}
  mcp.tool(
    'session_wait',
    '可选地阻塞等待一段(单段 ≤240s): until=turn-end(等指定/当前 turn 结束) | idle(等会话彻底空闲) | input(等出现待响应弹窗)。事件驱动 + 500ms 兜底轮询; 超时返回 {timeout:true, status}, 落定返回 {timeout:false, status}。非 live 会话无法等待(进程已不在), 立即返回 {live:false, status}(用 session_status 冷读)。',
    {
      sessionId: z.string().describe('会话 id'),
      until: z.enum(['turn-end', 'idle', 'input']).optional().describe('等待目标(默认 turn-end)'),
      timeoutMs: z.number().int().min(0).max(MAX_WAIT_MS).optional().describe('等待上限毫秒数(默认 60000, 上限 240000)'),
      sinceTurn: z.number().int().min(1).optional().describe('只等该 turn 号(含)之后的 turn 结束; 缺省 = 当前在飞 turn, 没有在飞 turn 时 = 下一个 turn'),
    },
    async ({ sessionId, until, timeoutMs, sinceTurn }) => {
      const target = until ?? 'turn-end'
      const cap = Math.min(timeoutMs ?? 60000, MAX_WAIT_MS)
      const startedAt = Date.now()
      const agent = liveAgentFor(ctx, sessionId) as Agent | undefined
      if (agent === undefined) {
        const status = await buildSessionStatus(ctx, sessionId)
        if (status === undefined) {
          return err(JSON.stringify({ error: `session not found: ${sessionId} (not live in this process and not persisted)` }))
        }
        return out(JSON.stringify({
          sessionId, live: false, until: target, timeout: false, waitedMs: 0,
          reason: 'session is not live in this process (after a restart nothing is running); returning the current status instead of blocking',
          status,
        }, null, 2))
      }
      const start = liveTurnState(ctx, agent.session)
      const minTurn = sinceTurn ?? (start.openTurn !== null ? start.openTurn.turn : (start.lastTurn?.turn ?? 0) + 1)
      const settled = await waitForSettle(ctx, sessionId, agent, target, cap, minTurn)
      const status = await buildSessionStatus(ctx, sessionId)
      return out(JSON.stringify({
        sessionId, live: true, until: target, timeout: !settled, waitedMs: Date.now() - startedAt,
        ...(settled ? {} : { note: `not settled within ${cap}ms; poll session_status again or retry with a larger timeoutMs (max ${MAX_WAIT_MS})` }),
        ...(status !== undefined ? { status } : {}),
      }, null, 2))
    },
  )

  // 打断会话当前回合(替代旧 task_cancel): 走 agent.cancel, turn/end reason.kind='aborted'
  mcp.tool(
    'session_cancel',
    "打断会话当前的 turn(agent.cancel {kind:'hook',reason:'harness-mcp-cancel'}): 中止活动回合并(缺省)清空未开始的排队输入, 回落 turn/end reason.kind='aborted'。keepInbox=true 保留排队输入。非 live(重启后)会话无可打断, 返回 noop —— 用 session_status 冷读其状态。",
    {
      sessionId: z.string().describe('会话 id'),
      keepInbox: z.boolean().optional().describe('true = 保留未开始的排队输入(缺省清空)'),
    },
    async ({ sessionId, keepInbox }) => {
      const agent = liveAgentFor(ctx, sessionId) as Agent | undefined
      if (agent === undefined) {
        return out(JSON.stringify({
          sessionId, live: false, cancelled: false,
          note: 'session is not live in this process; nothing to cancel (its log is the source of truth — query session_status)',
        }))
      }
      try {
        agent.cancel({ kind: 'hook', reason: 'harness-mcp-cancel' }, keepInbox === undefined ? undefined : { keepInbox })
      } catch (e) {
        return err(JSON.stringify({ error: `cancel failed: ${(e as Error)?.message ?? String(e)}` }))
      }
      // 已主动打断: 该会话不再计 MCP 在飞 turn(随后的 turn/end 事件到达时计数已为 0, 不受影响)
      unmarkMcpTurn(sessionId)
      const state = liveTurnState(ctx, agent.session)
      return out(JSON.stringify({
        sessionId, live: true, cancelled: true, keepInbox: keepInbox === true,
        openTurn: state.openTurn,
        note: "agent cancelled; the turn closes with turn/end reason.kind='aborted'",
      }, null, 2))
    },
  )

  // 给已有会话改名(走 sessionTitle 服务, 便于会话列表归档)
  mcp.tool(
    'rename_session',
    '给已有会话改名(走 sessionTitle 服务的 rename), 便于会话列表归档区分。',
    {
      sessionId: z.string().describe('要改名的会话 id(来自 session_send 结果或 session_list)'),
      title: z.string().describe('新标题'),
    },
    async ({ sessionId, title }) => {
      try {
        const sessions = ctx.get('sessions') as { get?: (id: string) => unknown } | undefined
        const session = sessions?.get?.(sessionId)
        if (!session) return err(JSON.stringify({ error: `session not found: ${sessionId}` }))
        const st = ctx.get('sessionTitle') as { rename?: (s: unknown, t: string) => unknown } | undefined
        if (!st?.rename) return err(JSON.stringify({ error: 'sessionTitle service unavailable' }))
        const snapshot = st.rename(session, title) as { title?: string } | undefined
        return out(JSON.stringify({ ok: true, sessionId, title: snapshot?.title ?? title }))
      } catch (e) {
        return err(JSON.stringify({ error: String(e) }))
      }
    },
  )

  // 会话清单: 让外部客户端看清可续接的会话及其上下文占用, 决定续接哪个 sessionId / 是否开新会话 / 是否压缩
  mcp.tool(
    'session_list',
    '列出可续接的会话(常驻池 / live / 持久化三层去重, 池优先), 含上下文占用 events/tokens/pressure/window/ratio(经 tokenMeter 测量 + llm 模型窗口)与生效模式 mode(preset/sandbox/approval; 池/live 行从会话日志折出, 持久化行仅 header 已知 preset); 持久化层未加载日志为 null。',
    {},
    async () => {
      const rows = new Map<string, {
        cwd?: string; source: 'pool' | 'live' | 'persisted'; title?: string
        context: ContextUsage | null
        mode?: { preset: string; sandbox: string; approval: string; permissionPreset?: string } | null
      }>()
      const titleSvc = ctx.get('sessionTitle') as SessionTitleView | undefined
      // 常驻池(本插件持有, 优先级最高; 上下文可直接测量; 标题直接从服务快照读, 不依赖 live 列表兜底)
      for (const [cwd, rec] of liveAgents) {
        const sid = String(rec.sessionId)
        const agent = rec.handle.agent
        rows.set(sid, {
          cwd, source: 'pool',
          title: titleSvc?.get?.(agent.session)?.title,
          context: await contextUsage(ctx, agent.session, agent),
          mode: sessionModeOf(ctx, sid, agent.session, (agent.session as { header?: { agentPreset?: string } }).header),
        })
      }
      // live 会话(ctx.agents.list(); 可读标题)
      const liveAgentsList = (ctx.agents as unknown as { list?: () => { session: { id: unknown; header?: { cwd?: string; agentPreset?: string } }; options?: { provider?: string; model?: string } }[] }).list?.() ?? []
      for (const agent of liveAgentsList) {
        const id = String(agent.session.id)
        const prev = rows.get(id)
        rows.set(id, {
          cwd: agent.session.header?.cwd ?? prev?.cwd,
          source: prev?.source ?? 'live',
          title: prev?.title ?? titleSvc?.get?.(agent.session)?.title,
          context: prev?.context ?? await contextUsage(ctx, agent.session, agent),
          mode: prev?.mode ?? sessionModeOf(ctx, id, agent.session, agent.session.header),
        })
      }
      // 持久化(未在上两层出现的会话; 日志未加载, 上下文未知; 仅 header 已知 preset)
      const persistence = ctx.get('sessionPersistence') as { list?: () => Promise<{ id: unknown; cwd?: string; agentPreset?: string }[]> } | undefined
      for (const h of (await persistence?.list?.()) ?? []) {
        const id = String(h.id)
        if (!rows.has(id)) {
          const preset = h.agentPreset ?? runtimeConfig.preset
          rows.set(id, { cwd: h.cwd, source: 'persisted', context: null, mode: { preset, sandbox: '', approval: '' } })
        }
      }
      const sessions = [...rows.entries()]
        .map(([sessionId, info]) => ({ sessionId, ...info }))
        .sort((a, b) => (a.cwd ?? '').localeCompare(b.cwd ?? ''))
      return out(JSON.stringify({ total: sessions.length, sessions }, null, 2))
    },
  )

  // 上下文压缩: 把会话早期历史压成一段模型摘要(走官方 ctx.compaction.compactNow; 需宿主加载 compaction 后端)
  mcp.tool(
    'session_compact',
    '把会话的早期历史压缩成一段模型摘要(走 ctx.compaction 的 compactNow; 需宿主已加载 compaction 后端如 dsh-compaction-basic)。压缩后上下文占用大幅下降, 被替换的细节仍保留在持久化日志里。会话忙碌(正在跑任务)时返回 busy 错误。',
    {
      sessionId: z.string().describe('要压缩的会话 id(池/live/持久化均可; 非 live 会临时 resume, 压缩后释放)'),
    },
    async ({ sessionId }) => {
      const engine = ctx.get('compaction') as { compactNow?: (agent: unknown, signal: AbortSignal) => Promise<unknown> } | undefined
      if (!engine?.compactNow) {
        return err(JSON.stringify({ error: 'compaction service unavailable (is dsh-compaction-basic loaded?)' }))
      }
      // 三级解析会话(池 → live → 持久化 resume); resume 出的句柄在结束后 flush+dispose
      let resolved: ResolvedAgent
      try {
        resolved = await getAgent(ctx, '', sessionId)
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      const agent = resolved.handle.agent
      const agentCtx = {
        session: agent.session,
        options: { provider: runtimeConfig.provider, ...(runtimeConfig.model ? { model: runtimeConfig.model } : {}) },
        runMaintenance: <T,>(task: (signal: AbortSignal) => Promise<T>) => agent.runMaintenance(task),
      }
      const before = await contextUsage(ctx, agent.session, agent)
      const controller = new AbortController()
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        const result = await (runtimeConfig.taskTimeoutMs > 0
          ? Promise.race([
              engine.compactNow(agentCtx as never, controller.signal),
              new Promise<never>((_resolve, reject) => {
                timer = setTimeout(() => { controller.abort(); reject(TASK_TIMEOUT) }, runtimeConfig.taskTimeoutMs)
              }),
            ])
          : engine.compactNow(agentCtx as never, controller.signal))
        const r = result as {
          compactionId?: string; summarySeq?: number; endSeq?: number
          shadowedSeqs?: number[]; shadowedTokenCount?: number
          summary?: { type?: string; text?: string }[]
        }
        const summaryText = (r.summary ?? []).filter((b) => b.type === 'text' && b.text).map((b) => b.text).join('\n').slice(0, 2000)
        const after = await contextUsage(ctx, agent.session, agent)
        return out(JSON.stringify({
          ok: true, sessionId,
          compactionId: r.compactionId,
          summarySeq: r.summarySeq, endSeq: r.endSeq,
          shadowedNodes: r.shadowedSeqs?.length ?? 0,
          shadowedTokens: r.shadowedTokenCount,
          before, after,
          summary: summaryText,
        }, null, 2))
      } catch (e) {
        if (e === TASK_TIMEOUT) {
          return err(JSON.stringify({ error: `compaction timed out after ${runtimeConfig.taskTimeoutMs}ms` }))
        }
        const err2 = e as { name?: string; code?: string; message?: string }
        return err(JSON.stringify({
          error: `compact failed${err2.code ? ` (${err2.code})` : ''}: ${err2?.message ?? String(e)}`,
          busy: err2.name === 'ManualCompactionError' && err2.code === 'busy',
        }))
      } finally {
        if (timer !== undefined) clearTimeout(timer)
        if (resolved.disposeAfter) {
          try { await (ctx.get('sessions') as { flush?: (s: unknown) => Promise<unknown> } | undefined)?.flush?.(agent.session) } catch { /* ignore */ }
          try { await resolved.handle.dispose() } catch { /* ignore */ }
        }
      }
    },
  )

  // 感知: 列出等待输入的弹窗(审批/提问)。审批一律转达调用方, 用 prompt_respond 响应; 未响应前任务挂起。
  mcp.tool(
    'pending_prompts',
    '列出当前等待输入的弹窗(审批/提问)。审批=权限审批待决策(approve/deny); 提问=agent 的澄清问题(自由文本回答)。可用 prompt_respond 响应; 未响应前任务保持挂起。',
    {
      sessionId: z.string().optional().describe('只列出该会话的弹窗(缺省: 全部 MCP 会话)'),
    },
    async ({ sessionId }) => {
      const prompts: Record<string, unknown>[] = []
      for (const pa of pendingApprovals.values()) {
        if (!sessionId || pa.agentId === sessionId) {
          prompts.push({ sessionId: pa.agentId, type: 'approval', id: pa.promptId, toolName: pa.toolName, ...(pa.reason !== undefined ? { reason: pa.reason } : {}) })
        }
      }
      for (const pq of pendingQuestions.values()) {
        if (!sessionId || pq.agentId === sessionId) prompts.push({ sessionId: pq.agentId, type: 'question', id: pq.promptId, questions: pq.questions })
      }
      // 本插件未接管提问时(questionsProviderOurs=false), MCP 会话里挂起的 ask_user_question 仍可感知(应答在 GUI)
      if (!questionsProviderOurs) {
        for (const sid of (sessionId ? [sessionId] : [...mcpSessionIds])) {
          const agent = liveAgentFor(ctx, sid)
          const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined
          if (detected) {
            prompts.push({ sessionId: sid, type: 'question', id: detected.id, questions: detected.questions, note: 'not claimed by MCP (MCP only claims questions asked while it drives the session); answer it in the DSH web UI' })
          }
        }
      }
      return out(JSON.stringify({ total: prompts.length, prompts }, null, 2))
    },
  )

  // 响应: 解除等待中的弹窗。审批 approve→allowed-once(一次性授权) / deny→rejected; 提问→自由文本回答。
  mcp.tool(
    'prompt_respond',
    '响应等待中的弹窗: 审批用 decision=approve|deny(approve 为一次性授权, 绝不自动放行——每次审批都必须显式决策); 提问用 answer 自由文本。响应后 agent 解除阻塞继续执行。',
    {
      sessionId: z.string().describe('弹窗所属会话 id'),
      promptId: z.string().describe('pending_prompts / session_status 返回的 prompt id'),
      decision: z.enum(['approve', 'deny']).optional().describe('审批类弹窗的决策(approve=放行一次, deny=拒绝)'),
      answer: z.string().optional().describe('提问类弹窗的自由文本回答'),
    },
    async ({ sessionId, promptId, decision, answer }) => {
      const pa = pendingApprovals.get(promptId)
      if (pa !== undefined) {
        if (pa.agentId !== sessionId) return err(JSON.stringify({ error: `prompt ${promptId} belongs to session ${pa.agentId}, not ${sessionId}` }))
        if (decision !== 'approve' && decision !== 'deny') {
          return err(JSON.stringify({ error: 'approval prompts require decision=approve|deny' }))
        }
        const outcome = decision === 'approve' ? 'allowed-once' : 'rejected'
        pa.resolve(outcome)
        return out(JSON.stringify({ ok: true, promptId, type: 'approval', resolved: outcome }, null, 2))
      }
      const pq = pendingQuestions.get(promptId)
      if (pq !== undefined) {
        if (pq.agentId !== sessionId) return err(JSON.stringify({ error: `prompt ${promptId} belongs to session ${pq.agentId}, not ${sessionId}` }))
        if (answer === undefined || answer === '') return err(JSON.stringify({ error: 'question prompts require answer text' }))
        const answered = pq.questions.map((q) => ({ id: q.id, selected: [], custom: answer }))
        pq.resolve({ answers: answered })
        return out(JSON.stringify({ ok: true, promptId, type: 'question', answered: answered.length }, null, 2))
      }
      // 未挂起: 若是 MCP 未接管、由 web GUI 应答链处理的挂起提问, 给出明确指引
      const agent = liveAgentFor(ctx, sessionId)
      const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined
      if (detected !== undefined && detected.id === promptId && !questionsProviderOurs) {
        return err(JSON.stringify({ error: 'this question was not claimed by MCP (MCP only claims questions asked while it drives the session); answer it in the DSH web UI' }))
      }
      return err(JSON.stringify({ error: `prompt not found: ${promptId}` }))
    },
  )

  // 切换会话模型: 改 agent.options.model(agent-loop 每轮 buildRequest 实时读取), 下个 turn 生效;
  // 持久化会话临时 resume 并记录会话级覆盖, 后续 resume 同样生效
  mcp.tool(
    'session_set_model',
    '给指定会话切换模型(改 agent.options.model, 下一个 turn 生效, 不打断当前执行)。池/live 直改; 持久化会话临时 resume 并记录覆盖(之后 resume 仍生效)。模型 id 参考 model_list(deepseek-v4-flash/pro、glm-5.3/flash 等)。',
    {
      sessionId: z.string().describe('会话 id(池/live/持久化均可)'),
      model: z.string().describe('目标模型 id'),
      provider: z.string().optional().describe('目标 provider 路由(缺省保持当前)'),
    },
    async ({ sessionId, model, provider }) => {
      let resolved: ResolvedAgent
      try {
        resolved = await getAgent(ctx, '', sessionId)
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      const agent = resolved.handle.agent
      const opts = (agent as unknown as { options?: { provider?: string; model?: string } }).options
      const old = { provider: opts?.provider, model: opts?.model }
      if (opts) {
        if (provider !== undefined) opts.provider = provider
        opts.model = model
      }
      sessionModelOverrides.set(sessionId, { provider: provider ?? old.provider, model })
      if (resolved.disposeAfter) {
        try { await (ctx.get('sessions') as { flush?: (s: unknown) => Promise<unknown> } | undefined)?.flush?.(agent.session) } catch { /* ignore */ }
        try { await resolved.handle.dispose() } catch { /* ignore */ }
      }
      return out(JSON.stringify({
        ok: true, sessionId,
        oldModel: old.model ?? '(unset)', newModel: model,
        oldProvider: old.provider ?? '(unset)', newProvider: provider ?? old.provider ?? '(unset)',
        note: 'takes effect from the next turn; recorded as session override for future resumes',
      }, null, 2))
    },
  )

  // 向运行中会话插入补充指令(steering): 走 DSH agent.inbox(append, 持久化), 下个 turn/step 边界读取, 不打断当前工具
  mcp.tool(
    'session_inject',
    '向指定会话的 agent 队列插入一条补充指令/上下文(steering 消息): 下个 turn 边界处理, 不打断当前正在执行的工具(参考 DSH agent.inbox / agent/inbox/spliced)。正在执行的任务会在下一步读到; 空闲会话的消息排队等待下个任务。',
    {
      sessionId: z.string().describe('会话 id(池/live/持久化均可)'),
      message: z.string().describe('要插入的补充指令/上下文文本'),
      target: z.enum(['next-turn', 'next-step']).optional().describe('插入位置(默认 next-turn = 队尾)'),
    },
    async ({ sessionId, message, target }) => {
      let resolved: ResolvedAgent
      try {
        resolved = await getAgent(ctx, '', sessionId)
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      const agent = resolved.handle.agent
      const inbox = (agent as unknown as { inbox?: { append?: (t: 'next-turn' | 'next-step', m: unknown) => void } }).inbox
      if (!inbox?.append) {
        if (resolved.disposeAfter) { try { await resolved.handle.dispose() } catch { /* ignore */ } }
        return err(JSON.stringify({ error: 'agent inbox unavailable' }))
      }
      try {
        const msg = createUserMessage({ content: [{ type: 'text', text: message }], source: { kind: 'plugin', plugin: 'harness-mcp-server' } })
        inbox.append(target ?? 'next-turn', msg as never)
      } catch (e) {
        if (resolved.disposeAfter) { try { await resolved.handle.dispose() } catch { /* ignore */ } }
        return err(JSON.stringify({ error: `inject failed: ${(e as Error)?.message ?? String(e)}` }))
      }
      if (resolved.disposeAfter) {
        try { await (ctx.get('sessions') as { flush?: (s: unknown) => Promise<unknown> } | undefined)?.flush?.(agent.session) } catch { /* ignore */ }
        try { await resolved.handle.dispose() } catch { /* ignore */ }
      }
      return out(JSON.stringify({
        ok: true, sessionId, target: target ?? 'next-turn',
        note: 'queued; processed at the next turn/step boundary without interrupting the current tool',
      }, null, 2))
    },
  )

  // 手动归组补给站: 官方 UI 没有"移动会话到工作区"功能, 本工具供随时归组
  mcp.tool(
    'attach_session',
    '把会话归组到工作区(补给站: 官方 UI 无移动会话功能)。path 缺省用该会话 header 的 cwd; 归组依赖官方 attachSession 的强校验——realpath(header.cwd) 必须与工作区路径精确相等, 不匹配会返回官方报错。',
    {
      sessionId: z.string().describe('要归组的会话 id(live 或已持久化)'),
      path: z.string().optional().describe('目标工作区目录(缺省: 会话 header 的 cwd)'),
    },
    async ({ sessionId, path }) => {
      const sid = SessionId(sessionId)
      const header = await findSessionHeader(ctx, sid)
      if (header === undefined) {
        return err(JSON.stringify({ error: `session not found: ${sessionId}(live 与持久化里都没有)` }))
      }
      const target = path ?? header.cwd
      if (target === undefined) {
        return err(JSON.stringify({ error: `session ${sessionId} 的 header 没有 cwd, 官方 attachSession 无法校验, 不能归组` }))
      }
      try {
        const canonical = await realpath(target) // 目标必须是存在的目录, 否则 ENOENT
        // 白名单一致化: 配置了 workspaceRoots 时, 归组目标同样受目录白名单约束
        if (!cwdAllowed(canonical)) {
          return err(JSON.stringify({ error: `path not allowed (outside workspaceRoots): ${canonical}` }))
        }
        const ws = await ensureWorkspace(ctx, canonical)
        if (!ws?.attachSession) return err(JSON.stringify({ error: 'workspaceRegistry unavailable' }))
        if (ws.sessionIds.includes(sid)) {
          return out(JSON.stringify({ sessionId, workspaceId: ws.id, workspacePath: ws.path, attached: false, note: 'already attached' }))
        }
        await ws.attachSession(sid)
        return out(JSON.stringify({ sessionId, workspaceId: ws.id, workspacePath: ws.path, attached: true }))
      } catch (e) {
        return err(JSON.stringify({ error: `attach failed: ${(e as Error)?.message ?? String(e)}` }))
      }
    },
  )
}

/**
 * 插件入口: 启动 MCP server(StreamableHTTP, 跨网), 通过 ctx 桥接 Harness 能力。
 */
export async function apply(ctx: Context, config: Config = {}): Promise<void> {
  // 初始化运行时配置(覆盖默认值)
  if (config.provider) runtimeConfig.provider = config.provider
  if (config.model) runtimeConfig.model = config.model
  if (config.preset) runtimeConfig.preset = config.preset
  if (config.maxAgents !== undefined) runtimeConfig.maxAgents = config.maxAgents
  if (config.taskTimeoutMs !== undefined) runtimeConfig.taskTimeoutMs = config.taskTimeoutMs
  if (config.authToken) runtimeConfig.authToken = config.authToken
  if (config.authTokens?.length) runtimeConfig.authTokens = [...config.authTokens]
  if (config.workspaceRoots) runtimeConfig.workspaceRoots = config.workspaceRoots

  // 生效监听配置: 入口 config 引导; 有 settings 服务时被用户层(设置界面)覆盖, 见下方注册段
  let effectiveHost = config.host ?? '127.0.0.1'
  let effectivePort = config.port ?? 8090
  // 安全默认: 仅监听本机。暴露公网/局域网前必须自行加认证+反代+TLS(见 README 警告)
  console.log('[harness-mcp-server] apply called, port=', effectivePort)

  // ── 审批应答者: 正被 MCP 投喂驱动的会话, 审批一律转达调用方, 绝不自动放行 ──
  // prepend 抢在 web GUI 应答者之前认领审批; 仅当会话属 MCP(mcpSessionIds)且此刻正被 MCP 投喂驱动
  // (mcpPendingTurns: 当前有 session_send 投喂的 turn 尚未落定)才接管转达调用方(Hermes);
  // 否则(例如用户经 web UI 直接向 MCP 创建过的会话发消息、此刻没有 MCP 任务在跑)不接管,
  // next() 交给 web GUI 应答链 —— 避免接管后 Hermes 调用方不知情/无法响应、web UI 也弹不出
  // 审批窗的双端死锁(旧版只看 mcpSessionIds 的死锁根因)。
  // approve → 'allowed-once'(一次性授权), deny → 'rejected', 任务取消/超时 → signal abort → 'cancelled'。
  const onApprovalRequest = (req: ApprovalRequestView, next: () => Promise<string>): Promise<string> => {
    if (req.signal?.aborted) return Promise.resolve('cancelled')
    // 防御: agent.id 为权威; 个别实现只挂 session.id 时兜底
    const agentId = String(req.agent.id ?? (req.agent.session as { id?: unknown } | undefined)?.id)
    // 仅当会话属 MCP 且有 MCP 在飞 turn 才接管转达调用方; 否则交给 web GUI 应答链
    if (!mcpSessionIds.has(agentId) || !mcpTurnInFlight(agentId)) return next()
    const promptId = approvalPromptIdOf(req)
    return new Promise<string>((resolve) => {
      let settled = false
      const settle = (outcome: ApprovalOutcomeValue) => {
        if (settled) return
        settled = true
        pendingApprovals.delete(promptId)
        req.signal?.removeEventListener('abort', onAbort)
        resolve(outcome)
        // 【2】web UI 提示(入队, 工具完成后安全落点): 弹窗已被 MCP 响应
        // 文案按系统/状态提示撰写(带 ✅ 与明确「审批已由 MCP 响应」措辞), 见 noticeUserMessage 的呈现说明
        queuePromptNotice(req.agent, `✅ 审批 ${promptId} 已由 MCP 侧响应: ${outcome}`, `✅ 审批已由 MCP 响应：${outcome}`)
      }
      const onAbort = () => settle('cancelled')
      pendingApprovals.set(promptId, { promptId, agentId, toolName: req.toolName, reason: req.reason, resolve: settle })
      req.signal?.addEventListener('abort', onAbort, { once: true })
      // 【2+3】web UI 提示: 该审批已被 MCP 拦截接管。⏳ 接管提示走挂起期即时投递
      // (notifyPromptIntercepted: next-step inbox 即时追加, web UI 挂起期立刻可见);
      // 不能在拦截期直接写 user/message(恒处于 tool_calls/tool_result 窗口, 0.9.4 回归),
      // 也不入队 pendingNotices(那要等响应后的 post-execute 才落地, 挂起期无提示)。
      notifyPromptIntercepted(
        req.agent,
        `⏳ 审批已由 MCP 接管（${req.toolName}${req.reason !== undefined ? `：${req.reason}` : ''}），等待 Hermes/客户端响应（prompt ${promptId}）`,
        `⏳ 审批已由 MCP 接管：${req.toolName}`,
      )
    })
  }
  ;(ctx.on as unknown as (name: string, listener: unknown, options?: { prepend?: boolean }) => unknown)(
    'approval/request',
    onApprovalRequest,
    { prepend: true },
  )

  // ── notice 安全投递: 响应类提示(✅/❌)与即时投递兜底只入队, 工具完成( tools/post-execute )后并入 additionalContexts ──
  // agent-loop 在 appendToolResult 之后把 additionalContexts splice 进 next-step inbox,
  // 下个 step 开始时才追加为 user/message —— 从不在 assistant(tool_calls) 与其 tool/result 之间
  // 插入消息(修复 0.9.4 起 notice 打断消息序列导致 INVALID_REQUEST 的回归)。
  ;(ctx.on as unknown as (name: string, listener: unknown, options?: { prepend?: boolean }) => unknown)(
    'tools/post-execute',
    async (exec: unknown, _result: unknown, next: () => Promise<{ kind?: string; additionalContexts?: unknown[] }>) => {
      const downstream = await next()
      return flushPromptNotices((exec as { agent?: unknown }).agent, downstream)
    },
  )

  // ── 提问应答者: 把挂起的 ask_user_question 交接给 MCP 调用方(用 prompt_respond 应答) ──
  // dsh-user-questions 有两代宿主形态, 用 API 探测兼容(两版宿主都要能跑):
  //   ① ≤0.1.1(旧版): UserQuestionService 有单槽 registerProvider(provider) —— 先到先得, 注册成功即全量接管;
  //   ② ≥0.1.5(新版): registerProvider 被删除(2026-09 升级), 改为 Agent 作用域 waterfall 事件
  //      'user-questions/request'。web GUI 客户端以 ctx.remote.$on 挂在同一事件上, waterfall 语义
  //      outermost-first、不调 next() 即 veto 后续 listener —— 故用 prepend 抢先认领(与 approval/request 同款)。
  // ⚠️ 只试 ①(旧代码)正是本 bug 的根因: 0.1.5 上 registerProvider 为 undefined → 静默跳过注册 →
  //    questionsProviderOurs=false → 提问全落到 GUI listener → prompt_respond 走「未挂起」拒绝分支。
  interface UserQuestionRequestView {
    questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }>
    agent?: { id: unknown; session?: unknown }
    signal?: AbortSignal
  }

  /** 提问交接实现(两版宿主共用): 生成 promptId → 挂 pendingQuestions → 等 prompt_respond / abort 落定。
   *  返回 AskUserQuestionAnswer = { answers: [{ id, selected, custom }] } ——
   *  已对照 0.1.5 lib/types/types.d.ts 确认答案形态与 0.1.1 一致(selected 可为空数组, 自由文本走 custom)。 */
  const answerQuestionViaMcp = (request: unknown): Promise<{ answers: Array<{ id: string; selected: string[]; custom?: string }> }> => {
    const r = request as UserQuestionRequestView
    const promptId = `q-${randomUUID()}`
    return new Promise((resolve, reject) => {
      let settled = false
      const settle = (fn: () => void) => {
        if (settled) return
        settled = true
        pendingQuestions.delete(promptId)
        r.signal?.removeEventListener('abort', onAbort)
        fn()
      }
      const onAbort = () => settle(() => reject(new Error('ask_user_question was aborted before the user answered')))
      pendingQuestions.set(promptId, {
        promptId,
        agentId: r.agent !== undefined ? String(r.agent.id) : '(host)',
        questions: r.questions.map((q) => ({
          id: q.id,
          question: q.question,
          ...(q.detail !== undefined ? { detail: q.detail } : {}),
          ...(q.options !== undefined ? { options: (q.options ?? []).map((o) => ({ label: o.label })) } : {}),
        })),
        resolve: (answer) => settle(() => {
          // 【2】web UI 提示(入队, 工具完成后安全落点): 提问已由 MCP 响应
          if (r.agent !== undefined) queuePromptNotice(r.agent, `✅ 提问 ${promptId} 已由 MCP 侧回答`, '✅ 提问已由 MCP 回答')
          resolve(answer)
        }),
        reject: (e) => settle(() => {
          if (r.agent !== undefined) queuePromptNotice(r.agent, `❌ 提问 ${promptId} 已取消/失败: ${(e as Error)?.message ?? String(e)}`, '❌ 提问已取消/失败')
          reject(e)
        }),
      })
      r.signal?.addEventListener('abort', onAbort, { once: true })
      // 【2+3】web UI 提示: 该提问已被 MCP 拦截接管 —— ⏳ 走挂起期即时投递
      // (notifyPromptIntercepted, 同审批; 拦截期不可直接写 user/message, 见该函数注释)
      if (r.agent !== undefined) {
        const first = r.questions[0]
        notifyPromptIntercepted(r.agent, `⏳ 提问已由 MCP 接管（${first?.question ?? '…'}），等待 Hermes/客户端响应（prompt ${promptId}）`, '⏳ 提问已由 MCP 接管')
      }
    })
  }

  /** 提问接管判据(与 onApprovalRequest 同判据): 会话属 MCP 且此刻正被 MCP 任务驱动才认领,
   *  否则 next() 透传给 web GUI 应答链 —— 用户经 web UI 直接向「MCP 用过的会话」发消息时, 提问照常
   *  弹在 GUI, 不会 MCP/GUI 两端都收不到而挂死(审批应答者 0.9.10 已按同一判据收紧, 提问沿用同闸)。
   *  【放宽】若要复刻旧版 registerProvider 的「无条件全量接管」, 让本函数恒返回 true 即可; 但那样
   *  web GUI 自己发起的提问也会被 MCP 抢走而无人应答(GUI 不再渲染问题卡), 故默认不做。 */
  const mcpOwnsQuestion = (agentId: string | undefined): boolean =>
    agentId !== undefined && mcpSessionIds.has(agentId) && mcpTurnInFlight(agentId)

  const userQuestions = ctx.get('userQuestions') as {
    registerProvider?: (p: { ask: (request: unknown) => Promise<unknown> }) => () => void
  } | undefined

  let questionAnswererMode: 'legacy-provider' | 'waterfall-listener' | 'none' = 'none'
  const legacyRegisterProvider = userQuestions?.registerProvider
  if (typeof legacyRegisterProvider === 'function') {
    // ① 旧版宿主(≤0.1.1): 单槽 provider。槽被 GUI 占先时抛 DUPLICATE_PROVIDER → 保持不接管(旧行为不变)
    try {
      legacyRegisterProvider.call(userQuestions, { ask: (request) => answerQuestionViaMcp(request) })
      questionAnswererMode = 'legacy-provider'
    } catch {
      questionAnswererMode = 'none'
    }
  } else {
    // ② 新版宿主(≥0.1.5): 注册到 waterfall, prepend 抢在 web GUI 的 remote listener 之前。
    //    listener 签名 (request, next) —— 认领即返回答案(不调 next, veto 后续应答者);
    //    不认领(非 MCP 会话/非 MCP 任务期)必须 return next() 透传给 GUI。
    try {
      ;(ctx.on as unknown as (name: string, listener: unknown, options?: { prepend?: boolean }) => unknown)(
        'user-questions/request',
        (request: unknown, next: () => Promise<unknown>): Promise<unknown> => {
          if (!mcpOwnsQuestion(agentIdOf((request as UserQuestionRequestView).agent))) return next()
          return answerQuestionViaMcp(request)
        },
        { prepend: true },
      )
      questionAnswererMode = 'waterfall-listener'
    } catch {
      questionAnswererMode = 'none'
    }
  }
  questionsProviderOurs = questionAnswererMode !== 'none'
  if (questionAnswererMode === 'legacy-provider') {
    console.log('[harness-mcp-server] user-questions answerer registered via legacy registerProvider() (dsh-user-questions ≤0.1.1); question prompts answerable via prompt_respond')
  } else if (questionAnswererMode === 'waterfall-listener') {
    console.log("[harness-mcp-server] user-questions answerer registered on the 'user-questions/request' waterfall, prepend (dsh-user-questions ≥0.1.5); question prompts answerable via prompt_respond")
  } else {
    console.warn('[harness-mcp-server] user-questions answerer not registered (legacy host, provider slot already taken); question prompts route to the web GUI answerer and remain visible via session_status/pending_prompts')
  }

  const servers = new Map<string, McpServer>()
  const transports = new Map<string, StreamableHTTPServerTransport>()

  // ── HTTP 请求处理(单一实例跨重绑复用: server.close() 后 listen 新地址) ──
  const handleHttpRequest = async (req: http.IncomingMessage, res: http.ServerResponse): Promise<void> => {
    // Bearer token 认证(配置了 authToken/authTokens 任一即强制全请求校验; 常时时间比较防时序侧信道)
    if (!bearerTokenOk(req.headers['authorization'])) {
      res.writeHead(401, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32001, message: 'Unauthorized' }, id: null }))
      return
    }
    const sessionId = (req.headers['mcp-session-id'] as string | undefined) ?? undefined
    const existing = sessionId ? transports.get(sessionId) : undefined

    // 已有 session: GET/POST/DELETE 都路由到对应 transport(支持 SSE 流 + 会话终止)
    if (existing) {
      if (req.method === 'GET' || req.method === 'POST' || req.method === 'DELETE') {
        await existing.handleRequest(req as never, res as never)
        return
      }
      res.writeHead(405, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32600, message: 'Method not allowed' }, id: null }))
      return
    }

    // 新 session 初始化(仅 POST 且无 session id)
    if (req.method === 'POST' && !sessionId) {
      const mcp = new McpServer({ name: 'harness', version: VERSION })
      registerTools(mcp, ctx)
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        onsessioninitialized: (sid) => {
          transports.set(sid, transport)
          servers.set(sid, mcp)
        },
      })
      // 会话关闭时清理映射(避免临时 key 泄漏 + 无效会话累积)
      transport.onclose = () => {
        const sid = transport.sessionId
        if (sid) {
          transports.delete(sid)
          servers.delete(sid)
        }
      }
      await mcp.connect(transport as never)
      await transport.handleRequest(req as never, res as never)
      return
    }

    // 未知 session → 404(不新建 transport, 避免遗留对象)
    if (sessionId) {
      res.writeHead(404, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32001, message: 'Session not found' }, id: null }))
      return
    }

    // 无 session 的非初始化请求 → 400
    res.writeHead(400, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32600, message: 'Invalid request' }, id: null }))
  }

  const server = http.createServer(handleHttpRequest)
  server.on('error', (e) => {
    console.error('[harness-mcp-server] HTTP server error:', e.message)
  })
  // 初始监听: 入口 config 引导值; settings 注册回调随后用解析值校正(必要时重绑)
  server.listen(effectivePort, effectiveHost, () => {
    const tokenCount = (runtimeConfig.authToken ? 1 : 0) + runtimeConfig.authTokens.length
    console.log(`[harness-mcp-server] MCP server listening on ${effectiveHost}:${effectivePort} (auth: ${tokenCount > 0 ? `on, ${tokenCount} token(s)` : 'OFF'})`)
  })

  /**
   * 按生效配置重绑监听(settings 界面改 host/port 时热生效):
   * 关旧监听 → 新 listen; transports/servers 映射不动, 已建立 MCP 会话跨重绑存活。
   * 新 listen 失败(EADDRINUSE 等)仅大声告警 —— 旧实例已关, 设置文档保留坏值待用户修正。
   */
  const rebind = (host: string, port: number): void => {
    server.close()
    server.listen(port, host, () => {
      const tokenCount = (runtimeConfig.authToken ? 1 : 0) + runtimeConfig.authTokens.length
      console.log(`[harness-mcp-server] MCP server listening on ${host}:${port} (auth: ${tokenCount > 0 ? `on, ${tokenCount} token(s)` : 'OFF'})`)
      // 非环回监听 = 暴露到局域网/公网, 未配 token 时大声告警(不阻断启动, 交给部署者决断)
      if (host !== '127.0.0.1' && host !== 'localhost' && host !== '::1' && tokenCount === 0) {
        console.warn(`[harness-mcp-server] ⚠️ WARNING: listening on ${host} (non-loopback) with NO auth token — anyone on the network can call this MCP server. Set authToken/authTokens in config or the settings UI.`)
      }
    })
  }

  /** 应用生效配置: token 集合即时生效; host/port 变化才重绑(避免无谓的监听重启)。 */
  const applyEffective = (next: { host: string; port: number; authToken: string }): void => {
    runtimeConfig.authToken = next.authToken
    const rebindNeeded = next.host !== effectiveHost || next.port !== effectivePort
    effectiveHost = next.host
    effectivePort = next.port
    if (rebindNeeded) rebind(next.host, next.port)
  }

  // ── settings 命名空间注册: web 设置面板「插件配置」卡片的宿主半区 ──
  // 入口 config 作为组合层 base(用户层未覆盖时的值); 用户经设置界面写入的值落在用户层并即时生效。
  // 无 settings 服务的部署(headless 等)回调不触发, 保持纯入口配置行为。
  if (typeof ctx.inject === 'function') {
    ;(ctx.inject as unknown as (names: string[], cb: (settingsCtx: { settings: SettingsProviderLike }) => void) => unknown)(
      ['settings'],
      (settingsCtx: { settings: SettingsProviderLike }) => {
    const scope = settingsCtx.settings.register(
      SETTINGS_NAMESPACE,
      HarnessMcpSettingsSchema,
      {
        base: {
          ...(config.host !== undefined ? { host: config.host } : {}),
          ...(config.port !== undefined ? { port: config.port } : {}),
          ...(config.authToken !== undefined ? { authToken: config.authToken } : {}),
        },
        applies: 'live',
        validate: (v) => {
          if (!v.host || v.host.trim() === '') throw new Error('host 不能为空')
          if (!Number.isInteger(v.port) || v.port < 1 || v.port > 65535) throw new Error('port 必须是 1-65535 的整数')
          if (/[\r\n]/.test(v.authToken)) throw new Error('authToken 不能包含换行')
        },
      },
    )
        applyEffective(scope.get())
        scope.watch((next) => applyEffective(next))
      },
    )
  }

  // 存量捞回: 启动后异步补挂未分组会话, 不阻塞启动; 全程兜底 try/catch 防 unhandled rejection
  void (async () => {
    try {
      const r = await reattachOrphanSessions(ctx)
      console.log(`[harness-mcp-server] 存量捞回完成: attached=${r.attached} failed=${r.failed}`)
    } catch (e) {
      console.warn('[harness-mcp-server] 存量捞回异常:', (e as Error)?.message ?? e)
    }
  })()

  // 标准 cordis 生命周期: 用 ctx.effect 注册清理(卸载时关 server + 清空全部映射/会话/队列)
  ctx.effect(() => {
    return () => {
      server.close()
      transports.clear()
      servers.clear()
      liveAgents.clear()
      sessionToCwd.clear()
      agentLocks.clear()
      for (const off of mcpTurnWatchers.values()) { try { off() } catch { /* ignore */ } }
      mcpTurnWatchers.clear()
      mcpPendingTurns.clear()
      mcpSessionIds.clear()
      sessionModelOverrides.clear()
      pendingNotices.clear()
      for (const pa of pendingApprovals.values()) pa.resolve('cancelled')
      pendingApprovals.clear()
      for (const pq of pendingQuestions.values()) pq.reject(new Error('harness-mcp-server unloaded'))
      pendingQuestions.clear()
    }
  }, 'harness-mcp-server')
}
