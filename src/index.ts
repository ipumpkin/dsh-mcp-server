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
import { randomUUID } from 'node:crypto'
import { realpath } from 'node:fs/promises'
import http from 'node:http'
import { resolve } from 'node:path'

/** Cordis 插件名 */
export const name = 'harness-mcp-server'

/** 插件版本(与 package.json 同步; MCP initialize 时上报) */
export const VERSION = "0.9.10"

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
  /** 任务队列容量上限(默认 100) */
  maxQueue?: number
  /** 已完成任务保留毫秒数(默认 60 分钟, 对齐 taskTimeoutMs, 避免异步工作流丢结果) */
  taskTtlMs?: number
  /** 常驻 agent 会话上限(默认 8, LRU 淘汰) */
  maxAgents?: number
  /** 单任务超时毫秒数, 超时自动 cancel 并回收部分输出(默认 60 分钟; 0 = 不限制) */
  taskTimeoutMs?: number
  /** Bearer token 认证(设置后所有请求必须带 Authorization: Bearer <token>) */
  authToken?: string
  /** cwd 白名单(设置后 agent 只能在列出的目录下干活) */
  workspaceRoots?: string[]
}

/** 运行时配置(apply 时从 config 初始化, 提供安全默认值) */
const runtimeConfig = {
  provider: 'deepseek-official',
  // 空字符串 = 不覆盖 model, 跟随 dsh 的用户/默认设置; 显式配置则覆盖
  model: '',
  preset: 'standard',
  maxQueue: 100,
  taskTtlMs: 60 * 60 * 1000,
  maxAgents: 8,
  taskTimeoutMs: 60 * 60 * 1000,
  authToken: '',
  workspaceRoots: [] as string[],
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
 *  这是外部客户端显式控制「是否复用会话」的入口(agent_run/task_inbox 的 newSession 参数)。
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
      // no-op dispose 兜底: executeTask 只在 disposeAfter 为 true 时调用 dispose
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
  // 旧会话正在跑任务时不 dispose(不掐任务), 仅从池摘除; 其任务结束后 agent 仍 live, 可凭 sessionId 接管或 session_close。
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
      // 这里保留检测以兼容更旧版本: 无 scope 时跳过挂载(降级为无工具 agent), 避免 agent_run 整体崩溃。
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

/** 结构化任务结果 */
interface TaskResult {
  taskId: string
  sessionId: string
  /** 会话标题: 显式 title, 或新建会话时按任务内容自动生成的名字(sessionTitle 服务缺失时缺省) */
  title?: string
  assistantText: string
  toolCalls: { name: string; args: string }[]
  toolResults: string[]
  changes: string
  verification: string
  leftovers: string
  /** true = 任务超时被自动 cancel(部分输出已回收, 可用 sessionId 续接) */
  timeout?: boolean
  /** 模型/执行失败(配额耗尽/网络/非法参数等): errorCategory = model(LLM 错误码) | execution(通用 UNKNOWN), 有值即本轮失败 */
  error?: { errorCode: string; errorMessage: string; errorCategory: 'model' | 'execution' }
  /** 任务结束瞬间所用会话的上下文占用 events/tokens/pressure/window/ratio(tokenMeter 缺失为 null;
   *  窗口不可解析时 window/ratio 为 null), 供调用方跟踪上下文占用/决定何时压缩或开新会话 */
  context?: ContextUsage | null
  /** 任务所用会话的生效模式(preset + 沙箱访问模式 + 审批策略; 本任务请求的模式已应用时 requested 与 effective 一致) */
  mode?: {
    preset: string
    sandbox: string
    approval: string
    permissionPreset?: string
    requested?: { preset?: string; sandbox?: string; approval?: string }
  }
}

/** 超时哨兵: 区分「超时打断」与 executeTask 内部的真实异常 */
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

/** 分字段限长, 保证返回的永远是完整合法 JSON(避免 slice(-16000) 截断开头导致非法 JSON) */
function truncateResult(result: TaskResult): TaskResult {
  return {
    ...result,
    assistantText: result.assistantText.slice(0, 8000),
    toolCalls: result.toolCalls.slice(0, 50).map((c) => ({ ...c, args: c.args.slice(0, 2000) })),
    toolResults: result.toolResults.slice(0, 20).map((r) => r.slice(0, 2000)),
  }
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

/** 会话上下文占用快照(任务 result/progress 与 session_list/task_list 通用形状) */
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

/** 任务进行中的步骤信息: 从任务开始点(baseline)之后的日志增量里提取, 供客户端汇报进度;
 *  同时附上任务会话的上下文占用 context(tokenMeter/窗口不可解析时为 null) */
async function taskProgressOf(ctx: Context, item: TaskItem): Promise<Record<string, unknown>> {
  const info: Record<string, unknown> = { status: item.status }
  if (item.status === 'queued') { info.context = null; return info }
  const agent = liveAgentFor(ctx, item.sessionId)
  const session = agent?.session as { log?: unknown[] } | undefined
  if (!session?.log) {
    // 会话已退役/未加载: 无法读增量, 只附完成时快照(仍为 null 表示无法测量)
    info.context = item.contextUsage ?? null
    return info
  }
  const slice = session.log.slice(item.baseline ?? 0)
  let toolCalls = 0
  let currentTool: { name: string; args: string } | undefined
  let lastText = ''
  for (const ev of slice) {
    const e = ev as { type?: string; data?: { name?: string; arguments?: string; message?: { content?: { type?: string; text?: string }[] } } }
    if (e.type === 'tool/call') {
      toolCalls++
      currentTool = { name: e.data?.name ?? '?', args: String(e.data?.arguments ?? '').slice(0, 300) }
    } else if (e.type === 'assistant/message' || e.type === 'user/message') {
      const text = (e.data?.message?.content ?? []).filter((c) => c.type === 'text' && c.text).map((c) => c.text).join(' ').trim()
      if (text) lastText = text.slice(0, 200)
    }
  }
  info.events = slice.length
  info.toolCalls = toolCalls
  if (currentTool) info.currentTool = currentTool
  if (lastText) info.lastText = lastText

  // 等待输入感知: 审批(本插件应答链挂起)与提问(本插件 provider 挂起 / GUI 路由的挂起 ask_user_question)
  // → status=waiting_input + prompts[], 供 MCP 调用方感知弹窗并响应(prompt_respond / web GUI)
  const prompts: Record<string, unknown>[] = []
  for (const pa of pendingApprovals.values()) {
    if (pa.agentId === item.sessionId) {
      prompts.push({ type: 'approval', id: pa.promptId, toolName: pa.toolName, ...(pa.reason !== undefined ? { reason: pa.reason } : {}) })
    }
  }
  for (const pq of pendingQuestions.values()) {
    if (pq.agentId === item.sessionId) prompts.push({ type: 'question', id: pq.promptId, questions: pq.questions })
  }
  if (!questionsProviderOurs) {
    const detected = detectPendingAskUser(session)
    if (detected) {
      prompts.push({ type: 'question', id: detected.id, questions: detected.questions, note: 'routed to the web GUI provider; answer in the DSH web UI' })
    }
  }
  if (prompts.length > 0) {
    info.status = 'waiting_input'
    info.prompts = prompts
  }
  // 上下文占用: 任务会话仍 live 时实时测量(池优先); 已退役/未加载则回退任务完成时快照
  info.context = agent
    ? await contextUsage(ctx, agent.session, agent)
    : (item.contextUsage ?? null)
  return info
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

/** 待响应的提问 prompt(仅当本插件持有 user-questions provider 时产生; web GUI 占槽时提问走 GUI) */
interface PendingQuestion {
  promptId: string
  agentId: string
  questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }>
  resolve: (answer: { answers: Array<{ id: string; selected: string[]; custom?: string }> }) => void
  reject: (e: Error) => void
}

/** MCP 驱动的会话 id 集(创建/接管即标记): 标识「该会话由 MCP 创建或接管过」(历史事实, 不随任务结束消失) */
const mcpSessionIds = new Set<string>()
/** 当前有 MCP 任务正在执行的会话 id 集(mcpBusySessionIds ⊆ mcpSessionIds): 这是「会话当前是否有 MCP
 *  活跃任务」(正被 agent_run / task_inbox 驱动)的精确判据 —— executeTask 里 getAgent 成功返回后标记,
 *  任务结束(成功/超时/取消/异常统一汇聚到 return result 前的落点)清除。审批应答者必须同时满足
 *  「在 mcpSessionIds 且在 mcpBusySessionIds」才接管: 单看 mcpSessionIds 只能证明历史接管过, 用户经
 *  web UI 直接向该会话发消息触发的审批也会被误接管, 而 Hermes 并无任务在等 → 两端都收不到, 死锁。 */
const mcpBusySessionIds = new Set<string>()
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
/** 提问 provider 是否由本插件持有(false = web GUI 占槽, 提问路由到 GUI) */
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

/** 检测挂起的 ask_user_question 工具调用(web GUI 持有提问 provider 时, 这是感知提问的唯一途径):
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

/** 核心执行: 组装任务(注入记忆上下文+结构化要求) → agent 执行 → 读结构化结果。
 *  opts.onAgent 在 agent 就绪后回调一次, 供 task_cancel 注册打断钩子;
 *  opts.fresh = true 且未传 sessionId 时强制全新会话(跳过池复用, 见 getAgent);
 *  opts.provider/model 为按次调用的模型覆盖(对新建/resume 会话生效);
 *  opts.mode 为按次的会话模式(解析后的 {preset?, sandbox?, approval?}): 指定即强制全新会话,
 *  创建时应用 —— 会话自创建起就跑在该模式下(避免后续再提权);
 *  opts.timeoutMs 为按次执行超时(覆盖全局 taskTimeoutMs; 0 = 不限制);
 *  opts.shouldAbort 在等锁后/执行前检查, 支持取消"排队/等锁中"的任务(agent 未就绪时 task_cancel 置 cancelled)。 */
async function executeTask(
  ctx: Context,
  task: string,
  context: string,
  cwd: string,
  resumeSessionId?: string,
  title?: string,
  opts?: {
    onAgent?: (agent: Agent) => void
    fresh?: boolean
    provider?: string
    model?: string
    mode?: { preset?: string; sandbox?: string; approval?: string }
    timeoutMs?: number
    shouldAbort?: () => boolean
  },
): Promise<TaskResult> {
  // 规范化 cwd: realpath 解析符号链接与 .. 段, 避免 /a、/a/.、相对路径、符号链接成为不同 Map key
  // 导致重复创建会话/并发冲突; 同时也是与 workspace.path 精确比对的唯一 canon
  const workdir = await canonicalCwd(cwd ? resolve(cwd) : process.cwd())
  // cwd 白名单: 配置了 workspaceRoots 时, 只允许在列出的目录下干活(防路径穿越)
  if (!cwdAllowed(workdir)) {
    throw new Error(`cwd not allowed (outside workspaceRoots): ${workdir}`)
  }
  // sessionId 用 session 锁, 否则用 cwd 锁——都防同一 agent 会话被并发 followup
  const lockKey = resumeSessionId ? `session:${resumeSessionId}` : workdir
  return withLock(lockKey, async () => {
    // 取消检查: 任务在等锁期间被 task_cancel(agent 未就绪路径)置了 cancelled → 执行前中止, 不启动 agent
    if (opts?.shouldAbort?.()) throw new Error('task cancelled')
    // 指定模式且未传 sessionId 时恒强制全新会话(池复用的存量会话无法安全套用新模式)
    const modeRequested = opts?.mode !== undefined && (opts.mode.preset !== undefined || opts.mode.sandbox !== undefined || opts.mode.approval !== undefined)
    // 会话命名: 显式 title 优先; 未传时按任务内容派生可读名称(只在新建会话时经 rename 落为 session/title 事件)
    const effectiveTitle = title ?? deriveSessionTitle(task || context)
    const { sessionId, handle, disposeAfter } = await getAgent(
      ctx, workdir, resumeSessionId, effectiveTitle, opts?.fresh || (modeRequested && resumeSessionId === undefined),
      { provider: opts?.provider, model: opts?.model }, opts?.mode,
    )
    // 标记「该会话当前有 MCP 任务在跑」(审批接管判据): 任务结束(成功/超时/取消/异常统一汇聚到
    // return result 前的落点)立即清除, 避免残留导致后续 web UI 直发消息触发的审批被误接管。
    mcpBusySessionIds.add(String(sessionId))
    const baseline = ((handle.agent.session as unknown as { log?: unknown[] }).log ?? []).length

    // 组装完整任务文本: 记忆上下文 + 任务 + 结构化输出要求
    const fullTask = [
      context ? `【记忆/上下文(供参考, 来自 Hermes 大脑)】\n${context}\n` : '',
      `【任务】\n${task}\n`,
      `【完成后必须】用一行 JSON 总结(不要 markdown 代码块包裹, 直接输出这一行):`,
      `{"changes":"改了什么","verification":"怎么验证的","leftovers":"遗留问题"}`,
    ].filter(Boolean).join('\n')

    // 结构化读输出(提前声明, 供执行异常兜底填充)
    const result: TaskResult = {
      taskId: '', sessionId, assistantText: '', toolCalls: [], toolResults: [],
      changes: '', verification: '', leftovers: '',
    }
    // 会话标题带回结果: 以 sessionTitle 服务快照为准(新建会话已由 rename 落事件; 复用会话读既有标题)
    try {
      const currentTitle = sessionTitleOf(ctx, handle.agent.session)
      if (currentTitle !== undefined) result.title = currentTitle
    } catch {
      /* 标题读取失败不阻断结果返回 */
    }

    // 驱动 agent 执行; 执行/调度层抛出的异常(非超时)转成结构化 error 结果, 不再上抛导致空跑
    try {
      handle.agent.followup(
        createUserMessage({ content: [{ type: 'text', text: fullTask }], source: { kind: 'plugin', plugin: 'harness-mcp-server' } }),
      )
      // agent 就绪: 通知调用方(供 task_cancel 注册打断钩子)
      opts?.onAgent?.(handle.agent)

      // 超时保护: whenIdle 无限等待会让 MCP 客户端挂死; 到点后 cancel 打断本轮, 回收部分输出
      // (per-task timeoutMs 覆盖全局 taskTimeoutMs)
      const taskTimeout = opts?.timeoutMs ?? runtimeConfig.taskTimeoutMs
      let timedOut = false
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        await Promise.race([
          handle.agent.whenIdle(),
          new Promise<never>((_resolve, reject) => {
            if (taskTimeout > 0) {
              timer = setTimeout(() => { timedOut = true; reject(TASK_TIMEOUT) }, taskTimeout)
            }
          }),
        ])
      } catch (e) {
        if (e !== TASK_TIMEOUT) throw e
        // cancel 丢弃未开始的排队输入, 中止活动回合; 之后 whenIdle 很快落定
        try { handle.agent.cancel({ kind: 'hook', reason: 'harness-mcp-timeout' }) } catch { /* 打断失败不阻断回收 */ }
        await handle.agent.whenIdle()
        result.timeout = true
      } finally {
        if (timer !== undefined) clearTimeout(timer)
      }
    } catch (e) {
      // 【1c】agent 执行/调度层异常(如 agent-loop 抛错): 转成结构化 error 结果, 任务以 status=error 结束
      const err = e as { code?: string; message?: string; name?: string }
      result.error = {
        errorCode: err?.code ?? 'AGENT_ERROR',
        errorMessage: err?.message ?? String(e),
        errorCategory: 'execution',
      }
    }

    // 结构化读输出
    try {
      const log = ((handle.agent.session as unknown as { log?: unknown[] }).log ?? []).slice(baseline)
      for (const e of log) {
        const ev = e as {
          type?: string
          message?: { content?: { type?: string; text?: string }[] }
          data?: unknown
        }
        if (ev.type === 'assistant/message') {
          const d = ev.data as { message?: { content?: { type?: string; text?: string }[] } } | undefined
          const content = d?.message?.content
          if (content) {
            const texts = content.filter((c) => c.type === 'text' && c.text).map((c) => c.text)
            if (texts.length) result.assistantText += texts.join('\n') + '\n'
          }
        } else if (ev.type === 'tool/call') {
          const d = ev.data as { name?: string; arguments?: string; input?: unknown } | undefined
          result.toolCalls.push({
            name: d?.name ?? '?',
            args: (d?.arguments ?? JSON.stringify(d?.input ?? null) ?? '').slice(0, 2000),
          })
        } else if (ev.type === 'tool/result') {
          const texts: string[] = []
          extractText(ev.data ?? ev, texts)
          if (texts.length) result.toolResults.push(texts.join('\n').slice(0, 3000))
        }
      }
      // 【1b】模型/执行失败感知: turn/end reason.kind ∈ {error, failed, max-tokens} → 结构化错误。
      // 真实模型接口报错(配额/网络/参数)以 reason.kind='error' 出现(实测 429 QUOTA 会话),
      // 'max-tokens' 为输出上限(模型侧限制); LLM 错误码原样保留, 'UNKNOWN' 视为通用执行失败。
      for (const e of log) {
        const ev = e as {
          type?: string
          data?: { reason?: { kind?: string; error?: { code?: string; message?: string } }; chunk?: { type?: string; reason?: { kind?: string; failure?: { code?: string; message?: string } } } }
        }
        const reason = ev.data?.reason
        if (ev.type === 'turn/end' && reason !== undefined && (reason.kind === 'error' || reason.kind === 'failed' || reason.kind === 'max-tokens')) {
          const err = (reason.kind === 'error' || reason.kind === 'failed') ? reason.error : undefined
          const code = err?.code ?? (reason.kind === 'max-tokens' ? 'MAX_TOKENS' : 'UNKNOWN')
          result.error = {
            errorCode: code,
            errorMessage: err?.message ?? (reason.kind === 'max-tokens' ? 'output token ceiling reached' : 'agent turn failed'),
            errorCategory: code === 'UNKNOWN' ? 'execution' : 'model',
          }
          break
        }
        // 兜底: assistant/chunk finish 报错(未及 turn/end 时)
        if (ev.type === 'assistant/chunk' && ev.data?.chunk?.type === 'finish' && ev.data.chunk.reason?.kind === 'error' && ev.data.chunk.reason.failure) {
          result.error = {
            errorCode: ev.data.chunk.reason.failure.code ?? 'MODEL_ERROR',
            errorMessage: ev.data.chunk.reason.failure.message ?? 'model request failed',
            errorCategory: 'model',
          }
          break
        }
      }
    } catch (e) {
      result.assistantText = `[读输出异常] ${String(e)}`
    }

    // 解析结构化 summary
    const summary = parseSummary(result.assistantText)
    result.changes = summary.changes
    result.verification = summary.verification
    result.leftovers = summary.leftovers

    // C3 兜底: 模型没按格式吐 JSON 时, 用最近工具结果摘要填充 changes(尽力而为, 不再全空)
    if (!result.changes && !result.verification && !result.leftovers) {
      const last = result.toolResults.slice(-5).join('\n').slice(0, 1500)
      if (last) result.changes = `(heuristic from tool output) ${last}`
    }

    // 超时标注: leftovers 为空时补一句引导, 提示可用 sessionId 续接
    if (result.timeout === true && !result.leftovers) {
      result.leftovers = '任务超时被自动取消, 以上为部分进展; 可用 sessionId 续接继续'
    }

    // 上下文占用快照: 任务结束瞬间(agent 仍 live)测量, 挂到 result 供调用方跟踪占用;
    // tokenMeter 缺失返回 null, 窗口不可解析时 window/ratio 为 null(不阻断结果返回)
    try {
      result.context = await contextUsage(ctx, handle.agent.session, handle.agent)
    } catch {
      result.context = null
    }

    // 模式快照: 任务所用会话的生效模式(preset + 沙箱 + 审批); 本任务请求的模式已应用时 requested 与 effective 一致
    try {
      const sidStr = String(sessionId)
      const eff = sessionModeOf(ctx, sidStr, handle.agent.session, (handle.agent.session as { header?: { agentPreset?: string } }).header)
      result.mode = {
        preset: eff.preset,
        sandbox: eff.sandbox,
        approval: eff.approval,
        ...(eff.permissionPreset !== undefined ? { permissionPreset: eff.permissionPreset } : {}),
        ...(opts?.mode !== undefined && (opts.mode.preset !== undefined || opts.mode.sandbox !== undefined || opts.mode.approval !== undefined)
          ? { requested: { ...(opts.mode.preset !== undefined ? { preset: opts.mode.preset } : {}), ...(opts.mode.sandbox !== undefined ? { sandbox: opts.mode.sandbox } : {}), ...(opts.mode.approval !== undefined ? { approval: opts.mode.approval } : {}) } }
          : {}),
      }
    } catch {
      /* 模式快照失败不阻断结果返回 */
    }

    // resume 兜底分支: 尽力 flush 持久化, 再释放我们 resume 出来的句柄(不留给僵尸 live agent)
    if (disposeAfter) {
      try {
        await (ctx.get('sessions') as { flush?: (session: unknown) => Promise<unknown> } | undefined)?.flush?.(handle.agent.session)
      } catch {
        /* flush 失败不阻断结果返回 */
      }
      try {
        await handle.dispose()
      } catch {
        /* 释放失败不影响结果 */
      }
    }

    // 任务结束统一落点(正常/超时/取消/错误都汇聚到这里): 不论成功/超时/取消/异常都清除
    // 「MCP 任务在跑」标记 —— 残留会让审批应答者误以为仍有 MCP 活跃任务而错误接管。
    mcpBusySessionIds.delete(String(sessionId))

    return result
  })
}

/** 异步任务队列(进程内存, 骨架阶段; 后续可持久化) */
interface TaskItem {
  id: string
  task: string
  context: string
  cwd: string
  sessionId?: string
  title?: string
  /** true = 执行时强制全新会话(不池复用) */
  newSession?: boolean
  /** 按次调用的模型/provider 覆盖 */
  model?: string
  provider?: string
  /** 按次的会话模式(解析后的 {preset?, sandbox?, approval?}): 创建会话时应用 */
  mode?: { preset?: string; sandbox?: string; approval?: string }
  /** 按次执行超时(覆盖全局 taskTimeoutMs; 0 = 不限制) */
  timeoutMs?: number
  /** agent 就绪时的会话日志起点(供进度提取本任务增量) */
  baseline?: number
  status: 'queued' | 'running' | 'done' | 'error'
  result?: TaskResult
  error?: string
  /** 任务结束时所用会话的上下文占用快照(agent 已退役后 task_result/task_list 仍可读) */
  contextUsage?: ContextUsage | null
  /** true = 被 task_cancel 主动打断(区别于超时自动 cancel) */
  cancelled?: boolean
  createdAt: number
  finishedAt?: number
}
const taskQueue = new Map<string, TaskItem>()

/** taskId → 打断钩子: 任务进入 running 且 agent 就绪后注册, task_cancel 调用后清理 */
const taskCancelHooks = new Map<string, () => Promise<void>>()

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

  // 运维总览: 队列/agent 池/live 会话/运行时配置 —— 外部客户端一眼看清系统水位
  mcp.tool(
    'harness_status',
    '系统水位总览: 任务队列(排队/执行/完成/失败)、agent 常驻池、live 会话数、运行时配置。',
    {},
    async () => {
      let queued = 0, running = 0, done = 0, error = 0
      for (const t of taskQueue.values()) {
        if (t.status === 'queued') queued++
        else if (t.status === 'running') running++
        else if (t.status === 'error') error++
        else done++
      }
      const liveCount = ((ctx.agents as unknown as { list?: () => unknown[] }).list?.() ?? []).length
      return out(JSON.stringify({
        uptimeSec: Math.round(process.uptime()),
        queue: { total: taskQueue.size, queued, running, done, error },
        agentPool: { size: liveAgents.size, max: runtimeConfig.maxAgents, liveAgents: liveCount },
        config: {
          provider: runtimeConfig.provider,
          model: runtimeConfig.model || '(dsh default)',
          preset: runtimeConfig.preset,
          maxQueue: runtimeConfig.maxQueue,
          maxAgents: runtimeConfig.maxAgents,
          taskTimeoutMs: runtimeConfig.taskTimeoutMs,
          taskTtlMs: runtimeConfig.taskTtlMs,
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
  // modes 汇总给出可传给 agent_run/task_inbox 的 mode= 规范 id(按类别), 与 model_list 的枚举姿势一致:
  // presets 经 ctx.agentPresets.list() 实时枚举, 沙箱/审批词汇固定, 默认值经 ctx.sandboxPolicy / ctx.approval /
  // ctx.permissionPresets(服务缺省时给安全回退)。withDetail=true 附带更多元数据与部署默认。
  mcp.tool(
    'mode_list',
    '列出可用会话模式: agent preset(standard/code/cordis/minimal 等, 来自 dsh agent-presets) + 沙箱访问模式(read-only/workspace-write/danger-full-access) + 审批策略(ask/never) + 权限预设(捆绑沙箱+审批, 如 workspace-write = workspace-write + ask)。modes 汇总给出可传给 agent_run/task_inbox 的 mode= 规范 id; 传 only 只列某一类。',
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

      // 5) modes 汇总: 传给 agent_run/task_inbox mode= 的规范 id(按类别去重, 与类别行同序)
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

  // 同步执行任务(简单场景: Hermes 下发 → 立即拿结果)。
  // 不传 sessionId 时 cwd 必填(避免误用 dsh 进程目录); 传 timeoutMs 可把长任务转成异步(返回 taskId 供轮询/取消)。
  mcp.tool(
    'agent_run',
    '同步执行任务(改代码/分析/跑命令), 返回结构化结果, 附任务会话上下文占用 context(events/tokens/pressure/window/ratio)与生效模式 mode(preset/sandbox/approval)。可传 sessionId 续接已有会话(长任务分多轮投喂); 可传 newSession:true 强制全新会话(不复用池); 可传 model/provider 按次选模型; 可传 preset/mode/sandbox/approval 指定会话模式(创建会话时应用, 指定即强制全新会话, 避免后续再提权); 传 timeoutMs(毫秒)超时后自动转为异步任务(返回 taskId, 用 task_result/task_cancel/task_wait 跟进)。',
    {
      task: z.string().describe('要 Harness 执行的自然语言任务'),
      context: z.string().optional().describe('Hermes 记忆/上下文, 注入给 agent 参考'),
      cwd: z.string().optional().describe('工作目录(不传 sessionId 时必填; 可用 workspace_list 查看可用目录)'),
      sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果里的 sessionId 字段)'),
      newSession: z.boolean().optional().describe('true = 强制全新会话(跳过该 cwd 的池复用, 旧会话退役但持久化保留, 仍可凭 sessionId 续接); 缺省 = 复用该 cwd 的常驻会话'),
      model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效; 池复用的会话保持原模型)'),
      provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
      mode: z.string().optional().describe('会话模式(创建新会话时应用; 指定即强制全新会话)。取值见 mode_list 的 modes: agent preset id(standard/code/cordis/minimal) 或权限预设名(如 workspace-write = 沙箱 workspace-write + 审批 ask) 或沙箱模式(read-only/workspace-write/danger-full-access) 或审批策略(ask/never)'),
      preset: z.string().optional().describe('agent preset id(standard/code/cordis/minimal 等; 挂载到新会话, 写进 session header 的 agentPreset)。resume 存量会话时也允许(在 setup 挂载该 preset)'),
      sandbox: z.enum(['read-only', 'workspace-write', 'danger-full-access']).optional().describe('沙箱访问模式(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
      approval: z.enum(['ask', 'never']).optional().describe('审批策略(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
      timeoutMs: z.number().int().min(0).optional().describe('同步等待上限毫秒数(默认 taskTimeoutMs=60 分钟; 建议按客户端 HTTP 超时设小, 如 120000; 0 = 不转异步)'),
      title: z.string().optional().describe('新会话的标题(创建时命名, 便于会话列表归档)'),
    },
    async ({ task, context, cwd, sessionId, newSession, model, provider, mode, preset, sandbox, approval, timeoutMs, title }) => {
      if (!cwd && !sessionId) {
        return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }))
      }
      // 模式解析与前置校验: 非法 mode/preset/sandbox/approval 立即报错(不排队); sandbox/approval 不可用于续接存量会话
      let modeRes: { preset?: string; sandbox?: string; approval?: string } | undefined
      try {
        modeRes = await resolveModeRequest(ctx, { mode, preset, sandbox, approval })
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      if (sessionId && (modeRes.sandbox !== undefined || modeRes.approval !== undefined)) {
        return err(JSON.stringify({ error: 'mode/sandbox/approval only apply when creating a new session; pass newSession:true or omit sessionId (preset alone is allowed when resuming)' }))
      }
      const now = Date.now()
      // TTL 清理: 删除已完成/失败且超时的任务
      for (const [tid, t] of taskQueue) {
        if ((t.status === 'done' || t.status === 'error') && t.finishedAt && now - t.finishedAt > runtimeConfig.taskTtlMs) {
          taskQueue.delete(tid)
        }
      }
      // 队列容量(与 task_inbox 一致): 活动任务超过上限则拒绝
      let active = 0
      for (const t of taskQueue.values()) if (t.status === 'queued' || t.status === 'running') active++
      if (active >= runtimeConfig.maxQueue) {
        return err(JSON.stringify({ error: `task queue full (${active}/${runtimeConfig.maxQueue})` }))
      }
      // 统一注册为可查/可取消的任务条目: 同步完成时回填真实 taskId, 超时转异步后同一条目继续
      const id = randomUUID()
      const item: TaskItem = {
        id, task, context: context ?? '', cwd: cwd ?? process.cwd(), status: 'running', createdAt: now,
        ...(sessionId ? { sessionId } : {}),
        ...(newSession ? { newSession: true } : {}),
        ...(model ? { model } : {}),
        ...(provider ? { provider } : {}),
        ...(modeRes && (modeRes.preset !== undefined || modeRes.sandbox !== undefined || modeRes.approval !== undefined) ? { mode: modeRes } : {}),
        ...(title ? { title } : {}),
      }
      taskQueue.set(id, item)
      const background: Promise<TaskResult | undefined> = (async () => {
        try {
          item.result = await executeTask(ctx, item.task, item.context, item.cwd, item.sessionId, item.title, {
            fresh: item.newSession === true,
            model: item.model,
            provider: item.provider,
            mode: item.mode,
            timeoutMs: item.timeoutMs,
            shouldAbort: () => item.cancelled === true,
            onAgent: (agent) => {
              // 记录会话与日志起点: 供进度提取(本任务增量)与 task_list 上下文占用
              item.sessionId = String(agent.session.id)
              item.baseline = ((agent.session as unknown as { log?: unknown[] }).log ?? []).length
              taskCancelHooks.set(id, async () => {
                item.cancelled = true
                agent.cancel({ kind: 'hook', reason: 'harness-mcp-task-cancel' })
              })
            },
          })
          item.result.taskId = id
          item.contextUsage = item.result.context ?? null
          item.sessionId = item.result.sessionId
          if (item.result.error) {
            // 模型/执行失败: 任务以 error 结束, client 不再误判成功
            item.error = `${item.result.error.errorCode}: ${item.result.error.errorMessage}`
            item.status = 'error'
          } else {
            item.status = 'done'
          }
          return item.result
        } catch (e) {
          item.error = String(e)
          item.status = 'error'
          return undefined
        } finally {
          taskCancelHooks.delete(id)
          item.finishedAt = Date.now()
        }
      })()
      const waitMs = timeoutMs ?? runtimeConfig.taskTimeoutMs
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        const result = await Promise.race([
          background,
          new Promise<never>((_resolve, reject) => {
            if (waitMs > 0) timer = setTimeout(() => reject(TASK_TIMEOUT), waitMs)
          }),
        ])
        if (!result) return err(JSON.stringify({ error: `task failed: ${item.error ?? 'unknown'}` }))
        // 模型/执行失败: 同步路径也以 isError 返回, 附结构化 error + 部分输出供续接
        if (result.error) {
          return err(JSON.stringify({ error: result.error, taskId: id, sessionId: result.sessionId, partial: truncateResult(result) }, null, 2))
        }
        return out(JSON.stringify(truncateResult(result), null, 2))
      } catch (e) {
        if (e !== TASK_TIMEOUT) throw e
        // 转异步: 任务在后台继续跑, 客户端用 taskId 轮询/等待/取消; 附带当前进度(含上下文占用)供汇报
        return out(JSON.stringify({
          status: 'async',
          taskId: id,
          // 实际会话 id(onAgent 就绪后回填; 任务仍在等锁时可能缺省): 转异步后调用方凭它跟进
          // waiting_input 的审批/提问(prompt_respond)或 session_read 审计, 无需绕道 task_list
          ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
          progress: await taskProgressOf(ctx, item),
          note: `task still running after ${waitMs}ms; poll via task_result / task_wait, or cancel via task_cancel`,
        }, null, 2))
      } finally {
        if (timer !== undefined) clearTimeout(timer)
      }
    },
  )

  // 异步 push 任务到队列(Hermes → Harness 任务入口)
  mcp.tool(
    'task_inbox',
    'Hermes 把结构化任务(任务+记忆上下文)推入 Harness 队列, 异步执行, 返回 taskId; 任务结束后的上下文占用经 task_result/task_wait/task_list 读取。可传 newSession:true 强制全新会话; 可传 preset/mode/sandbox/approval 指定会话模式(创建会话时应用, 指定即强制全新会话, 避免后续再提权)。',
    {
      task: z.string().describe('任务内容'),
      context: z.string().optional().describe('Hermes 记忆/上下文, 随任务注入给 agent'),
      cwd: z.string().optional().describe('工作目录'),
      sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果)'),
      newSession: z.boolean().optional().describe('true = 强制全新会话(不复用该 cwd 的常驻会话); 缺省 = 复用'),
      model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效)'),
      provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
      mode: z.string().optional().describe('会话模式(创建新会话时应用; 指定即强制全新会话)。取值见 mode_list 的 modes: agent preset id 或权限预设名(如 workspace-write = 沙箱 workspace-write + 审批 ask) 或沙箱模式(read-only/workspace-write/danger-full-access) 或审批策略(ask/never)'),
      preset: z.string().optional().describe('agent preset id(standard/code/cordis/minimal 等; 挂载到新会话, 写进 session header 的 agentPreset)。resume 存量会话时也允许'),
      sandbox: z.enum(['read-only', 'workspace-write', 'danger-full-access']).optional().describe('沙箱访问模式(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
      approval: z.enum(['ask', 'never']).optional().describe('审批策略(显式指定, 覆盖 mode 捆绑里的值; 仅新建会话时应用)'),
      timeoutMs: z.number().int().min(0).optional().describe('本次任务的执行超时毫秒数(默认 taskTimeoutMs=60 分钟; 超时自动 cancel 并回收部分输出; 0 = 不限制)'),
      title: z.string().optional().describe('新会话的标题(创建时命名)'),
    },
    async ({ task, context, cwd, sessionId, newSession, model, provider, mode, preset, sandbox, approval, timeoutMs, title }) => {
      if (!cwd && !sessionId) {
        return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }))
      }
      // 模式解析与前置校验: 非法 mode/preset/sandbox/approval 立即报错(不排队); sandbox/approval 不可用于续接存量会话
      let modeRes: { preset?: string; sandbox?: string; approval?: string } | undefined
      try {
        modeRes = await resolveModeRequest(ctx, { mode, preset, sandbox, approval })
      } catch (e) {
        return err(JSON.stringify({ error: (e as Error)?.message ?? String(e) }))
      }
      if (sessionId && (modeRes.sandbox !== undefined || modeRes.approval !== undefined)) {
        return err(JSON.stringify({ error: 'mode/sandbox/approval only apply when creating a new session; pass newSession:true or omit sessionId (preset alone is allowed when resuming)' }))
      }
      const now = Date.now()
      // TTL 清理: 删除已完成/失败且超时的任务
      for (const [tid, t] of taskQueue) {
        if ((t.status === 'done' || t.status === 'error') && t.finishedAt && now - t.finishedAt > runtimeConfig.taskTtlMs) {
          taskQueue.delete(tid)
        }
      }
      // 队列容量上限: 活动任务(排队+执行中)超过上限则拒绝
      let active = 0
      for (const t of taskQueue.values()) if (t.status === 'queued' || t.status === 'running') active++
      if (active >= runtimeConfig.maxQueue) {
        return err(JSON.stringify({ error: `task queue full (${active}/${runtimeConfig.maxQueue})` }))
      }
      const id = randomUUID()
      const item: TaskItem = {
        id, task, context: context ?? '', cwd: cwd ?? process.cwd(), status: 'queued', createdAt: now,
        ...(sessionId ? { sessionId } : {}),
        ...(newSession ? { newSession: true } : {}),
        ...(model ? { model } : {}),
        ...(provider ? { provider } : {}),
        ...(modeRes && (modeRes.preset !== undefined || modeRes.sandbox !== undefined || modeRes.approval !== undefined) ? { mode: modeRes } : {}),
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
        ...(title ? { title } : {}),
      }
      taskQueue.set(id, item)
      // 异步执行(不阻塞 Hermes); agent 就绪后注册打断钩子, 供 task_cancel 主动打断
      void (async () => {
        item.status = 'running'
        try {
          item.result = await executeTask(ctx, item.task, item.context, item.cwd, item.sessionId, item.title, {
            fresh: item.newSession === true,
            model: item.model,
            provider: item.provider,
            mode: item.mode,
            timeoutMs: item.timeoutMs,
            shouldAbort: () => item.cancelled === true,
            onAgent: (agent) => {
              // 记录会话与日志起点: 供进度提取(本任务增量)与 task_list 上下文占用
              item.sessionId = String(agent.session.id)
              item.baseline = ((agent.session as unknown as { log?: unknown[] }).log ?? []).length
              taskCancelHooks.set(id, async () => {
                item.cancelled = true
                agent.cancel({ kind: 'hook', reason: 'harness-mcp-task-cancel' })
              })
            },
          })
          item.result.taskId = id
          item.contextUsage = item.result.context ?? null
          item.sessionId = item.result.sessionId // 回填实际使用的会话, 供 task_list 附上下文占用/后续续接
          if (item.result.error) {
            // 模型/执行失败: 任务以 error 结束, client 不再误判成功
            item.error = `${item.result.error.errorCode}: ${item.result.error.errorMessage}`
            item.status = 'error'
          } else {
            item.status = 'done'
          }
        } catch (e) {
          item.error = String(e)
          item.status = 'error'
        } finally {
          taskCancelHooks.delete(id)
          item.finishedAt = Date.now()
        }
      })()
      // 队列已接收: context 恒 null(任务未开始, 无会话可测); 执行中/结束后的占用经 task_result/task_wait/task_list 读取。
      // sessionId 仅当调用方显式传入时回显(此时是「请求续接的会话」); 未传时实际会话由执行期 getAgent
      // 决定(池复用/新建), 待 agent 就绪后经 task_result/task_list 的 sessionId 字段可见。
      return out(JSON.stringify({
        taskId: id,
        status: 'queued',
        ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
        context: null,
      }))
    },
  )

  // 取回任务结果(结构化 changes/verification/leftovers; 未完成时带进行中进度)
  mcp.tool(
    'task_result',
    '取回 task_inbox 提交任务的结构化结果(changes/verification/leftovers); 未完成时返回 progress 供汇报进度; 附任务会话上下文占用 context(events/tokens/pressure/window/ratio)。',
    { taskId: z.string().describe('task_inbox 返回的 taskId') },
    async ({ taskId }) => {
      const item = taskQueue.get(taskId)
      if (!item) return err(JSON.stringify({ error: `task not found: ${taskId}` }))
      return out(JSON.stringify({
        taskId: item.id,
        // 实际会话 id(onAgent 就绪后回填, 池复用/新建时可能 ≠ 传入的 resume id; 未就绪时省略):
        // waiting_input 时与 progress.prompts 的 promptId 配对直调 prompt_respond, 无需绕道 task_list
        sessionId: item.sessionId,
        status: item.status,
        error: item.error,
        cancelled: item.cancelled === true || undefined,
        context: item.contextUsage ?? null,
        progress: await taskProgressOf(ctx, item),
        result: item.result ? truncateResult(item.result) : undefined,
      }, null, 2))
    },
  )

  // 阻塞等待任务完成: 一次往返替代 N 次轮询(服务端每 500ms 查一次, 到 timeoutMs 或任务落定返回);
  // 无论完成还是超时, 都附带 progress(进行中的步骤/已用工具/最新文本)供客户端汇报进度
  mcp.tool(
    'task_wait',
    '阻塞等待一个任务完成/失败后返回其结果(服务端等待, 一次往返替代多次轮询); 超过 timeoutMs 返回当前状态与 progress(正在执行的步骤/工具调用/上下文占用)。客户端 HTTP 超时应大于 timeoutMs。',
    {
      taskId: z.string().describe('task_inbox / agent_run(转异步)返回的 taskId'),
      timeoutMs: z.number().int().min(100).max(600000).optional().describe('等待上限毫秒数(默认 60000; 上限 10 分钟)'),
    },
    async ({ taskId, timeoutMs }) => {
      const item = taskQueue.get(taskId)
      if (!item) return err(JSON.stringify({ error: `task not found: ${taskId}` }))
      const wait = timeoutMs ?? 60000
      const deadline = Date.now() + wait
      while (item.status === 'queued' || item.status === 'running') {
        const remain = deadline - Date.now()
        if (remain <= 0) break
        await new Promise((r) => setTimeout(r, Math.min(500, remain)))
      }
      return out(JSON.stringify({
        taskId: item.id,
        // 实际会话 id(onAgent 就绪后回填, 池复用/新建时可能 ≠ 传入的 resume id; 未就绪时省略):
        // waiting_input 时与 progress.prompts 的 promptId 配对直调 prompt_respond, 无需绕道 task_list
        sessionId: item.sessionId,
        status: item.status,
        error: item.error,
        cancelled: item.cancelled === true || undefined,
        context: item.contextUsage ?? null,
        progress: await taskProgressOf(ctx, item),
        result: item.result ? truncateResult(item.result) : undefined,
      }, null, 2))
    },
  )

  // 列出最近任务: 供批量轮询与队列观察(task_result 只能逐个查); 可按 sessionId 过滤
  mcp.tool(
    'task_list',
    '列出最近的任务(taskId/状态/目录/时间/会话上下文/进行中进度), 便于批量轮询与观察队列; 按 createdAt 倒序, 可按 sessionId 过滤。',
    {
      limit: z.number().int().min(1).max(200).optional().describe('最多返回条数(默认 20)'),
      sessionId: z.string().optional().describe('只返回使用了该会话的任务'),
    },
    async ({ limit, sessionId }) => {
      const n = limit ?? 20
      const items = [...taskQueue.values()]
        .filter((t) => !sessionId || t.sessionId === sessionId)
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, n)
      const tasks = []
      for (const t of items) {
        // 任务所用会话仍 live 时附上实时上下文占用(池优先); 已退役/未加载则回退任务完成时快照(仍为 null 表示无法测量)
        const agent = liveAgentFor(ctx, t.sessionId)
        const context = agent ? await contextUsage(ctx, agent.session, agent) : (t.contextUsage ?? null)
        const progress = t.status === 'queued' || t.status === 'running' ? await taskProgressOf(ctx, t) : undefined
        tasks.push({
          taskId: t.id,
          status: t.status,
          cwd: t.cwd,
          sessionId: t.sessionId,
          createdAt: t.createdAt,
          finishedAt: t.finishedAt,
          cancelled: t.cancelled === true || undefined,
          timeout: t.result?.timeout === true || undefined,
          error: t.error,
          hasResult: t.result !== undefined,
          context,
          progress,
        })
      }
      return out(JSON.stringify({ total: taskQueue.size, tasks }, null, 2))
    },
  )

  // 打断一个 queued/running 任务: 与超时保护共用 agent.cancel 路径
  mcp.tool(
    'task_cancel',
    '打断一个 queued/running 的任务(cancel agent 当前回合, 回收部分输出); 已结束的任务为幂等 no-op。',
    { taskId: z.string().describe('task_inbox 返回的 taskId') },
    async ({ taskId }) => {
      const item = taskQueue.get(taskId)
      if (!item) return err(JSON.stringify({ error: `task not found: ${taskId}` }))
      if (item.status === 'done' || item.status === 'error') {
        return out(JSON.stringify({ taskId, sessionId: item.sessionId, status: item.status, cancelled: false, note: 'already finished' }))
      }
      const cancel = taskCancelHooks.get(taskId)
      if (!cancel) {
        // agent 未就绪(等锁/排队中): 置 cancelled 标记, executeTask 在锁释放后执行前检查并中止
        item.cancelled = true
        // sessionId 未就绪时为 undefined(JSON 序列化自动省略)
        return out(JSON.stringify({ taskId, sessionId: item.sessionId, status: item.status, cancelled: true, note: 'cancel requested before agent started; will abort on start' }))
      }
      try {
        await cancel()
      } catch (e) {
        return err(JSON.stringify({ error: `cancel failed: ${(e as Error)?.message ?? String(e)}` }))
      }
      return out(JSON.stringify({ taskId, sessionId: item.sessionId, status: item.status, cancelled: true }))
    },
  )

  // 给已有会话改名(走 sessionTitle 服务, 便于会话列表归档)
  mcp.tool(
    'rename_session',
    '给已有会话改名(走 sessionTitle 服务的 rename), 便于会话列表归档区分。',
    {
      sessionId: z.string().describe('要改名的会话 id(来自 agent_run 结果里的 sessionId 字段)'),
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

  // 显式退役池会话: 外部主动释放常驻句柄(会话保留在持久化, 仍可凭 sessionId 续接)
  mcp.tool(
    'session_close',
    '显式退役一个常驻池会话(dispose 句柄并移出池; 会话保留在持久化, 仍可凭 sessionId 续接)。只能关闭本插件池里的会话; live/persisted 会话归其创建者所有, 返回 no-op。',
    {
      sessionId: z.string().describe('要退役的会话 id(来自 session_list 或 agent_run 结果的 sessionId 字段)'),
    },
    async ({ sessionId }) => {
      let targetCwd: string | undefined
      for (const [cwd, rec] of liveAgents) {
        if (String(rec.sessionId) === sessionId) { targetCwd = cwd; break }
      }
      if (targetCwd === undefined) {
        return out(JSON.stringify({ sessionId, closed: false, note: 'not in pool (pool only; live/persisted sessions are owned by their creator)' }))
      }
      const rec = liveAgents.get(targetCwd) as ResolvedAgent | undefined
      if (!rec) return out(JSON.stringify({ sessionId, closed: false, note: 'already closed' }))
      // 忙会话不掐: 正在跑任务的会话拒绝退役, 引导先取消任务
      const status = (rec.handle.agent as unknown as { status?: string }).status
      if (status !== 'idle') {
        return out(JSON.stringify({ sessionId, cwd: targetCwd, closed: false, note: 'busy: session has a running task; use task_list / task_cancel first' }))
      }
      liveAgents.delete(targetCwd)
      sessionToCwd.delete(sessionId)
      try {
        await rec.handle.dispose()
      } catch (e) {
        return err(JSON.stringify({ error: `dispose failed: ${(e as Error)?.message ?? String(e)}` }))
      }
      return out(JSON.stringify({ sessionId, cwd: targetCwd, closed: true, note: 'session persisted; resumable by sessionId' }))
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
      // web GUI 持有提问 provider 时, MCP 会话里挂起的 ask_user_question 调用仍可感知(应答在 GUI)
      if (!questionsProviderOurs) {
        for (const sid of (sessionId ? [sessionId] : [...mcpSessionIds])) {
          const agent = liveAgentFor(ctx, sid)
          const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined
          if (detected) {
            prompts.push({ sessionId: sid, type: 'question', id: detected.id, questions: detected.questions, note: 'routed to the web GUI provider; answer in the DSH web UI' })
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
      promptId: z.string().describe('pending_prompts / progress 返回的 prompt id'),
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
      // 未挂起: 若是 GUI 路由的挂起提问则给出明确指引
      const agent = liveAgentFor(ctx, sessionId)
      const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined
      if (detected !== undefined && detected.id === promptId && !questionsProviderOurs) {
        return err(JSON.stringify({ error: 'this question is routed to the web GUI provider; answer it in the DSH web UI (the MCP-side provider slot is owned by the GUI in this deployment)' }))
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
  if (config.maxQueue !== undefined) runtimeConfig.maxQueue = config.maxQueue
  if (config.taskTtlMs !== undefined) runtimeConfig.taskTtlMs = config.taskTtlMs
  if (config.maxAgents !== undefined) runtimeConfig.maxAgents = config.maxAgents
  if (config.taskTimeoutMs !== undefined) runtimeConfig.taskTimeoutMs = config.taskTimeoutMs
  if (config.authToken) runtimeConfig.authToken = config.authToken
  if (config.workspaceRoots) runtimeConfig.workspaceRoots = config.workspaceRoots

  const port = config.port ?? 8090
  // 安全默认: 仅监听本机。暴露公网/局域网前必须自行加认证+反代+TLS(见 README 警告)
  const host = config.host ?? '127.0.0.1'
  console.log('[harness-mcp-server] apply called, port=', port)

  // ── 审批应答者: 正被 MCP 任务驱动的会话, 审批一律转达调用方, 绝不自动放行 ──
  // prepend 抢在 web GUI 应答者之前认领审批; 仅当会话属 MCP(mcpSessionIds)且此刻正被 MCP 任务驱动
  // (mcpBusySessionIds: 当前有 agent_run/task_inbox 在跑)才接管转达调用方(Hermes);
  // 否则(例如用户经 web UI 直接向 MCP 创建过的会话发消息、此刻没有 MCP 任务在跑)不接管,
  // next() 交给 web GUI 应答链 —— 避免接管后 Hermes 调用方不知情/无法响应、web UI 也弹不出
  // 审批窗的双端死锁(旧版只看 mcpSessionIds 的死锁根因)。
  // approve → 'allowed-once'(一次性授权), deny → 'rejected', 任务取消/超时 → signal abort → 'cancelled'。
  const onApprovalRequest = (req: ApprovalRequestView, next: () => Promise<string>): Promise<string> => {
    if (req.signal?.aborted) return Promise.resolve('cancelled')
    // 防御: agent.id 为权威; 个别实现只挂 session.id 时兜底
    const agentId = String(req.agent.id ?? (req.agent.session as { id?: unknown } | undefined)?.id)
    // 仅当会话属 MCP 且正被 MCP 任务驱动才接管转达调用方; 否则交给 web GUI 应答链
    if (!mcpSessionIds.has(agentId) || !mcpBusySessionIds.has(agentId)) return next()
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

  // ── 提问 provider: 单槽能力缝; web GUI 已占用时降级(提问路由到 GUI, MCP 仍可感知但需在 GUI 应答) ──
  const userQuestions = ctx.get('userQuestions') as {
    registerProvider?: (p: { ask: (request: unknown) => Promise<unknown> }) => () => void
  } | undefined
  if (userQuestions?.registerProvider) {
    try {
      userQuestions.registerProvider({
        ask: (request) => {
          const r = request as {
            questions: Array<{ id: string; question: string; detail?: string; options?: { label: string }[] }>
            agent?: { id: unknown; session?: unknown }
            signal?: AbortSignal
          }
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
        },
      })
      questionsProviderOurs = true
      console.log('[harness-mcp-server] user-questions provider registered (question prompts answerable via prompt_respond)')
    } catch {
      questionsProviderOurs = false
      console.warn('[harness-mcp-server] user-questions provider already registered (web GUI); question prompts route to the GUI and remain visible via progress/pending_prompts')
    }
  }

  const servers = new Map<string, McpServer>()
  const transports = new Map<string, StreamableHTTPServerTransport>()

  const server = http.createServer(async (req, res) => {
    // Bearer token 认证(配置了 authToken 时强制所有请求校验)
    if (runtimeConfig.authToken) {
      const auth = req.headers['authorization']
      if (auth !== `Bearer ${runtimeConfig.authToken}`) {
        res.writeHead(401, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32001, message: 'Unauthorized' }, id: null }))
        return
      }
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
  })

  server.listen(port, host, () => {
    console.log(`[harness-mcp-server] MCP server listening on ${host}:${port}`)
  })
  server.on('error', (e) => {
    console.error('[harness-mcp-server] HTTP server error:', e.message)
  })

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
      taskQueue.clear()
      taskCancelHooks.clear()
      mcpSessionIds.clear()
      mcpBusySessionIds.clear()
      sessionModelOverrides.clear()
      pendingNotices.clear()
      for (const pa of pendingApprovals.values()) pa.resolve('cancelled')
      pendingApprovals.clear()
      for (const pq of pendingQuestions.values()) pq.reject(new Error('harness-mcp-server unloaded'))
      pendingQuestions.clear()
    }
  }, 'harness-mcp-server')
}
