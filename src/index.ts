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
 *   - attach_session      : 把会话归组到其 cwd 对应的工作区(手动补给站)
 *   - rename_session      : 给已有会话改名
 *
 * 上下文占用: session_list/task_list 通过 ctx.tokenMeter.measure(session) 输出事件数与启发式 token 数
 * (固定密度定价, 与 dsh token-meter 同源); tokenMeter 服务缺失时该字段为 null。
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
import { createUserMessage } from '@deepseek-ai/dsh-llm'
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
export const VERSION = '0.9.0'

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

/** 工具回调统一返回 MCP text content */
function out(content: string) {
  return { content: [{ type: 'text' as const, text: content }] }
}

/** 错误响应: 结构化 JSON 文本 + isError 标记(MCP 客户端可据此识别失败, 不写回记忆) */
function err(content: string) {
  return { content: [{ type: 'text' as const, text: content }], isError: true as const }
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
 *  modelOpts 提供按次调用的 provider/model 覆盖(只对新建/resume 的会话生效; 池命中的复用会话保持原模型)。 */
async function getAgent(
  ctx: Context,
  cwd: string,
  sessionId?: string,
  title?: string,
  fresh?: boolean,
  modelOpts?: { provider?: string; model?: string },
): Promise<ResolvedAgent> {
  const effectiveModel = modelOpts?.model ?? runtimeConfig.model
  const agentOptions = {
    provider: modelOpts?.provider ?? runtimeConfig.provider,
    // model 为空则省略, 让 dsh 跟随用户/默认设置; 显式配置则覆盖
    ...(effectiveModel ? { model: effectiveModel } : {}),
  }
  // 指定 sessionId: 接管已有会话(长任务分多轮投喂 / 中断后恢复 / UI 手开的会话)
  if (sessionId) {
    // 先看本进程常驻池(指定 sessionId 时定位到对应 cwd 的常驻会话; 命中 LRU 移到末尾, 保留上游语义)
    const targetCwd = sessionToCwd.get(sessionId)
    if (targetCwd !== undefined) {
      const existing = liveAgents.get(targetCwd)
      if (existing) {
        liveAgents.delete(targetCwd)
        liveAgents.set(targetCwd, existing)
        return existing
      }
    }
    const sid = SessionId(sessionId)
    // 不在常驻池: 看 live(UI 手开的、别的插件持有的会话), 直接接管、不持有 dispose(归其 owner)
    const live = ctx.agents.get(sid)
    if (live) {
      // live 会话也补挂工作区(幂等): 用户手开的会话若尚未归组, 这里一并挂名
      await attachSessionCwd(ctx, sid, live.session.header.cwd)
      // no-op dispose 兜底: executeTask 只在 disposeAfter 为 true 时调用 dispose
      return { sessionId: sid, handle: { agent: live, dispose: () => Promise.resolve() }, disposeAfter: false }
    }
    // live 也没有: 从持久化会话存储 resume 并接管(进程重启前的会话、LRU 淘汰后被释放的会话)
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
          await ctx.agentPresets.mount(agentCtx, runtimeConfig.preset)
        },
      })
    } catch (e) {
      // 恢复失败返回明确错误(沿用上游错误风格): 不在常驻池、不是 live、持久化里也没有(或 resume 失败)
      throw new Error(`session not found for resume: ${sessionId} (not live and not persisted; ${(e as Error)?.message ?? e})`)
    }
    await attachSessionCwd(ctx, sid, handle.agent.session.header.cwd)
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
    return createPoolAgent(ctx, cwd, title, agentOptions)
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
  return createPoolAgent(ctx, cwd, title, agentOptions)
}

/** 新建一个 cwd 的常驻池会话: LRU 淘汰(只淘汰 idle 的) → agents.create(挂 preset) → 入池 → 工作区分组 → 可选命名 */
async function createPoolAgent(ctx: Context, cwd: string, title?: string, agentOptions?: { provider?: string; model?: string }): Promise<ResolvedAgent> {
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
  const handle = await ctx.agents.create({
    sessionId: newSessionId,
    // meta.agentPreset 自 dsh 0.1.1-rc.2 起是官方字段(session header 记录/预置选择器消费);
    // 但 preset 仍需在 setup 里显式 mount —— agentPresets 不做自动挂载, 只对未挂载 agent 告警。
    meta: { cwd: canonical, agentPreset: runtimeConfig.preset },
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
      await ctx.agentPresets.mount(agentCtx, runtimeConfig.preset)
    },
  })
  const rec = { sessionId: newSessionId, handle }
  liveAgents.set(cwd, rec)
  sessionToCwd.set(String(newSessionId), cwd)

  // 分组: 把会话归属到 cwd 对应的工作区(resolveByPath ?? create + attachSession; 可选依赖; headless 环境自动跳过)
  void (async () => {
    try {
      const ws = await ensureWorkspace(ctx, canonical)
      if (ws?.attachSession) await ws.attachSession(newSessionId)
    } catch (e) {
      console.warn('[harness-mcp-server] workspace attach failed:', String(e))
    }
  })()

  // title 命名(可选): 创建会话后立即命名(走 sessionTitle 服务的 rename)
  if (title) {
    try {
      const session = handle.agent.session as { id?: unknown }
      const st = ctx.get('sessionTitle') as { rename?: (s: unknown, t: string) => unknown } | undefined
      st?.rename?.(session, title)
    } catch (e) {
      console.warn('[harness-mcp-server] session title set failed:', String(e))
    }
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
  assistantText: string
  toolCalls: { name: string; args: string }[]
  toolResults: string[]
  changes: string
  verification: string
  leftovers: string
  /** true = 任务超时被自动 cancel(部分输出已回收, 可用 sessionId 续接) */
  timeout?: boolean
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

/** 经 ctx.llm.resolveModel 解析某 provider/model 的上下文窗口; 不可解析返回 null */
async function modelWindowOf(ctx: Context, provider: string | undefined, model: string | undefined): Promise<number | null> {
  if (!provider || !model) return null
  const key = `${provider}:${model}`
  const cached = modelWindowCache.get(key)
  if (cached !== undefined) return cached
  let window: number | null = null
  try {
    const llm = ctx.get('llm') as { resolveModel?: (p: string, m: string, s?: AbortSignal) => Promise<{ context?: { contextWindow?: number } }> } | undefined
    const info = await llm?.resolveModel?.(provider, model)
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

/** 完整上下文占用: 事件数 + 表面 token 数 + 最近请求压力 + 模型窗口 + 占用比(百分比, 1 位小数);
 *  tokenMeter 缺失返回 null; 窗口不可解析时 window/ratio 为 null。 */
async function contextUsage(
  ctx: Context,
  session: unknown,
  agent?: { options?: { provider?: string; model?: string } },
): Promise<{ events: number; tokens: number; pressure: number; window: number | null; ratio: number | null } | null> {
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

/** 任务进行中的步骤信息: 从任务开始点(baseline)之后的日志增量里提取, 供客户端汇报进度 */
function taskProgressOf(ctx: Context, item: TaskItem): Record<string, unknown> {
  const info: Record<string, unknown> = { status: item.status }
  if (item.status === 'queued') return info
  const session = liveAgentFor(ctx, item.sessionId)?.session as { log?: unknown[] } | undefined
  if (!session?.log) return info
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

/** 核心执行: 组装任务(注入记忆上下文+结构化要求) → agent 执行 → 读结构化结果。
 *  opts.onAgent 在 agent 就绪后回调一次, 供 task_cancel 注册打断钩子;
 *  opts.fresh = true 且未传 sessionId 时强制全新会话(跳过池复用, 见 getAgent);
 *  opts.provider/model 为按次调用的模型覆盖(对新建/resume 会话生效);
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
    const { sessionId, handle, disposeAfter } = await getAgent(ctx, workdir, resumeSessionId, title, opts?.fresh, { provider: opts?.provider, model: opts?.model })
    const baseline = ((handle.agent.session as unknown as { log?: unknown[] }).log ?? []).length

    // 组装完整任务文本: 记忆上下文 + 任务 + 结构化输出要求
    const fullTask = [
      context ? `【记忆/上下文(供参考, 来自 Hermes 大脑)】\n${context}\n` : '',
      `【任务】\n${task}\n`,
      `【完成后必须】用一行 JSON 总结(不要 markdown 代码块包裹, 直接输出这一行):`,
      `{"changes":"改了什么","verification":"怎么验证的","leftovers":"遗留问题"}`,
    ].filter(Boolean).join('\n')

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
    } finally {
      if (timer !== undefined) clearTimeout(timer)
    }

    // 结构化读输出
    const result: TaskResult = {
      taskId: '', sessionId, assistantText: '', toolCalls: [], toolResults: [],
      changes: '', verification: '', leftovers: '',
    }
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
    result.timeout = timedOut
    if (timedOut && !result.leftovers) {
      result.leftovers = '任务超时被自动取消, 以上为部分进展; 可用 sessionId 续接继续'
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
  /** 按次执行超时(覆盖全局 taskTimeoutMs; 0 = 不限制) */
  timeoutMs?: number
  /** agent 就绪时的会话日志起点(供进度提取本任务增量) */
  baseline?: number
  status: 'queued' | 'running' | 'done' | 'error'
  result?: TaskResult
  error?: string
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
        const events: { seq?: number; type?: string; text?: string }[] = []
        for (const ev of log.slice(-(limit ?? 100))) {
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
        return out(JSON.stringify({ sessionId, total: log.length, returned: events.length, events }, null, 2))
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
    '同步执行任务(改代码/分析/跑命令), 返回结构化结果。可传 sessionId 续接已有会话(长任务分多轮投喂); 可传 newSession:true 强制全新会话(不复用池); 可传 model/provider 按次选模型; 传 timeoutMs(毫秒)超时后自动转为异步任务(返回 taskId, 用 task_result/task_cancel/task_wait 跟进)。',
    {
      task: z.string().describe('要 Harness 执行的自然语言任务'),
      context: z.string().optional().describe('Hermes 记忆/上下文, 注入给 agent 参考'),
      cwd: z.string().optional().describe('工作目录(不传 sessionId 时必填; 可用 workspace_list 查看可用目录)'),
      sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果里的 sessionId 字段)'),
      newSession: z.boolean().optional().describe('true = 强制全新会话(跳过该 cwd 的池复用, 旧会话退役但持久化保留, 仍可凭 sessionId 续接); 缺省 = 复用该 cwd 的常驻会话'),
      model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效; 池复用的会话保持原模型)'),
      provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
      timeoutMs: z.number().int().min(0).optional().describe('同步等待上限毫秒数(默认 taskTimeoutMs=60 分钟; 建议按客户端 HTTP 超时设小, 如 120000; 0 = 不转异步)'),
      title: z.string().optional().describe('新会话的标题(创建时命名, 便于会话列表归档)'),
    },
    async ({ task, context, cwd, sessionId, newSession, model, provider, timeoutMs, title }) => {
      if (!cwd && !sessionId) {
        return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }))
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
        ...(title ? { title } : {}),
      }
      taskQueue.set(id, item)
      const background: Promise<TaskResult | undefined> = (async () => {
        try {
          item.result = await executeTask(ctx, item.task, item.context, item.cwd, item.sessionId, item.title, {
            fresh: item.newSession === true,
            model: item.model,
            provider: item.provider,
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
          item.sessionId = item.result.sessionId
          item.status = 'done'
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
        return out(JSON.stringify(truncateResult(result), null, 2))
      } catch (e) {
        if (e !== TASK_TIMEOUT) throw e
        // 转异步: 任务在后台继续跑, 客户端用 taskId 轮询/等待/取消; 附带当前进度供汇报
        return out(JSON.stringify({
          status: 'async',
          taskId: id,
          progress: taskProgressOf(ctx, item),
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
    'Hermes 把结构化任务(任务+记忆上下文)推入 Harness 队列, 异步执行, 返回 taskId。记忆喂编码的入口。可传 newSession:true 强制全新会话。',
    {
      task: z.string().describe('任务内容'),
      context: z.string().optional().describe('Hermes 记忆/上下文, 随任务注入给 agent'),
      cwd: z.string().optional().describe('工作目录'),
      sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果)'),
      newSession: z.boolean().optional().describe('true = 强制全新会话(不复用该 cwd 的常驻会话); 缺省 = 复用'),
      model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效)'),
      provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
      timeoutMs: z.number().int().min(0).optional().describe('本次任务的执行超时毫秒数(默认 taskTimeoutMs=60 分钟; 超时自动 cancel 并回收部分输出; 0 = 不限制)'),
      title: z.string().optional().describe('新会话的标题(创建时命名)'),
    },
    async ({ task, context, cwd, sessionId, newSession, model, provider, timeoutMs, title }) => {
      if (!cwd && !sessionId) {
        return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }))
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
          item.sessionId = item.result.sessionId // 回填实际使用的会话, 供 task_list 附上下文占用/后续续接
          item.status = 'done'
        } catch (e) {
          item.error = String(e)
          item.status = 'error'
        } finally {
          taskCancelHooks.delete(id)
          item.finishedAt = Date.now()
        }
      })()
      return out(JSON.stringify({ taskId: id, status: 'queued' }))
    },
  )

  // 取回任务结果(结构化 changes/verification/leftovers; 未完成时带进行中进度)
  mcp.tool(
    'task_result',
    '取回 task_inbox 提交任务的结构化结果(changes/verification/leftovers); 未完成时返回 progress 供汇报进度。',
    { taskId: z.string().describe('task_inbox 返回的 taskId') },
    async ({ taskId }) => {
      const item = taskQueue.get(taskId)
      if (!item) return err(JSON.stringify({ error: `task not found: ${taskId}` }))
      return out(JSON.stringify({
        taskId: item.id,
        status: item.status,
        error: item.error,
        cancelled: item.cancelled === true || undefined,
        progress: taskProgressOf(ctx, item),
        result: item.result ? truncateResult(item.result) : undefined,
      }, null, 2))
    },
  )

  // 阻塞等待任务完成: 一次往返替代 N 次轮询(服务端每 500ms 查一次, 到 timeoutMs 或任务落定返回);
  // 无论完成还是超时, 都附带 progress(进行中的步骤/已用工具/最新文本)供客户端汇报进度
  mcp.tool(
    'task_wait',
    '阻塞等待一个任务完成/失败后返回其结果(服务端等待, 一次往返替代多次轮询); 超过 timeoutMs 返回当前状态与 progress(正在执行的步骤/工具调用)。客户端 HTTP 超时应大于 timeoutMs。',
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
        status: item.status,
        error: item.error,
        cancelled: item.cancelled === true || undefined,
        progress: taskProgressOf(ctx, item),
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
        // 任务所用会话仍 live 时附上上下文占用(池优先; 已退役/未加载则 null); 未完成任务附进行中进度
        const agent = liveAgentFor(ctx, t.sessionId)
        const context = agent ? await contextUsage(ctx, agent.session, agent) : null
        const progress = t.status === 'queued' || t.status === 'running' ? taskProgressOf(ctx, t) : undefined
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
        return out(JSON.stringify({ taskId, status: item.status, cancelled: false, note: 'already finished' }))
      }
      const cancel = taskCancelHooks.get(taskId)
      if (!cancel) {
        // agent 未就绪(等锁/排队中): 置 cancelled 标记, executeTask 在锁释放后执行前检查并中止
        item.cancelled = true
        return out(JSON.stringify({ taskId, status: item.status, cancelled: true, note: 'cancel requested before agent started; will abort on start' }))
      }
      try {
        await cancel()
      } catch (e) {
        return err(JSON.stringify({ error: `cancel failed: ${(e as Error)?.message ?? String(e)}` }))
      }
      return out(JSON.stringify({ taskId, status: item.status, cancelled: true }))
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
    '列出可续接的会话(常驻池 / live / 持久化三层去重, 池优先), 含上下文占用 events/tokens/pressure/window/ratio(经 tokenMeter 测量 + llm 模型窗口); 持久化层未加载日志为 null。',
    {},
    async () => {
      const rows = new Map<string, {
        cwd?: string; source: 'pool' | 'live' | 'persisted'; title?: string
        context: { events: number; tokens: number; pressure: number; window: number | null; ratio: number | null } | null
      }>()
      // 常驻池(本插件持有, 优先级最高; 上下文可直接测量)
      for (const [cwd, rec] of liveAgents) {
        rows.set(String(rec.sessionId), { cwd, source: 'pool', context: await contextUsage(ctx, rec.handle.agent.session, rec.handle.agent) })
      }
      // live 会话(ctx.agents.list(); 可读标题)
      const titleSvc = ctx.get('sessionTitle') as { get?: (s: unknown) => { title?: string } | undefined } | undefined
      const liveAgentsList = (ctx.agents as unknown as { list?: () => { session: { id: unknown; header?: { cwd?: string } }; options?: { provider?: string; model?: string } }[] }).list?.() ?? []
      for (const agent of liveAgentsList) {
        const id = String(agent.session.id)
        const prev = rows.get(id)
        rows.set(id, {
          cwd: agent.session.header?.cwd ?? prev?.cwd,
          source: prev?.source ?? 'live',
          title: prev?.title ?? titleSvc?.get?.(agent.session)?.title,
          context: prev?.context ?? await contextUsage(ctx, agent.session, agent),
        })
      }
      // 持久化(未在上两层出现的会话; 日志未加载, 上下文未知)
      const persistence = ctx.get('sessionPersistence') as { list?: () => Promise<{ id: unknown; cwd?: string }[]> } | undefined
      for (const h of (await persistence?.list?.()) ?? []) {
        const id = String(h.id)
        if (!rows.has(id)) rows.set(id, { cwd: h.cwd, source: 'persisted', context: null })
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
    }
  }, 'harness-mcp-server')
}
