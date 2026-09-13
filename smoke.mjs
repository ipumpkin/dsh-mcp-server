// Dev-only smoke test (not shipped): drives apply() with a minimal fake ctx and verifies the
// contract surface on top of upstream. Fast, self-contained, no real dsh process needed.
//
// v0.12.0 工具面(20 个): session_send / session_status / session_tail / session_wait / session_cancel
//   + preset_list(原 mode_list) + attach_session / rename_session / session_list / session_read /
//   session_compact / session_set_model / session_inject / pending_prompts / prompt_respond /
//   harness_status / harness_list_tools / model_list / workspace_list / echo。
// 本文件只做「契约级」冒烟(响应形状/字段/错误文案/事件落点), 不做真实 harness 的端到端
// —— 后者归 verify-session-turn.mjs(独立 e2e, 与本文件互补, 不重复)。
//
// 覆盖场景:
//   1. 会话续接三级: live 接管 / 持久化 resume(+turn 落定后 flush&dispose) / 未知会话明确报错
//   2. realpath 规范化: create 的 meta.cwd 为 realpath 值; 目录不存在时回退 resolve 不阻断; cwd 白名单
//   3. attach_session 工具(live/持久化/未知三态 + 越界路径拒绝) + 启动存量捞回(两源)
//   4. 派活契约: session_send 立即返回 {sessionId, status, inboxDepth, openTurn} 且不阻塞;
//      同会话运行中再投喂 → 只入队(inboxDepth=1, openTurn 不前进) —— 取代旧的「任务队列/等锁取消」
//   5. 主动查询: session_status 的 phase/openTurn/lastTurn/changes/verification/leftovers/context/logEvents;
//      未落定 turn → phase=running; 已闭合 → idle + reason.kind; 失败 turn → reason.kind='error' + error.code
//   6. session_tail 契约: 只出表面事件(内部 chunk/step 被滤掉)、每条 {seq,type,text,turn}、filter=surface|tools、sinceSeq
//   7. session_wait: turn-end 落定 / idle 立即满足 / input 超时 {timeout:true,status} / 卡死 turn 超时; 非 live 立即返回
//   8. session_cancel: 打断在飞 turn → reason.kind='aborted'; keepInbox 语义; 非 live no-op; MCP 在飞计数归零
//   9. 外部显式控制会话复用: newSession 强制全新会话(旧池会话退役 dispose)、session_list 三层盘点 + 上下文占用
//   10. 上下文占用与压缩: session_list/session_status 的 events/tokens/pressure/window/ratio; session_compact 走 ctx.compaction;
//       窗口不可解析时 window/ratio 为 null 不崩溃
//   11. 运维/审计/选型: harness_status(agentPool + mcpInFlightTurns + config)、session_read 事件流(limit 按表面事件计)、
//       workspace_list 分组、model_list 目录、session_send 按次 model 覆盖(透传 create)
//   12. preset_list 目录: presets/sandboxModes/approvalPolicies/permissionPresets/modes + deployment + only 过滤 + withDetail
//       (字段名与 mode_list 时期完全一致, 只换工具名)
//   13. 按 preset/mode 建会话: preset 写进 meta.agentPreset、会话日志落 sandbox/mode + approval/policy 持久事件、
//       session_list 的 mode 快照验证生效; 非法 mode 报错; sandbox/approval 不可续接存量会话, preset 单独允许 resume
//   14. notice 安全落点(回归修复): 审批/提问拦截只入队不写日志; 工具完成后经 tools/post-execute 并入
//       additionalContexts, 在 tool/result 之后追加 —— 不打断 assistant(tool_calls) 与其 tool/result 的
//       模型消息序列(无 INVALID_REQUEST); 反向控制验证旧版插入位置会被校验器检出; notice 原生呈现契约
//       (form:'notice' + 折叠行 summary 非空)
//   15. 会话自动命名: 新建会话未传 title 时按任务内容派生可读名称(sessionTitle.rename → session/title 事件,
//       session_list 行可见); 显式 title 优先; 复用不改名; sessionTitle 服务缺失时静默降级
//   16. 挂起期即时投递: 审批/提问被 MCP 拦截的 ⏳ 提示在拦截当下立即追加到 agent 的 next-step inbox
//       (source: { kind: 'user' }, web GUI steering 同款形状), 拦截期绝不写会话日志; inbox 不可用时退回入队
//   17. 提问应答者换代(dsh 0.1.5 回归修复): 新宿主删除 registerProvider, 提问改为 Agent 作用域 waterfall
//       'user-questions/request' —— 验证新宿主走 listener、prepend 抢在 GUI 之前、MCP 在飞 turn 期的提问被接管
//       (session_status.prompts / pending_prompts / prompt_respond 全程可用)、非 MCP 会话的提问 next() 透传给 GUI;
//       以及旧宿主(≤0.1.1, 有 registerProvider)仍走老路径
//   18. 执行层异常: followup 抛错 → session_send 返回 isError(send failed) 且不残留 MCP 在飞 turn 计数
//
// 已随 v0.11.0/0.12.0 废弃的旧场景(工具已删, 不再覆盖): 任务超时保护(taskTimeoutMs→whenIdle 卡死自动 cancel)、
//   异步任务队列(task_inbox/task_result/task_list/task_cancel)、session_close 手动退役、agent_run 超时转异步、
//   task_wait 阻塞等待、同步结果回填 taskId、mode_list 工具名。对应的新契约由 4/5/6/7/8 与 verify-session-turn.mjs 覆盖。
import { realpathSync } from 'node:fs'
import { resolve } from 'node:path'
import { apply } from './lib/index.js'

const attachedIds = []
const created = []
const resumed = []
const disposed = []
const flushed = []

// smoke 文件所在目录的 realpath(win32 反斜杠规范路径) —— 与 workspace.path / fs.realpath 结果同 canon
const FAKE_CWD = realpathSync(new URL('.', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'))
// 卡死 cwd: 只开 turn 不闭合, 供 session_wait 超时 / session_cancel / keepInbox 用例
const HANG_CWD = resolve(FAKE_CWD, 'hang-zone')
// 审批弹窗 / 提问弹窗 / 模型失败 / followup 抛错专用 cwd
const APPROVAL_CWD = resolve(FAKE_CWD, 'approval-zone')
const QUESTION_CWD = resolve(FAKE_CWD, 'question-zone')
const ERROR_CWD = resolve(FAKE_CWD, 'error-zone')
const THROW_CWD = resolve(FAKE_CWD, 'throw-zone')
const MODE_CWD = resolve(FAKE_CWD, 'mode-zone')
const TITLE_CWD = resolve(FAKE_CWD, 'title-zone')

// 假 agent 驱动方式(按 cwd 选择)
const MODE_OF_CWD = new Map([
  [HANG_CWD, 'hang'],
  [APPROVAL_CWD, 'approval'],
  [QUESTION_CWD, 'question'],
  [ERROR_CWD, 'error'],
  [THROW_CWD, 'throw'],
])

// ── 事件总线: session/event 是宿主在 append 提交后广播的(它驱动插件的在飞 turn 计数与临时句柄释放) ──
const eventHandlers = new Map()
function emit(name, ...args) {
  for (const h of [...(eventHandlers.get(name) ?? [])]) h(...args)
}
// 复刻 cordis waterfall 语义(outermost-first, 不调 next() 即 veto) —— 与真实宿主一致
function runWaterfall(name, request, fallback) {
  const chain = [...(eventHandlers.get(name) ?? [])]
  const run = async () => {
    const h = chain.shift()
    return h ? h(request, run) : fallback()
  }
  return run()
}

// dsh-user-questions ≥0.1.5 的服务形态: 只有 ask()(registerProvider 已被删除), ask 内部派发
// Agent 作用域 waterfall 事件 'user-questions/request'。
const NO_ANSWERER = () => Promise.reject(new Error('no user-questions answerer accepted the request'))
const askUserQuestions = (request) => runWaterfall('user-questions/request', request, NO_ANSWERER)
const fakeUserQuestions = { ask: askUserQuestions }

const fakeWs = {
  id: 'ws-fake',
  title: 'fake',
  path: FAKE_CWD,
  sessionIds: [],
  attachSession: async (id) => { attachedIds.push(id) },
}
const wsRegistry = {
  list: () => [fakeWs],
  resolveByPath: async (p) => (p === FAKE_CWD ? fakeWs : undefined),
  create: async () => fakeWs,
}

const inboxAppends = []
const agentsById = new Map()

// 假 agentPresets: 与真实 dsh-agent-presets 服务同形(list/resolve/defaultId/mount)
const fakePresets = [
  { id: 'standard', name: '标准模式', description: '功能完整的编码 Agent', trust: 'system', order: 1, path: '/presets/standard' },
  { id: 'code', name: 'PTC 模式', description: '标准模式全部能力 + Code Mode SDK', trust: 'system', order: 2, path: '/presets/code' },
  { id: 'cordis', name: '创造模式', description: '创建自定义 Agent preset', trust: 'system', order: 4, path: '/presets/cordis' },
  { id: 'minimal', name: '极简模式', description: '双工具编码 Agent', trust: 'system', order: 3, path: '/presets/minimal' },
]
const fakeAgentPresets = {
  list: async () => fakePresets,
  resolve: async (id) => {
    const p = fakePresets.find((x) => x.id === id)
    if (!p) {
      const e = new Error(`unknown preset ${id}`)
      e.available = fakePresets.map((x) => x.id)
      throw e
    }
    return p
  },
  defaultId: 'standard',
  mount: async (_agentCtx, id) => fakePresets.find((x) => x.id === id) ?? { id },
}

// 假 sandboxPolicy / approval / permissionPresets: 与真实 dsh 服务同形
const fakeSandboxPolicy = { defaultMode: 'read-only', workspaceRoot: '/ws' }
const fakeApprovalService = { config: { policy: 'ask' } }
const fakePermissionPresets = {
  names: ['workspace-write', 'danger-full-access'],
  resolve: (name) => (name === 'workspace-write'
    ? { name, sandbox: 'workspace-write', approval: 'ask', description: '工作区可写 + 每次审批' }
    : name === 'danger-full-access'
      ? { name, sandbox: 'danger-full-access', approval: 'never', description: '完全访问 + 永不询问' }
      : undefined),
  defaultPreset: 'workspace-write',
}

// 每个 turn 闭合时 agent 吐出的结构化总结(供 session_status 的 parseSummary 提取)
const SUMMARY = { changes: 'smoke changes', verification: 'smoke verification', leftovers: '无' }
const SUMMARY_JSON = JSON.stringify({ changes: SUMMARY.changes, verification: SUMMARY.verification, leftovers: SUMMARY.leftovers })

// ── 假 agent: followup 同步开 turn, 异步(10ms)闭合 —— 与真实 agent-loop 同序:
//    session_send 立即返回时 turn 已打开(openTurn 可读), turn/end 稍后落定(因此查询侧要轮询)。 ──
function makeAgent(id, cwd, mode = 'ok', delayMs = 10) {
  const log = []
  const events = []
  const session = {
    id, log, events,
    header: { version: 0, id, createdAt: Date.now(), cwd },
    append: (type, data) => {
      const ev = { seq: log.length, time: Date.now(), type, data }
      log.push(ev)
      events.push(ev)
      // 宿主语义: 事件提交后广播 session/event(插件据此递减在飞 turn 计数 / 释放临时 resume 句柄)
      emit('session/event', session, ev)
      return ev
    },
  }
  const agent = {
    id,
    session,
    options: { provider: 'deepseek-official', model: 'deepseek-v4-flash' },
    status: 'idle',
    _turn: 0,
    _cancelCalls: [],
    _lastTask: '',
    _mode: mode,
    inbox: {
      nextTurn: [],
      nextStep: [],
      append: (t, m) => {
        inboxAppends.push({ t, m })
        ;(t === 'next-step' ? agent.inbox.nextStep : agent.inbox.nextTurn).push(m)
      },
      clear: () => { agent.inbox.nextTurn.length = 0; agent.inbox.nextStep.length = 0 },
    },
    whenIdle: async () => {},
    runMaintenance: async (task) => task(new AbortController().signal),
    closeTurn: (turn, kind, extra) => {
      session.append('assistant/message', {
        turn, step: 1,
        message: {
          id: `a-${turn}`, role: 'assistant',
          content: [{ type: 'text', text: `done: ${agent._lastTask.slice(0, 40)}\n${SUMMARY_JSON}` }],
          source: { kind: 'model', provider: 'deepseek-official', model: 'deepseek-v4-flash' },
        },
      })
      session.append('turn/end', { turn, reason: { kind, ...(extra ?? {}) } })
      agent.status = 'idle'
      afterClose()
    },
    // 真实语义: followup 只入队 + 唤醒驱动, turn/start 在随后的宏任务里才落日志。
    // 因此 session_send 返回时 turn 可能尚未打开 —— 查询侧必须以 session_status 轮询为准。
    followup: (msg) => {
      agent._lastTask = (msg?.content ?? []).filter((c) => c.type === 'text').map((c) => c.text).join('\n')
      if (mode === 'throw') throw new Error('followup exploded')
      agent.inbox.nextTurn.push(msg)
      if (agent.status === 'running') return // 运行中: 只入队, 由下一个 turn 边界 claim
      setTimeout(claimTurn, 0)
    },
    cancel: (cause, options) => {
      agent._cancelCalls.push({ cause, options })
      if (agent.status === 'running') {
        session.append('turn/end', { turn: agent._turn, reason: { kind: 'aborted', reason: cause } })
        agent.status = 'idle'
      }
      if (options?.keepInbox !== true) agent.inbox.nextTurn.length = 0
    },
  }
  // 驱动循环: turn 边界 claim 恰好一条排队输入并开 turn(与真实 agent-loop 的 claim 语义一致)
  function claimTurn() {
    if (agent.status === 'running' || agent.inbox.nextTurn.length === 0) return
    agent.inbox.nextTurn.shift()
    agent._turn += 1
    const turn = agent._turn
    session.append('turn/start', { turn })
    agent.status = 'running'
    if (mode === 'error') {
      session.append('assistant/chunk', { turn, step: 1, chunk: { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: '429: Usage limit reached for 5 hour' } } } })
      session.append('turn/end', { turn, reason: { kind: 'error', error: { code: 'QUOTA', message: '429: Usage limit reached for 5 hour' } } })
      agent.status = 'idle'
      afterClose()
      return
    }
    if (mode === 'hang') return // 永不闭合: phase=running, 只等 session_cancel / session_wait 超时
    if (mode === 'approval') {
      // 模拟 DSH ApprovalService: 审计事件落日志 + waterfall 派发到应答链, 落定后闭合 turn
      session.append('approval/asked', { id: 'appr-e2e-1', toolName: 'bash', callId: 'c-1' })
      const ac = new AbortController()
      const req = { agent, toolName: 'bash', callId: 'c-1', reason: 'write outside workspace root', signal: ac.signal }
      runWaterfall('approval/request', req, () => 'unavailable').then((outcome) => {
        agent.approvalOutcome = outcome
        session.append('approval/decided', { id: 'appr-e2e-1', outcome })
        agent.closeTurn(turn, 'completed')
      })
      return
    }
    if (mode === 'question') {
      // 模拟 dsh-tool-ask-user → ctx.userQuestions.ask() 派发 waterfall, 拿到答案后闭合 turn
      const ac = new AbortController()
      const req = { questions: [{ id: 'q1', question: 'Which DB?', options: [{ label: 'pg' }, { label: 'mysql' }] }], agent, signal: ac.signal }
      askUserQuestions(req).then((a) => { agent.questionAnswer = a; agent.closeTurn(turn, 'completed') },
        (e) => { agent.questionError = e; agent.closeTurn(turn, 'failed') })
      return
    }
    setTimeout(() => { if (agent.status === 'running') agent.closeTurn(turn, 'completed') }, delayMs)
  }
  // turn 落定后若队列仍有排队输入, 驱动继续开下一个 turn(取代旧的「任务队列」)
  function afterClose() {
    if (agent.inbox.nextTurn.length > 0) setTimeout(claimTurn, 0)
  }
  agentsById.set(id, agent)
  return agent
}

const liveAgent = makeAgent('sess-live', FAKE_CWD)
const liveSession2 = { id: 'sess-live2', log: [], events: [], header: { version: 0, id: 'sess-live2', createdAt: 1, cwd: FAKE_CWD } }

const fakeSessions = {
  get: (id) => (id === 'sess-live' ? liveAgent.session : id === 'sess-live2' ? liveSession2 : undefined),
  list: () => [liveSession2],
  flush: async (session) => { flushed.push(session.id); return true },
}

// 假 sessionTitle 服务: 与真实 dsh-session-title 同形(rename 追加 session/title 事件并返回快照;
// get 从事件流折叠最新标题)。titleServiceActive 供「服务缺失静默降级」用例开关。
let titleServiceActive = true
const fakeSessionTitle = {
  rename: (session, title) => {
    const snap = { title, messageSeqs: [], source: { kind: 'user' } }
    const ev = { seq: session?.log?.length ?? session?.events?.length ?? 0, type: 'session/title', data: snap }
    session?.log?.push(ev)
    session?.events?.push(ev)
    return snap
  },
  get: (session) => {
    const evs = session?.events ?? session?.log ?? []
    for (let i = evs.length - 1; i >= 0; i--) {
      if (evs[i]?.type === 'session/title') return evs[i].data
    }
    return undefined
  },
}
const fakePersistence = {
  list: async () => [{ version: 0, id: 'sess-persisted', createdAt: 1, cwd: FAKE_CWD }],
}

// tokenMeter / llm / compaction 的只读 fake: 测量按日志长度估 token(sess-live 特例 30 验证 ratio);
// llm.resolveModelInfo 固定窗口 200(未知模型抛错 → 验证 window/ratio 为 null 的降级路径);
// compactNow 记录调用并返回固定结果
const fakeMeter = {
  measure: (session) => {
    const len = session?.log?.length ?? 0
    const tokens = session?.id === 'sess-live' ? 30 : len * 10
    return { logRevision: len, totalTokens: len * 10, surfaceTokens: tokens }
  },
}
const fakeLlm = {
  resolveModelInfo: async (provider, model) => {
    if (model === 'nope-model') throw new Error(`unknown model ${model}`)
    return { provider, id: model, context: { contextWindow: 200 } }
  },
  listProviders: () => [
    { id: 'deepseek-official', name: 'DeepSeek' },
    { id: 'zai', name: 'ZAI' },
  ],
  listConfigurableProviders: () => [
    { provider: 'zai', displayName: 'ZAI' },
    { provider: 'custom-gw', displayName: 'Custom Gateway' },
  ],
  listModels: async (p) => (p === 'zai'
    ? [{ id: 'glm-5.3', name: 'GLM-5.3' }, { id: 'glm-5.3-flash', name: 'GLM-5.3-Flash' }]
    : [{ id: 'deepseek-v4-flash', name: 'Flash', description: 'fast' }, { id: 'deepseek-v4-pro', name: 'Pro', description: 'big' }]),
}
// agentDefaultModel 默认选择: 供未显式指定 model 时兜底(persona {{model}} 变量需要)
const fakeDefaultModel = {
  currentSelection: () => ({ provider: 'deepseek-official', model: 'deepseek-v4-default' }),
}
const compactCalls = []
const fakeCompaction = {
  compactNow: async (agentCtx) => {
    compactCalls.push({ sessionId: String(agentCtx.session.id) })
    return {
      compactionId: 'cmp-1', summarySeq: 100, endSeq: 101,
      shadowedSeqs: [1, 2, 3], shadowedTokenCount: 500,
      summary: [{ type: 'text', text: 'compacted summary' }],
    }
  },
}

const ctx = {
  tools: { keys: () => [] },
  llm: {},
  agents: {
    // 真实 ctx.agents 是「所有 live agent」的注册表: 池建/接管/resume 出来的都在里面
    get: (id) => (id === 'sess-live' ? liveAgent : agentsById.get(String(id))),
    list: () => [liveAgent, ...[...agentsById.values()].filter((a) => a.id !== 'sess-live')],
    create: async ({ sessionId, meta, agentOptions }) => {
      const id = String(sessionId)
      created.push({ id, cwd: meta?.cwd, preset: meta?.agentPreset, options: agentOptions })
      const agent = makeAgent(id, meta?.cwd, MODE_OF_CWD.get(meta?.cwd) ?? 'ok')
      // 应用 create 传入的 agentOptions(真实 dsh 会把 options 写到 agent.options)
      if (agentOptions) {
        if (agentOptions.provider !== undefined) agent.options.provider = agentOptions.provider
        if (agentOptions.model !== undefined) agent.options.model = agentOptions.model
      }
      return { agent, dispose: async () => { disposed.push(id); agentsById.delete(id) } }
    },
    resume: async ({ resumeSessionId, agentOptions }) => {
      const id = String(resumeSessionId)
      if (id !== 'sess-persisted') throw new Error(`no persisted session "${id}"`)
      resumed.push(id)
      const agent = makeAgent(id, FAKE_CWD, 'ok', 120)
      if (agentOptions?.model !== undefined) agent.options.model = agentOptions.model
      return { agent, dispose: async () => { disposed.push(id); agentsById.delete(id) } }
    },
  },
  agentPresets: fakeAgentPresets,
  sessions: fakeSessions,
  sessionPersistence: fakePersistence,
  workspaceRegistry: wsRegistry,
  effect: (fn) => { disposer = fn(); return disposer },
  // 事件注册(prepend 支持)—— 审批应答者/提问 waterfall/notice 落点/session 事件广播共用
  on: (name, handler, options) => {
    const list = eventHandlers.get(name) ?? []
    if (options?.prepend) list.unshift(handler); else list.push(handler)
    eventHandlers.set(name, list)
    return () => { const i = list.indexOf(handler); if (i >= 0) list.splice(i, 1) }
  },
  get: (name) => (name === 'workspaceRegistry' ? wsRegistry
    : name === 'sessions' ? fakeSessions
    : name === 'sessionPersistence' ? fakePersistence
    : name === 'tokenMeter' ? fakeMeter
    : name === 'llm' ? fakeLlm
    : name === 'agentDefaultModel' ? fakeDefaultModel
    : name === 'userQuestions' ? fakeUserQuestions
    : name === 'compaction' ? fakeCompaction
    : name === 'sessionTitle' && titleServiceActive ? fakeSessionTitle
    : name === 'agentPresets' ? fakeAgentPresets
    : name === 'sandboxPolicy' ? fakeSandboxPolicy
    : name === 'approval' ? fakeApprovalService
    : name === 'permissionPresets' ? fakePermissionPresets
    : undefined),
}
let disposer = () => {}

const PORT = 8099
const BASE = `http://127.0.0.1:${PORT}/mcp`

async function rpc(sessionId, body) {
  const res = await fetch(BASE, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      ...(sessionId ? { 'Mcp-Session-Id': sessionId } : {}),
    },
    body: JSON.stringify(body),
  })
  const sid = res.headers.get('mcp-session-id') ?? sessionId
  const text = await res.text()
  return { sid, status: res.status, text }
}

function parsePayload(text) {
  for (const line of text.split('\n')) {
    const t = line.trim()
    if (t.startsWith('data: ')) return JSON.parse(t.slice(6))
  }
  return JSON.parse(text)
}

// 解出 MCP envelope 里的内层 JSON(text content 是 out() 字符串); isError 结果取错误文本
function innerOf(resp) {
  const payload = parsePayload(resp.text)
  if (payload.error) return { error: payload.error.message }
  const r = payload.result
  if (r.isError) return { error: r.content?.[0]?.text ?? 'isError' }
  return JSON.parse(r.content[0].text)
}

// ── notice 安全落点用例的假日志工具 ──
// appendStep 按 harness 事件格式 push(seq 连续), 模拟真实 Session.append 的形状
function appendStep(log, ev) {
  log.push({ seq: log.length, ...ev })
}
// assistant 消息, 携带一条 tool-call 块(模型请求里正是这些 id 要求后续 tool/result 匹配)
function makeAssistantWithToolCall(callId, name) {
  return {
    id: `asst-${callId}`,
    role: 'assistant',
    content: [{ type: 'tool-call', id: callId, name, arguments: '{}' }],
    source: { kind: 'model', provider: 'deepseek-official', model: 'deepseek-v4-flash' },
  }
}
// tool/result 消息: content[0] 为 tool-result 块, 带 toolCallId 回指 assistant 的 tool-call
function makeToolResult(callId, text) {
  return {
    id: `tool-${callId}`,
    role: 'user',
    content: [{ type: 'tool-result', toolCallId: callId, content: [{ type: 'text', text }] }],
    source: { kind: 'tool', name: 'bash', callId },
  }
}
// 复现 OpenAI 兼容约束: assistant 带 tool-call 后, 后续模型消息必须依次紧跟对应 tool/result
// (toolCallId 按序匹配), 期间插入任何 user/assistant 消息都判为 INVALID_REQUEST。
function modelSequenceError(log) {
  const pending = []
  for (const e of log) {
    if (e.type !== 'user/message' && e.type !== 'assistant/message' && e.type !== 'tool/result') continue
    const msg = e.type === 'user/message' ? e.data : e.data?.message
    if (!msg) continue
    const calls = (msg.content ?? []).filter((b) => b?.type === 'tool-call' && b?.id !== undefined)
    if (calls.length > 0) {
      for (const c of calls) pending.push(c.id)
      continue
    }
    if (pending.length > 0) {
      if (e.type !== 'tool/result' || msg.content?.[0]?.type !== 'tool-result' || msg.content[0].toolCallId !== pending[0]) {
        return `assistant(tool_calls) 后插入了 ${e.type}, 待响应 tool_call_id: ${pending.join(',')}`
      }
      pending.shift()
    }
  }
  return pending.length > 0 ? `未闭合的 tool_call_id: ${pending.join(',')}` : null
}

const checks = {}
function check(name, ok, detail) {
  checks[name] = ok === true
  if (ok !== true && detail !== undefined) console.log(`      ↳ ${name}: ${JSON.stringify(detail)}`)
}
let rid = 0
let mcpSid
const call = (name, args) => rpc(mcpSid, {
  jsonrpc: '2.0', id: ++rid, method: 'tools/call',
  params: { name, arguments: args ?? {} },
})
const callIn = async (name, args) => innerOf(await call(name, args))
// session_status 轮询 —— 派活后主动去查, 取代旧的 task_result 轮询
async function statusOf(sessionId) { return callIn('session_status', { sessionId }) }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
// 等到 phase 变为目标值(followup 只是唤醒驱动, turn 稍后才开, 所以不能假设 send 返回即 running)
async function waitPhase(sessionId, phase, tries = 200) {
  for (let i = 0; i < tries; i++) {
    const st = await statusOf(sessionId)
    if (st.error !== undefined || st.phase === phase) return st
    await sleep(20)
  }
  return statusOf(sessionId)
}
// 等到 lastTurn.turn >= target 且 phase 离开 running(会话已不存在时原样返回错误响应)
async function waitTurnSettled(sessionId, target, tries = 250) {
  let st = await statusOf(sessionId)
  for (let i = 0; i < tries; i++) {
    st = await statusOf(sessionId)
    if (st.error !== undefined) return st
    if (st.phase !== 'running' && st.lastTurn != null && st.lastTurn.turn >= target) return st
    await sleep(20)
  }
  return st
}
// 等到「本轮投喂之后」的 turn 落定(以调用时的 lastTurn 为基线 +1)
async function waitSettled(sessionId, tries = 250) {
  const base = (await statusOf(sessionId)).lastTurn?.turn ?? 0
  return waitTurnSettled(sessionId, base + 1, tries)
}

try {
  // 模拟 web GUI 的审批应答者: 先注册(在我们的应答者之前), 验证 prepend 抢序 + 非 MCP 会话放行
  const guiHandler = (req, next) => Promise.resolve('gui-claimed')
  ctx.on('approval/request', guiHandler, { prepend: false })
  // 模拟 web GUI 的提问应答者(≥0.1.5: dsh-client-ui-user-questions 用 ctx.remote.$on 挂同一 waterfall):
  // 同样先注册, 验证 prepend 抢序 + 非 MCP 会话的提问透传给 GUI
  const guiQuestionHandler = (request) => Promise.resolve({
    answers: request.questions.map((q) => ({ id: q.id, selected: [], custom: 'gui-claimed' })),
  })
  ctx.on('user-questions/request', guiQuestionHandler, { prepend: false })

  await apply(ctx, { port: PORT, host: '127.0.0.1', taskTimeoutMs: 600, workspaceRoots: [FAKE_CWD] })
  await new Promise((r) => setTimeout(r, 400))

  const init = await rpc(undefined, {
    jsonrpc: '2.0', id: 1, method: 'initialize',
    params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'smoke', version: '1.0' } },
  })
  check('initialize 拿到 sessionId', Boolean(init.sid))
  mcpSid = init.sid
  await rpc(mcpSid, { jsonrpc: '2.0', method: 'notifications/initialized' })

  const echo = await call('echo', { text: 'ping-8099' })
  check('echo 通', echo.status === 200 && echo.text.includes('ping-8099'))

  // ── 工具面: 20 个(v0.12.0), 旧名零残留 ──
  const EXPECTED = ['attach_session', 'echo', 'harness_list_tools', 'harness_status', 'model_list',
    'pending_prompts', 'preset_list', 'prompt_respond', 'rename_session', 'session_cancel', 'session_compact',
    'session_inject', 'session_list', 'session_read', 'session_send', 'session_set_model', 'session_status',
    'session_tail', 'session_wait', 'workspace_list']
  const toolNames = parsePayload((await rpc(mcpSid, { jsonrpc: '2.0', id: ++rid, method: 'tools/list', params: {} })).text)
    .result?.tools?.map((t) => t.name) ?? []
  check('工具清单 = 20 个 v0.12.0 工具', toolNames.length === EXPECTED.length
    && EXPECTED.every((t) => toolNames.includes(t)), { got: toolNames })
  check('旧工具名零残留(agent_run/task_*/session_close/mode_list)',
    !toolNames.some((t) => ['agent_run', 'task_inbox', 'task_result', 'task_list', 'task_wait', 'task_cancel', 'session_close', 'mode_list'].includes(t)))

  // ── 4. 派活契约: session_send 立即返回 + 缺参报错 ──
  const noTarget = await call('session_send', { message: 'no target' })
  check('session_send 缺 sessionId/cwd 明确报错', String((await innerOf(noTarget)).error ?? '').includes('sessionId or cwd is required'))

  const t0 = Date.now()
  const send1 = await callIn('session_send', { cwd: FAKE_CWD, message: 'say ok', context: 'memory' })
  const sendMs = Date.now() - t0
  const sidPoolA = String(send1.sessionId)
  check('session_send 立即返回(不等 turn 落定)', sendMs < 500 && send1.status === 'accepted', { sendMs, send1 })
  check('session_send 返回 sessionId/cwd/inboxDepth/openTurn/hint(形状契约)', Boolean(sidPoolA)
    && send1.cwd === FAKE_CWD && typeof send1.inboxDepth === 'number'
    && 'openTurn' in send1 && typeof send1.hint === 'string', send1)
  check('session_send 投喂到池新建会话(meta.cwd 透传)', created.some((c) => c.id === sidPoolA && c.cwd === FAKE_CWD))

  // ── 5. 主动查询: session_status ──
  // followup 只是唤醒驱动, turn 稍后才开 → 这里以轮询为准(这就是新模型: 派活立即回, 状态主动查)
  const running = await waitPhase(sidPoolA, 'running')
  check('session_status: 轮询到在飞 turn → phase=running + openTurn', running.phase === 'running'
    && running.openTurn?.turn === 1 && running.live === true && running.source === 'live', running)
  // 运行中再投喂: 只入队, 不新开 turn(openTurn 不前进)
  const send2 = await callIn('session_send', { sessionId: sidPoolA, message: 'queued behind' })
  check('session_send 运行中再投喂: 只入队(inboxDepth>=1, 不新开 turn)', send2.inboxDepth >= 1
    && send2.openTurn?.turn === 1, send2)
  check('session_status: context(仅 live 可测) 非空且含 events/tokens/pressure/window/ratio', running.context !== null
    && typeof running.context?.events === 'number' && typeof running.context?.pressure === 'number'
    && running.context?.window === 200 && typeof running.context?.ratio === 'number', running.context)
  const settled = await waitTurnSettled(sidPoolA, 2) // 本轮 + 排队那一轮都落定
  check('session_status: 轮询到落定 → phase=idle + lastTurn.completed', settled.phase === 'idle'
    && settled.lastTurn?.reason?.kind === 'completed', settled.lastTurn)
  check('排队输入在下一个 turn 边界被 claim 并落定(turn=2)', settled.lastTurn?.turn === 2, settled.lastTurn)
  check('session_status: changes/verification/leftovers 由 parseSummary 提取', settled.changes === SUMMARY.changes
    && settled.verification === SUMMARY.verification && settled.leftovers === SUMMARY.leftovers, settled)
  check('session_status: lastText 含 assistant 文本 + logEvents 计数', String(settled.lastText).includes('done:')
    && settled.logEvents === agentsById.get(sidPoolA).session.log.length, { lastText: settled.lastText, logEvents: settled.logEvents })
  const stUnknown = await call('session_status', { sessionId: 'sess-nope-404' })
  check('session_status: 未知会话报结构化错误', parsePayload(stUnknown.text).result?.isError === true
    && String((await innerOf(stUnknown)).error ?? '').includes('session not found'))

  // ── 6. session_tail 契约 ──
  const tail = await callIn('session_tail', { sessionId: sidPoolA, n: 10 })
  const tailTypes = tail.events.map((e) => e.type)
  check('session_tail: 返回表面事件且每条 {seq,type,text,turn}', tail.source === 'live'
    && tail.events.every((e) => typeof e.seq === 'number' && typeof e.type === 'string' && typeof e.text === 'string' && [1, 2].includes(e.turn))
    && tailTypes.includes('turn/start') && tailTypes.includes('turn/end') && tailTypes.includes('assistant/message'), tail.events)
  check('session_tail: 内部事件被滤掉(只留表面类型)', tailTypes.every((t) => ['user/message', 'assistant/message', 'tool/call', 'tool/result', 'turn/start', 'turn/end'].includes(t)), tailTypes)
  check('session_tail: 无 "(no text blocks)" 噪声', !tail.events.some((e) => e.text === '(no text blocks)'))
  const tailSurface = await callIn('session_tail', { sessionId: sidPoolA, n: 10, filter: 'surface' })
  check('session_tail filter=surface: 不含工具事件', tailSurface.events.every((e) => !e.type.startsWith('tool/')), tailSurface.events.map((e) => e.type))
  const tailTools = await callIn('session_tail', { sessionId: sidPoolA, n: 10, filter: 'tools' })
  check('session_tail filter=tools: 只含工具事件', tailTools.events.every((e) => e.type.startsWith('tool/')), tailTools.events)
  const tailSince = await callIn('session_tail', { sessionId: sidPoolA, n: 50, sinceSeq: 1 })
  check('session_tail sinceSeq: 只回 seq>sinceSeq 的事件', tailSince.events.every((e) => e.seq > 1), tailSince.events.map((e) => e.seq))
  check('session_tail: n 限制返回条数', (await callIn('session_tail', { sessionId: sidPoolA, n: 2 })).events.length === 2)

  // ── 7/8. session_wait + session_cancel(卡死 turn) ──
  const hangSend = await callIn('session_send', { cwd: HANG_CWD, message: 'hang forever' })
  const sidHang = String(hangSend.sessionId)
  check('session_send(卡死 agent): 立即返回', hangSend.status === 'accepted' && Boolean(sidHang))
  const hangRunning = await waitPhase(sidHang, 'running')
  check('卡死 turn: phase=running + openTurn 1', hangRunning.phase === 'running' && hangRunning.openTurn?.turn === 1, hangRunning)
  const idleWait = await callIn('session_wait', { sessionId: sidPoolA, until: 'idle', timeoutMs: 2000 })
  check('session_wait until=idle: 已空闲立即落定 {timeout:false,status}', idleWait.timeout === false
    && idleWait.status?.phase === 'idle', idleWait)
  const turnWait = await callIn('session_wait', { sessionId: sidPoolA, until: 'turn-end', sinceTurn: 1, timeoutMs: 2000 })
  check('session_wait until=turn-end(sinceTurn): 已闭合 turn 立即落定', turnWait.timeout === false
    && turnWait.status?.phase === 'idle' && (turnWait.status?.lastTurn?.turn ?? 0) >= 1, turnWait)
  const hangWait = await callIn('session_wait', { sessionId: sidHang, until: 'turn-end', timeoutMs: 300 })
  check('session_wait 卡死 turn: 超时返回 {timeout:true,status}', hangWait.timeout === true
    && hangWait.status?.phase === 'running' && hangWait.status?.openTurn?.turn === 1, hangWait)
  const inputWait = await callIn('session_wait', { sessionId: sidHang, until: 'input', timeoutMs: 200 })
  check('session_wait until=input 无弹窗: 超时且带 status', inputWait.timeout === true && inputWait.status !== undefined)
  // 运行中投喂 → 排队; keepInbox=false 取消会清掉排队输入, keepInbox=true 保留
  await callIn('session_send', { sessionId: sidHang, message: 'must stay queued' })
  check('MCP 在飞 turn 计数: 卡死 turn 期间 harness_status 可见', (await callIn('harness_status', {})).mcpInFlightTurns?.total >= 1)
  const keepCancel = await callIn('session_cancel', { sessionId: sidHang, keepInbox: true })
  check('session_cancel keepInbox=true: 保留排队输入', keepCancel.cancelled === true
    && agentsById.get(sidHang).inbox.nextTurn.length === 1, { keepCancel, queued: agentsById.get(sidHang).inbox.nextTurn.length })
  check('session_cancel: turn 以 reason.kind=aborted 落定', (await statusOf(sidHang)).lastTurn?.reason?.kind === 'aborted')
  // keepInbox=true 时队列里仍有一个待 claim 的 MCP turn → 该会话计数应为 1(语义正确: 还有 MCP 工作在等)
  const afterKeep = await callIn('harness_status', {})
  check('MCP 在飞 turn 计数: keepInbox=true 保留待落定 turn(逐会话可见)',
    afterKeep.mcpInFlightTurns?.detail?.some((d) => d.sessionId === sidHang && d.turns === 1) === true, afterKeep.mcpInFlightTurns)
  check('session_cancel: 记录 cancel 原因(harness-mcp-cancel)', agentsById.get(sidHang)._cancelCalls.some((c) => c.cause?.reason === 'harness-mcp-cancel'))
  const dropCancel = await callIn('session_cancel', { sessionId: sidHang })
  check('session_cancel 缺省: 清空排队输入', agentsById.get(sidHang).inbox.nextTurn.length === 0 && dropCancel.keepInbox === false)
  check('MCP 在飞 turn 计数: 清空队列后归零', (await callIn('harness_status', {})).mcpInFlightTurns?.total === 0,
    (await callIn('harness_status', {})).mcpInFlightTurns)
  const cancelCold = await callIn('session_cancel', { sessionId: 'sess-nope-404' })
  check('session_cancel 非 live 会话: noop', cancelCold.live === false && cancelCold.cancelled === false)

  // ── 1. 任意会话续接三级 ──
  const runLive = await callIn('session_send', { message: 'say ok', sessionId: 'sess-live' })
  check('session_send 接管 live 会话(不 resume 不 dispose)', runLive.sessionId === 'sess-live'
    && resumed.length === 0 && disposed.length === 0, { runLive, resumed, disposed })
  const runUnknown = await call('session_send', { message: 'say ok', sessionId: 'sess-unknown' })
  check('session_send 未知会话明确报错(session not found for resume)',
    String((await innerOf(runUnknown)).error ?? '').includes('session not found for resume'))

  // 持久化 resume: 在飞期在 session_list 里以 live 行可见(resume 挂载的 preset 生效), 落定后 flush + dispose
  const presetResume = await callIn('session_send', { message: 'preset resume', sessionId: 'sess-persisted', preset: 'code' })
  check('session_send 持久化会话 resume(preset 单独允许)', presetResume.sessionId === 'sess-persisted'
    && resumed.includes('sess-persisted'), presetResume)
  const resumedRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === 'sess-persisted')
  check('resume 的 preset 在会话快照里生效(mode.preset=code)', resumedRow?.mode?.preset === 'code', resumedRow)
  // 句柄释放后就查不到这个会话了, 因此直接等 release 落定(flush + dispose 才是要验的契约)
  for (let i = 0; i < 250 && !disposed.includes('sess-persisted'); i++) await sleep(20)
  check('resume 句柄在 turn 落定后 flush + dispose(不驻池)', flushed.includes('sess-persisted')
    && disposed.includes('sess-persisted'), { flushed, disposed })

  // ── 2. realpath 规范化 + 白名单 ──
  check('池新建: meta.cwd 为 realpath 值', created.some((c) => c.id === sidPoolA && c.cwd === FAKE_CWD))
  const missingDir = resolve(FAKE_CWD, 'nonexistent-xyz')
  const missSend = await callIn('session_send', { cwd: missingDir, message: 'say ok' })
  check('目录不存在: realpath 回退 resolve 且不阻断', Boolean(missSend.sessionId)
    && created.some((c) => c.id === String(missSend.sessionId) && c.cwd === missingDir))
  const outside = await call('session_send', { cwd: '/tmp', message: 'outside' })
  check('cwd 白名单: 越界路径拒绝', String((await innerOf(outside)).error ?? '').includes('not allowed'))

  // ── 9. 外部显式控制会话复用(newSession / session_list) ──
  const freshSend = await callIn('session_send', { cwd: FAKE_CWD, message: 'fresh session', newSession: true })
  const sidFresh = String(freshSend.sessionId)
  check('newSession: 强制全新会话(不池命中旧会话)', sidFresh !== sidPoolA
    && created.some((c) => c.id === sidFresh))
  check('newSession: 旧池会话被退役 dispose', disposed.includes(sidPoolA))
  const reuseSend = await callIn('session_send', { cwd: FAKE_CWD, message: 'reuse again' })
  check('缺省: 复用池里的新会话', String(reuseSend.sessionId) === sidFresh, reuseSend)
  await waitSettled(sidFresh)
  const sessions = await callIn('session_list', {})
  check('session_list: 池会话可见', sessions.sessions.some((s) => s.sessionId === sidFresh && s.source === 'pool'))
  check('session_list: live 会话可见', sessions.sessions.some((s) => s.sessionId === 'sess-live' && s.source === 'live'))
  check('session_list: 持久化会话可见', sessions.sessions.some((s) => s.sessionId === 'sess-persisted' && s.source === 'persisted'))
  const poolRow = sessions.sessions.find((s) => s.sessionId === sidFresh)
  const liveRow = sessions.sessions.find((s) => s.sessionId === 'sess-live')
  const persistedRow = sessions.sessions.find((s) => s.sessionId === 'sess-persisted')
  check('session_list: 池/live 行带上下文占用', typeof poolRow?.context?.tokens === 'number'
    && typeof liveRow?.context?.tokens === 'number', { pool: poolRow?.context, live: liveRow?.context })
  check('session_list: 上下文带窗口与占用比(ratio=tokens/window)', poolRow?.context?.window === 200
    && poolRow?.context?.ratio === Math.round((poolRow.context.tokens / 200) * 1000) / 10
    && liveRow?.context?.tokens === 30 && liveRow?.context?.ratio === 15, { pool: poolRow?.context, live: liveRow?.context })
  check('session_list: 持久化行上下文为 null', persistedRow?.context === null)

  // ── 10. 上下文压缩 ──
  const compact = await callIn('session_compact', { sessionId: sidFresh })
  check('session_compact: 压缩成功(ok+compactionId+shadowedNodes)', compact.ok === true
    && compact.compactionId === 'cmp-1' && compact.shadowedNodes === 3)
  check('session_compact: 调用了 compaction 服务且带 before/after', compactCalls.some((c) => c.sessionId === sidFresh)
    && 'before' in compact && 'after' in compact)
  const compactUnknown = await call('session_compact', { sessionId: 'sess-unknown' })
  check('session_compact: 未知会话明确报错', String((await innerOf(compactUnknown)).error ?? '').includes('session not found'))

  // ── 11. 运维/审计/选型 ──
  const status = await callIn('harness_status', {})
  check('harness_status: agent 池 + MCP 在飞 turn + config', status.agentPool?.size >= 1
    && typeof status.mcpInFlightTurns?.total === 'number' && status.config?.taskTimeoutMs === 600
    && typeof status.uptimeSec === 'number', status)
  check('harness_status: 不再有任务队列字段', status.queue === undefined)

  const read = await callIn('session_read', { sessionId: 'sess-live' })
  check('session_read: 返回事件流', read.sessionId === 'sess-live' && Array.isArray(read.events) && typeof read.total === 'number')
  const readUnknown = await call('session_read', { sessionId: 'sess-unknown' })
  check('session_read: 未知会话报错', String((await innerOf(readUnknown)).error ?? '').includes('session not found'))

  // 回归: 真实 dsh 日志里 chunk/内部事件占绝大多数, limit 必须按「表面事件」计而非原始日志条数
  {
    const fakeLog = []
    for (let i = 0; i < 60; i++) {
      fakeLog.push({ seq: fakeLog.length, type: 'assistant/chunk', data: { chunk: 'x' } })
      fakeLog.push({ seq: fakeLog.length, type: 'step/start' })
      if (i % 10 === 0) fakeLog.push({ seq: fakeLog.length, type: 'user/message', data: { message: { content: [{ type: 'text', text: `msg-${i}` }] } } })
      if (i % 5 === 0) fakeLog.push({ seq: fakeLog.length, type: 'tool/call', data: { name: 'bash', arguments: `echo ${i}` } })
      if (i % 5 === 0) fakeLog.push({ seq: fakeLog.length, type: 'tool/result', data: { content: [{ type: 'text', text: `out-${i}` }] } })
      fakeLog.push({ seq: fakeLog.length, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: `asst-${i}` }] } } })
      fakeLog.push({ seq: fakeLog.length, type: 'step/end' })
    }
    const savedLog = liveAgent.session.log
    liveAgent.session.log = fakeLog
    const read50 = await callIn('session_read', { sessionId: 'sess-live', limit: 50 })
    check('session_read: limit 按表面事件计(chunk 不占额)', read50.returned === 50 && read50.total === 90 && read50.logEvents > read50.total)
    check('session_read: 返回按时间序的最近表面事件', read50.events[49]?.text === 'asst-59')
    check('session_read: 缺省 limit=100 返回全部表面事件', (await callIn('session_read', { sessionId: 'sess-live' })).returned === 90)
    liveAgent.session.log = savedLog
  }

  const ws = await callIn('workspace_list', {})
  check('workspace_list: 列出工作区', Array.isArray(ws.workspaces)
    && ws.workspaces.some((w) => w.id === 'ws-fake' && w.path === FAKE_CWD))

  const modelSend = await callIn('session_send', { cwd: FAKE_CWD, message: 'model test', newSession: true, model: 'custom-model' })
  const sidModel = String(modelSend.sessionId)
  check('session_send model 参数: 透传到 create', created.some((c) => c.id === sidModel && c.options?.model === 'custom-model'))
  check('session_send 缺省 model: 从 agentDefaultModel 兜底填充', created.some((c) => c.id === sidModel
    && c.options?.model === 'custom-model' && c.options?.provider === 'deepseek-official'))
  await waitSettled(sidModel)
  const models = await callIn('model_list', {})
  check('model_list: 列出所有 provider 的模型', Array.isArray(models.providers)
    && models.providers.some((r) => r.provider === 'deepseek-official' && r.active === true
      && r.models.some((m) => m.id === 'deepseek-v4-flash')))
  check('model_list: 含 zai 配置模型', models.providers.some((r) => r.provider === 'zai' && r.models.some((m) => m.id === 'glm-5.3')))
  check('model_list: 目录补全未激活 provider', models.providers.some((r) => r.provider === 'custom-gw' && r.active === false))
  const modelsZai = await callIn('model_list', { provider: 'zai' })
  check('model_list: 指定 provider 只列该 provider', modelsZai.providers.length === 1
    && modelsZai.providers[0].provider === 'zai' && modelsZai.providers[0].models.length === 2)
  const winRow = (await callIn('model_list', { withWindow: true })).providers.find((r) => r.provider === 'deepseek-official')
  check('model_list: withWindow 解析上下文窗口', winRow?.models.every((m) => m.contextWindow === 200))

  // 窗口不可解析(resolveModelInfo 抛错): window/ratio 必须为 null 而非崩溃, tokens 仍可读。
  // 用独立 cwd: newSession:true 会退役同 cwd 的池会话, 不能撞掉后面 session_set_model/session_inject 要用的会话。
  const nopeSend = await callIn('session_send', { cwd: resolve(FAKE_CWD, 'nope-model-zone'), message: 'no window', newSession: true, model: 'nope-model' })
  const nopeSt = await statusOf(String(nopeSend.sessionId))
  check('窗口不可解析: context.window/ratio 为 null(不崩溃)', nopeSt.context?.window === null
    && nopeSt.context?.ratio === null && typeof nopeSt.context?.tokens === 'number', nopeSt.context)

  // session_set_model / session_inject 紧跟在它们使用的池会话之后: 常驻池是 LRU(默认上限 8 个 cwd),
  // 继续往后面开新 cwd 会把该会话淘汰出池 → 之后就只能靠 resume, 而它在 fake 里不是持久化会话。
  // ── session_set_model / session_inject ──
  const setModel = await callIn('session_set_model', { sessionId: sidModel, model: 'glm-5.3' })
  check('session_set_model: 返回新旧模型', setModel.ok === true && setModel.oldModel === 'custom-model' && setModel.newModel === 'glm-5.3', setModel)
  check('session_set_model: agent.options 已切换', agentsById.get(sidModel)?.options?.model === 'glm-5.3')
  const inj = await callIn('session_inject', { sessionId: sidModel, message: '请先用 git status 看改动' })
  check('session_inject: 插入 steering 消息', inj.ok === true
    && inboxAppends.some((x) => String(x.m?.content?.[0]?.text ?? '').includes('git status') && x.t === 'next-turn'), inj)
  const injUnknown = await call('session_inject', { sessionId: 'sess-unknown', message: 'x' })
  check('session_inject: 未知会话报错', String((await innerOf(injUnknown)).error ?? '').includes('session not found'))


  // ── 3. attach_session 三态 + 存量捞回 ──
  const attachLive = await call('attach_session', { sessionId: 'sess-live' })
  check('attach_session live 会话', attachLive.status === 200 && (await innerOf(attachLive)).attached === true)
  const attachMissing = await call('attach_session', { sessionId: 'sess-nope' })
  check('attach_session 未知会话报错', attachMissing.status === 200 && typeof (await innerOf(attachMissing)).error === 'string')
  check('错误响应带 isError 标记', parsePayload(attachMissing.text).result?.isError === true)
  const attachPersisted = await call('attach_session', { sessionId: 'sess-persisted' })
  check('attach_session 持久化会话(经 sessionPersistence)', attachPersisted.status === 200 && (await innerOf(attachPersisted)).attached === true)
  const attachOutside = await call('attach_session', { sessionId: 'sess-persisted', path: '/tmp' })
  check('attach_session 越界路径被白名单拒绝', String((await innerOf(attachOutside)).error ?? '').includes('not allowed'))

  // ── 12. preset_list 目录(字段名与 mode_list 时期完全一致) ──
  const presets = await callIn('preset_list', {})
  check('preset_list: 列出 agent presets(standard 默认)', Array.isArray(presets.presets)
    && presets.presets.some((p) => p.id === 'standard' && p.default === true)
    && presets.presets.some((p) => p.id === 'code' && p.default === false))
  check('preset_list: 列出沙箱模式(3 个, read-only 默认)', Array.isArray(presets.sandboxModes)
    && presets.sandboxModes.length === 3 && presets.sandboxModes.find((s) => s.id === 'read-only')?.default === true
    && presets.sandboxModes.some((s) => s.id === 'workspace-write'))
  check('preset_list: 列出审批策略(ask 默认)', Array.isArray(presets.approvalPolicies)
    && presets.approvalPolicies.length === 2 && presets.approvalPolicies.find((a) => a.id === 'ask')?.default === true)
  check('preset_list: 权限预设(workspace-write = 沙箱 workspace-write + 审批 ask)', Array.isArray(presets.permissionPresets)
    && presets.permissionPresets.some((pp) => pp.id === 'workspace-write' && pp.sandbox === 'workspace-write'
      && pp.approval === 'ask' && pp.default === true))
  check('preset_list: modes 汇总含可传 mode= 的规范 id', Array.isArray(presets.modes)
    && presets.modes.some((m) => m.id === 'workspace-write' && m.kind === 'permission')
    && presets.modes.some((m) => m.id === 'standard' && m.kind === 'preset')
    && presets.modes.some((m) => m.id === 'ask' && m.kind === 'approval')
    && presets.modes.some((m) => m.id === 'read-only' && m.kind === 'sandbox'))
  check('preset_list: 部署默认(deployment)', presets.deployment?.defaultPreset === 'standard'
    && presets.deployment?.defaultSandboxMode === 'read-only' && presets.deployment?.defaultApprovalPolicy === 'ask')
  const presetsOnly = await callIn('preset_list', { only: 'preset' })
  check('preset_list: only=preset 只列 preset', Array.isArray(presetsOnly.presets)
    && presetsOnly.presets.length >= 1 && presetsOnly.sandboxModes === undefined && presetsOnly.modes === undefined)
  const presetsDetail = await callIn('preset_list', { withDetail: true })
  check('preset_list: withDetail 附带路径等细节', typeof presetsDetail.presets?.[0]?.path === 'string'
    && typeof presetsDetail.sandboxModes?.[0]?.kind === 'string')

  // ── 13. 按 preset/mode 建会话 ──
  const modeSend = await callIn('session_send', { cwd: MODE_CWD, message: 'mode test', preset: 'code', mode: 'workspace-write' })
  const sidMode = String(modeSend.sessionId)
  const modeCreated = created.find((c) => c.id === sidMode)
  check('session_send mode: 强制全新会话(不池复用)', Boolean(sidMode) && modeCreated !== undefined)
  check('session_send preset: meta.agentPreset 记录到创建事实', modeCreated?.preset === 'code')
  check('session_send mode: 会话日志落 sandbox/mode + approval/policy 持久事件',
    (agentsById.get(sidMode)?.session.log ?? []).some((e) => e.type === 'sandbox/mode' && e.data?.mode === 'workspace-write')
    && (agentsById.get(sidMode)?.session.log ?? []).some((e) => e.type === 'approval/policy' && e.data?.policy === 'ask'))
  const modeRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === sidMode)
  check('session_list mode 快照: 权限预设捆绑生效(workspace-write = 沙箱+ask)', modeRow?.mode?.preset === 'code'
    && modeRow?.mode?.sandbox === 'workspace-write' && modeRow?.mode?.approval === 'ask'
    && modeRow?.mode?.permissionPreset === 'workspace-write', modeRow?.mode)
  await waitSettled(sidMode)

  const fullSend = await callIn('session_send', { cwd: MODE_CWD, message: 'full access', mode: 'danger-full-access' })
  const sidFull = String(fullSend.sessionId)
  const fullRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === sidFull)
  check('session_send mode=danger-full-access: 捆绑沙箱+审批生效', fullRow?.mode?.sandbox === 'danger-full-access'
    && fullRow?.mode?.approval === 'never' && fullRow?.mode?.permissionPreset === 'danger-full-access', fullRow?.mode)
  check('session_send mode: 再次指定仍强制全新会话', sidFull !== sidMode)
  await waitSettled(sidFull)

  const explicitSend = await callIn('session_send', { cwd: MODE_CWD, message: 'explicit', sandbox: 'read-only', approval: 'never' })
  const explicitRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === String(explicitSend.sessionId))
  check('session_send sandbox+approval 显式: 生效', explicitRow?.mode?.sandbox === 'read-only'
    && explicitRow?.mode?.approval === 'never' && explicitRow?.mode?.permissionPreset === undefined, explicitRow?.mode)
  await waitSettled(String(explicitSend.sessionId))

  const modeBad = await call('session_send', { cwd: MODE_CWD, message: 'x', mode: 'nope-mode' })
  check('session_send mode 非法值报错', String((await innerOf(modeBad)).error ?? '').includes('unknown mode'))
  const presetBad = await call('session_send', { cwd: MODE_CWD, message: 'x', preset: 'nope-preset' })
  check('session_send preset 非法值报错', String((await innerOf(presetBad)).error ?? '').includes('unknown preset'))
  const modeResume = await call('session_send', { message: 'x', sessionId: 'sess-live', mode: 'workspace-write' })
  check('mode+sandbox/approval 不可续接存量会话(需新建)',
    String((await innerOf(modeResume)).error ?? '').includes('only apply when creating a new session'))

  // ── 15. 会话自动命名 ──
  const titleSend = await callIn('session_send', { cwd: TITLE_CWD, message: '修复登录页 token 过期后的自动刷新逻辑并补单测' })
  const sidTitle = String(titleSend.sessionId)
  const titleRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === sidTitle)
  check('自动命名: 新会话在 session_list 里带派生 title', typeof titleRow?.title === 'string'
    && titleRow.title.length > 0 && titleRow.title.length <= 60 && titleRow.title.includes('修复登录页'), titleRow?.title)
  check('自动命名: 会话日志落 session/title 事件(rename 生效)', (agentsById.get(sidTitle)?.session.log ?? []).some(
    (e) => e.type === 'session/title' && e.data?.title === titleRow?.title && e.data?.source?.kind === 'user'))
  await waitSettled(sidTitle)
  const explicitTitle = await callIn('session_send', { cwd: resolve(FAKE_CWD, 'title-zone-2'), message: '随便改点什么', title: '我的显式标题' })
  const explicitTitleRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === String(explicitTitle.sessionId))
  check('自动命名: 显式 title 优先(不派生覆盖)', explicitTitleRow?.title === '我的显式标题', explicitTitleRow?.title)
  await waitSettled(String(explicitTitle.sessionId))
  await callIn('session_send', { sessionId: sidTitle, message: '后续任务不改名' })
  await waitSettled(sidTitle)
  const afterReuse = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === sidTitle)
  check('自动命名: 复用会话不改名(标题保持)', afterReuse?.title === titleRow?.title, afterReuse?.title)
  titleServiceActive = false
  const noSvc = await callIn('session_send', { cwd: resolve(FAKE_CWD, 'title-zone-3'), message: 'no title svc' })
  const noSvcRow = (await callIn('session_list', {})).sessions.find((s) => s.sessionId === String(noSvc.sessionId))
  check('自动命名: sessionTitle 服务缺失时静默降级', Boolean(noSvc.sessionId) && noSvcRow?.title === undefined, noSvcRow?.title)
  titleServiceActive = true
  await waitSettled(String(noSvc.sessionId))

  // ── 模型/执行失败 + 切模型 + 注入 ──
  const errSend = await callIn('session_send', { cwd: ERROR_CWD, message: 'boom' })
  const errSt = await waitSettled(String(errSend.sessionId))
  check('模型错误: session_status 反映 reason.kind=error + error.code', errSt.lastTurn?.reason?.kind === 'error'
    && errSt.lastTurn?.reason?.error?.code === 'QUOTA' && errSt.phase === 'idle', errSt.lastTurn)
  const throwSend = await call('session_send', { cwd: THROW_CWD, message: 'crash' })
  check('执行异常: followup 抛错 → session_send isError(send failed)', parsePayload(throwSend.text).result?.isError === true
    && String((await innerOf(throwSend)).error ?? '').includes('send failed'))
  const throwSid = String(created.find((c) => c.cwd === THROW_CWD)?.id ?? '')
  const afterThrow = await callIn('harness_status', {})
  check('执行异常: 不残留 MCP 在飞 turn 计数', afterThrow.mcpInFlightTurns?.total === 0
    && !afterThrow.mcpInFlightTurns?.detail?.some((d) => d.sessionId === throwSid), afterThrow.mcpInFlightTurns)

  // ── 12. 弹窗感知与响应(审批绝不自动放行; 提问可程序化回答) ──
  check('审批应答者 prepend 优先于 GUI', eventHandlers.get('approval/request')?.[0] !== guiHandler)
  {
    const bareAgent = makeAgent('bare-sess', FAKE_CWD)
    bareAgent.session.events.push({ type: 'approval/asked', data: { id: 'appr-bare', toolName: 'bash', callId: 'c-9' } })
    const bareReq = { agent: bareAgent, toolName: 'bash', callId: 'c-9', reason: 'x', signal: new AbortController().signal }
    check('非 MCP 会话审批放行给 GUI 应答链',
      (await runWaterfall('approval/request', bareReq, () => 'unavailable')) === 'gui-claimed')
  }
  // 审批流程: session_send → session_status 感知 waiting_input → prompt_respond approve → turn 继续并完成
  const apprSend = await callIn('session_send', { cwd: APPROVAL_CWD, message: 'write outside' })
  const apprSid = String(apprSend.sessionId)
  let apprSt
  for (let i = 0; i < 100; i++) {
    apprSt = await statusOf(apprSid)
    if (apprSt.phase === 'waiting_input') break
    await new Promise((r) => setTimeout(r, 20))
  }
  const apprPrompt = apprSt?.prompts?.find((p) => p.type === 'approval')
  check('审批弹窗感知: session_status phase=waiting_input + 原文', apprSt?.phase === 'waiting_input'
    && apprPrompt?.id === 'appr-e2e-1' && String(apprPrompt?.reason ?? '').includes('outside'), apprSt?.prompts)
  const apprList = await callIn('pending_prompts', {})
  check('pending_prompts: 列出审批弹窗', apprList.prompts.some((p) => p.type === 'approval' && p.id === 'appr-e2e-1'))
  const apprResp = await callIn('prompt_respond', { sessionId: apprSid, promptId: 'appr-e2e-1', decision: 'approve' })
  check('prompt_respond: 审批 approve→allowed-once', apprResp.resolved === 'allowed-once')
  const apprDone = await waitSettled(apprSid)
  check('审批后 turn 继续并完成(allowed-once 到达应答链)', apprDone.phase === 'idle'
    && apprDone.lastTurn?.reason?.kind === 'completed'
    && agentsById.get(apprSid)?.approvalOutcome === 'allowed-once', { apprDone, outcome: agentsById.get(apprSid)?.approvalOutcome })

  // 【16】notice 安全落点: ⏳ 接管提示拦截当下即时入 next-step inbox; ✅ 响应提示入队, 工具完成后经
  // tools/post-execute 并入 additionalContexts, 由 agent-loop 在 tool/result 之后追加 —— 全程不打断
  // assistant(tool_calls) 与其 tool/result 的模型消息序列。
  const apprAgent = agentsById.get(apprSid)
  const apprLog = apprAgent?.session.log ?? []
  check('notice: 审批拦截/响应期不写会话日志(即时投递只走 inbox)',
    apprLog.filter((e) => e.type === 'user/message' && e.data?.source?.form === 'notice').length === 0)
  {
    const steer = inboxAppends.filter((x) => x.t === 'next-step' && String(x.m?.content?.[0]?.text ?? '').includes('⏳ 审批已由 MCP 接管'))
    check('notice: ⏳ 接管提示拦截期即时入 inbox(next-step)', steer.length >= 1)
    check('notice: ⏳ 即时提示为 user 形状(web UI steering 投影前提)', steer.every((x) => x.m?.source?.kind === 'user' && x.m?.role === 'user')
      && steer.some((x) => String(x.m?.content?.[0]?.text ?? '').includes('prompt appr-e2e-1')))
  }
  // 模拟 harness 循环提交该 step 的日志形状(assistant 带 tool-call → tool/call → tool/result)
  appendStep(apprLog, { type: 'assistant/message', data: { turn: 1, step: 1, message: makeAssistantWithToolCall('c-1', 'bash') } })
  appendStep(apprLog, { type: 'tool/call', data: { turn: 1, step: 1, callId: 'c-1', name: 'bash', arguments: '{}' } })
  appendStep(apprLog, { type: 'tool/result', data: { turn: 1, step: 1, message: makeToolResult('c-1', 'done') } })
  const apprToolResultIdx = apprLog.length - 1
  let apprDecision = { kind: 'accept' }
  for (const h of eventHandlers.get('tools/post-execute') ?? []) {
    apprDecision = await h({ agent: apprAgent, callId: 'c-1', name: 'bash' }, { isError: false, content: [] }, async () => apprDecision)
  }
  const apprFlushed = (apprDecision.additionalContexts ?? []).filter((m) => m?.source?.form === 'notice')
  for (const msg of apprFlushed) appendStep(apprLog, { type: 'user/message', data: msg })
  const apprNoticeIdx = apprLog.findIndex((e) => e.type === 'user/message' && e.data?.source?.form === 'notice')
  check('notice: ✅ 响应提示在 tool/result 之后安全落点', apprFlushed.length === 1 && apprNoticeIdx > apprToolResultIdx)
  check('notice: 追加后模型消息序列合法(无 INVALID_REQUEST)', modelSequenceError(apprLog) === null, modelSequenceError(apprLog))
  check('notice: 原生 notice 呈现契约(form:notice + summary 非空)', apprFlushed.length === 1
    && apprFlushed.every((m) => m?.source?.form === 'notice' && typeof m.source.summary === 'string' && m.source.summary.length > 0)
    && apprFlushed.some((m) => String(m.content?.[0]?.text ?? '').includes('✅ 审批') && String(m.content?.[0]?.text ?? '').includes('已由 MCP 侧响应')))
  {
    const brokenLog = [
      { seq: 0, type: 'user/message', data: { id: 'u0', role: 'user', content: [{ type: 'text', text: 'task' }], source: { kind: 'human' } } },
      { seq: 1, type: 'assistant/message', data: { turn: 1, step: 1, message: makeAssistantWithToolCall('b-c1', 'bash') } },
      { seq: 2, type: 'user/message', data: { id: 'n0', role: 'user', content: [{ type: 'text', text: '⏳ notice' }], source: { kind: 'plugin', plugin: 'harness-mcp-server', form: 'notice', summary: 'x' } } },
      { seq: 3, type: 'tool/result', data: { turn: 1, step: 1, message: makeToolResult('b-c1', 'ok') } },
    ]
    check('回归可复现: 旧版插入位置被序列校验器判为 INVALID_REQUEST', typeof modelSequenceError(brokenLog) === 'string')
  }

  // ── 17. 提问应答者换代(dsh-user-questions ≥0.1.5) ──
  check('提问探测: 0.1.5 宿主(fake userQuestions 无 registerProvider)下注册 waterfall listener',
    (eventHandlers.get('user-questions/request') ?? []).length >= 1)
  check('提问探测: prepend 抢在 GUI 应答者之前(waterfall outermost-first)',
    eventHandlers.get('user-questions/request')?.[0] !== guiQuestionHandler)
  {
    const guiOnlyAnswer = await askUserQuestions({
      questions: [{ id: 'gq1', question: 'GUI only?' }],
      agent: { id: 'sess-gui-only', session: { id: 'sess-gui-only' } },
      signal: new AbortController().signal,
    })
    check('提问降级: 非 MCP 会话的提问透传给 GUI 应答者', guiOnlyAnswer?.answers?.[0]?.custom === 'gui-claimed')
    check('提问降级: 透传后 MCP 侧无该提问挂起', (await callIn('pending_prompts', { sessionId: 'sess-gui-only' })).total === 0)
  }
  const qSend = await callIn('session_send', { cwd: QUESTION_CWD, message: 'need db choice' })
  const qSid = String(qSend.sessionId)
  let qSt
  for (let i = 0; i < 100; i++) {
    qSt = await statusOf(qSid)
    if (qSt.phase === 'waiting_input') break
    await new Promise((r) => setTimeout(r, 20))
  }
  const qPrompt = qSt?.prompts?.find((p) => p.type === 'question')
  check('提问感知(MCP 在飞 turn): session_status phase=waiting_input + 原文', qPrompt !== undefined
    && qPrompt.questions?.[0]?.question === 'Which DB?', qSt?.prompts)
  const qListed = (await callIn('pending_prompts', {})).prompts.find((p) => p.type === 'question' && p.id === qPrompt?.id)
  check('pending_prompts: 列出提问弹窗(含原文, 无「未接管」note)', qListed !== undefined
    && qListed.questions?.[0]?.question === 'Which DB?' && qListed.note === undefined)
  const respQ = await callIn('prompt_respond', { sessionId: qSid, promptId: String(qPrompt?.id), answer: 'pg' })
  check('prompt_respond: 提问自由文本回答', respQ.ok === true)
  const qAnswer = agentsById.get(qSid)?.questionAnswer
  check('提问应答链收到回答(0.1.5 answer 形态: answers[{id,selected,custom}])',
    qAnswer?.answers?.[0]?.id === 'q1' && qAnswer?.answers?.[0]?.custom === 'pg' && Array.isArray(qAnswer?.answers?.[0]?.selected))
  const qDone = await waitSettled(qSid)
  check('提问回答后 turn 继续并完成', qDone.phase === 'idle' && qDone.lastTurn?.reason?.kind === 'completed', qDone.lastTurn)
  const qAgent = agentsById.get(qSid)
  const qLog = qAgent?.session.log ?? []
  check('提问 notice: 拦截期不写日志', qLog.filter((e) => e.type === 'user/message' && e.data?.source?.form === 'notice').length === 0)
  {
    const qSteer = inboxAppends.filter((x) => x.t === 'next-step' && String(x.m?.content?.[0]?.text ?? '').includes('⏳ 提问已由 MCP 接管'))
    check('提问 notice: ⏳ 接管提示拦截期即时入 inbox(next-step, user 形状)', qSteer.length >= 1
      && qSteer.every((x) => x.m?.source?.kind === 'user' && x.m?.role === 'user'))
  }
  appendStep(qLog, { type: 'assistant/message', data: { turn: 1, step: 1, message: makeAssistantWithToolCall('q-c1', 'ask_user_question') } })
  appendStep(qLog, { type: 'tool/call', data: { turn: 1, step: 1, callId: 'q-c1', name: 'ask_user_question', arguments: '{}' } })
  appendStep(qLog, { type: 'tool/result', data: { turn: 1, step: 1, message: makeToolResult('q-c1', 'answered') } })
  const qToolResultIdx = qLog.length - 1
  let qDecision = { kind: 'accept' }
  for (const h of eventHandlers.get('tools/post-execute') ?? []) {
    qDecision = await h({ agent: qAgent, callId: 'q-c1', name: 'ask_user_question' }, { isError: false, content: [] }, async () => qDecision)
  }
  const qFlushed = (qDecision.additionalContexts ?? []).filter((m) => m?.source?.form === 'notice')
  for (const msg of qFlushed) appendStep(qLog, { type: 'user/message', data: msg })
  const qNoticeIdx = qLog.findIndex((e) => e.type === 'user/message' && e.data?.source?.form === 'notice')
  check('提问 notice: ✅ 回答提示在 tool/result 之后安全落点', qFlushed.length === 1 && qNoticeIdx > qToolResultIdx)
  check('提问 notice: 落点后序列合法(无 INVALID_REQUEST), ⏳ 已由 inbox 检查承接',
    qFlushed.some((m) => String(m.content?.[0]?.text ?? '').includes('已由 MCP 侧回答')) && modelSequenceError(qLog) === null)

  // ── 3. 启动存量捞回(sessions.list + sessionPersistence.list 两源) ──
  await new Promise((r) => setTimeout(r, 500))
  check('存量捞回: live 列表会话补挂', attachedIds.includes('sess-live2'))
  check('存量捞回: 持久化会话补挂', attachedIds.includes('sess-persisted'))

  // ── 18. 旧宿主(≤0.1.1)兼容探测 —— 第二个 fake ctx 暴露 registerProvider, 插件必须走老路径 ──
  // 放在末尾: 第二次 apply 会改写模块级 questionsProviderOurs/pendingQuestions, 不能影响前面的用例
  {
    let legacyAsk
    const legacyEvents = new Map()
    const legacyCtx = {
      ...ctx,
      get: (name) => (name === 'userQuestions'
        ? { registerProvider: (p) => { legacyAsk = p.ask; return () => { legacyAsk = undefined } } }
        : ctx.get(name)),
      on: (name, handler, options) => {
        const list = legacyEvents.get(name) ?? []
        if (options?.prepend) list.unshift(handler); else list.push(handler)
        legacyEvents.set(name, list)
        return () => { const i = list.indexOf(handler); if (i >= 0) list.splice(i, 1) }
      },
    }
    const mainDisposer = disposer
    await apply(legacyCtx, { port: PORT + 1, host: '127.0.0.1', workspaceRoots: [FAKE_CWD] })
    // fake ctx.effect 会把模块级 disposer 变量改写为第二次 apply 的清理函数: 取出后立刻还原,
    // 让末尾的 disposer() 仍关第一个 server(legacyDisposer 只用于本块收尾)
    const legacyDisposer = disposer
    disposer = mainDisposer
    await new Promise((r) => setTimeout(r, 200))
    check('旧宿主探测: registerProvider 存在时走老路径(不注册 waterfall listener)',
      typeof legacyAsk === 'function' && (legacyEvents.get('user-questions/request') ?? []).length === 0)
    if (typeof legacyAsk === 'function') {
      const legacyAnswerPromise = legacyAsk({
        questions: [{ id: 'lq1', question: 'legacy host?' }],
        agent: { id: 'sess-legacy', session: { id: 'sess-legacy' } },
        signal: new AbortController().signal,
      })
      await new Promise((r) => setTimeout(r, 100))
      const legacyListed = (await callIn('pending_prompts', { sessionId: 'sess-legacy' }))
        .prompts.find((p) => p.type === 'question' && p.questions?.[0]?.question === 'legacy host?')
      check('旧宿主: 提问挂起可被 pending_prompts 感知', legacyListed !== undefined)
      if (legacyListed !== undefined) {
        const legacyResp = await callIn('prompt_respond', { sessionId: 'sess-legacy', promptId: legacyListed.id, answer: 'yes' })
        check('旧宿主: prompt_respond 可答(老路径仍可用)', legacyResp.ok === true)
        check('旧宿主: 老路径答案形态与 0.1.5 一致', (await legacyAnswerPromise)?.answers?.[0]?.custom === 'yes')
      }
    }
    await legacyDisposer()
  }

  const failed = Object.entries(checks).filter(([, ok]) => !ok)
  for (const [name, ok] of Object.entries(checks)) console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}`)
  console.log('attach_session 路径记录:', JSON.stringify(attachedIds))
  console.log(`断言 ${Object.keys(checks).length} 项, 失败 ${failed.length} 项`)
  console.log(failed.length === 0 ? 'SMOKE PASS' : `SMOKE FAIL (${failed.length} 项)`)
  disposer()
  await new Promise((r) => setTimeout(r, 100))
  process.exit(failed.length === 0 ? 0 : 1)
} catch (e) {
  console.error('SMOKE ERROR:', e)
  process.exit(1)
}
