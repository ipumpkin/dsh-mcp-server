// Dev-only smoke test (not shipped): drives apply() with a minimal fake ctx and verifies the
// increments on top of upstream:
//   1. 任意会话续接: live 接管 / 持久化 resume / 明确报错(三级)
//   2. realpath 规范化: create 的 meta.cwd 为 realpath 值; 目录不存在时回退 resolve 不阻断
//   3. attach_session 工具 + 启动存量捞回(workspaceRegistry/sessions/sessionPersistence 三服务路径)
//   4. 任务超时保护: whenIdle 卡死 → taskTimeoutMs 到点自动 cancel → timeout 标记 + leftovers 引导
//   5. 异步队列: task_inbox → task_result; task_list 列出; task_cancel(未知报错 / 已结束 no-op)
//   6. 外部显式控制会话复用: newSession 强制全新会话(旧池会话退役)、session_list 三层盘点、session_close 退役池会话
//   7. 上下文占用与压缩: session_list/task_list 输出 events/tokens/pressure/window/ratio; session_compact 走 ctx.compaction
//   8. 运维/审计/选型: harness_status 水位、session_read 事件流、workspace_list 分组、model_list 目录、agent_run 按次 model 覆盖
//   9. 客户端契约: 缺 cwd 报错、agent_run 超时转异步(taskId)+task_cancel、task_wait 阻塞等待、同步结果回填 taskId、isError 标记
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
// 卡死 cwd: fake create 对这个 cwd 返回 whenIdle 永不落定的 agent, 只等 cancel
const HANG_CWD = resolve(FAKE_CWD, 'hang-zone')
// 第二个卡死 cwd: 供「agent_run 超时转异步」用例使用(首次创建的 hang agent 未释放)
const HANG_CWD2 = resolve(FAKE_CWD, 'hang-zone-2')
// 第三/四个卡死 cwd: 等锁取消用例(HANG_CWD3)与 per-task 超时用例(HANG_CWD4)
const HANG_CWD3 = resolve(FAKE_CWD, 'hang-zone-3')
const HANG_CWD4 = resolve(FAKE_CWD, 'hang-zone-4')
const HANG_SET = new Set([HANG_CWD, HANG_CWD2, HANG_CWD3, HANG_CWD4])

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

function makeAgent(id, cwd) {
  return {
    session: { id, log: [], events: [], header: { version: 0, id, createdAt: Date.now(), cwd } },
    options: { provider: 'deepseek-official', model: 'deepseek-v4-flash' },
    status: 'idle',
    followup: () => {},
    whenIdle: async () => {},
    cancel: () => {},
    runMaintenance: async (task) => task(new AbortController().signal),
  }
}

const liveAgent = makeAgent('sess-live', FAKE_CWD)
const liveSession2 = { id: 'sess-live2', log: [], events: [], header: { version: 0, id: 'sess-live2', createdAt: 1, cwd: FAKE_CWD } }

const fakeSessions = {
  get: (id) => (id === 'sess-live' ? liveAgent.session : id === 'sess-live2' ? liveSession2 : undefined),
  list: () => [liveSession2],
  flush: async (session) => { flushed.push(session.id); return true },
}
const fakePersistence = {
  list: async () => [{ version: 0, id: 'sess-persisted', createdAt: 1, cwd: FAKE_CWD }],
}

// tokenMeter / llm / compaction 的只读 fake: 测量按日志长度估 token(sess-live 特例 30 验证 ratio);
// llm.resolveModel 固定窗口 200; compactNow 记录调用并返回固定结果
const fakeMeter = {
  measure: (session) => {
    const len = session?.log?.length ?? 0
    const tokens = session?.id === 'sess-live' ? 30 : len * 10
    return { logRevision: len, totalTokens: len * 10, surfaceTokens: tokens }
  },
}
const fakeLlm = {
  resolveModel: async () => ({ id: 'deepseek-v4-flash', context: { contextWindow: 200 } }),
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
    get: (id) => (id === 'sess-live' ? liveAgent : undefined),
    list: () => [liveAgent],
    create: async ({ sessionId, meta, agentOptions }) => {
      const id = String(sessionId)
      created.push({ id, cwd: meta?.cwd, options: agentOptions })
      const agent = makeAgent(id, meta?.cwd)
      // 卡死 agent: whenIdle 只在 cancel 后落定(模拟真实 dsh: cancel 打断活动回合)
      if (HANG_SET.has(meta?.cwd)) {
        let released = false
        let release
        agent.whenIdle = () => (released ? Promise.resolve() : new Promise((res) => { release = () => { released = true; res() } }))
        agent.cancel = () => { release?.() }
      }
      return { agent, dispose: async () => { disposed.push(id) } }
    },
    resume: async ({ resumeSessionId }) => {
      const id = String(resumeSessionId)
      if (id !== 'sess-persisted') throw new Error(`no persisted session "${id}"`)
      resumed.push(id)
      return { agent: makeAgent(id, FAKE_CWD), dispose: async () => { disposed.push(id) } }
    },
  },
  agentPresets: { mount: async () => ({ id: 'standard' }) },
  sessions: fakeSessions,
  sessionPersistence: fakePersistence,
  workspaceRegistry: wsRegistry,
  effect: (fn) => { disposer = fn(); return disposer },
  get: (name) => (name === 'workspaceRegistry' ? wsRegistry
    : name === 'sessions' ? fakeSessions
    : name === 'sessionPersistence' ? fakePersistence
    : name === 'tokenMeter' ? fakeMeter
    : name === 'llm' ? fakeLlm
    : name === 'compaction' ? fakeCompaction
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

const checks = {}
let rid = 0
const call = (sessionId, name, args) => rpc(sessionId, {
  jsonrpc: '2.0', id: ++rid, method: 'tools/call',
  params: { name, arguments: args ?? {} },
})
try {
  await apply(ctx, { port: PORT, host: '127.0.0.1', taskTimeoutMs: 600, workspaceRoots: [FAKE_CWD] })
  await new Promise((r) => setTimeout(r, 400))

  const init = await rpc(undefined, {
    jsonrpc: '2.0', id: 1, method: 'initialize',
    params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'smoke', version: '1.0' } },
  })
  checks['initialize 拿到 sessionId'] = Boolean(init.sid)
  await rpc(init.sid, { jsonrpc: '2.0', method: 'notifications/initialized' })

  const echo = await call(init.sid, 'echo', { text: 'ping-8099' })
  checks['echo 通'] = echo.status === 200 && echo.text.includes('ping-8099')

  const toolsList = await rpc(init.sid, { jsonrpc: '2.0', id: ++rid, method: 'tools/list', params: {} })
  const toolNames = parsePayload(toolsList.text).result?.tools?.map((t) => t.name) ?? []
  for (const t of ['attach_session', 'task_list', 'task_cancel', 'session_list', 'session_close', 'session_compact',
    'harness_status', 'model_list', 'workspace_list', 'session_read', 'task_wait']) {
    checks[`工具清单含 ${t}`] = toolNames.includes(t)
  }

  // ── 增量3: attach_session 工具(live / 持久化 / 未知三态) ──
  const attachLive = await call(init.sid, 'attach_session', { sessionId: 'sess-live' })
  checks['attach_session live 会话'] = attachLive.status === 200 && innerOf(attachLive).attached === true

  const attachMissing = await call(init.sid, 'attach_session', { sessionId: 'sess-nope' })
  checks['attach_session 未知会话报错'] = attachMissing.status === 200 && typeof innerOf(attachMissing).error === 'string'
  checks['错误响应带 isError 标记'] = parsePayload(attachMissing.text).result?.isError === true

  const attachPersisted = await call(init.sid, 'attach_session', { sessionId: 'sess-persisted' })
  checks['attach_session 持久化会话(经 sessionPersistence)'] = attachPersisted.status === 200 && innerOf(attachPersisted).attached === true

  // ── 增量1: 任意会话续接三级 ──
  const runLive = await call(init.sid, 'agent_run', { task: 'say ok', sessionId: 'sess-live' })
  const runLiveInner = runLive.status === 200 ? innerOf(runLive) : { error: 'bad' }
  checks['agent_run 接管 live 会话(不 resume 不 dispose)'] = runLiveInner.sessionId === 'sess-live' && resumed.length === 0 && disposed.length === 0

  const runPersisted = await call(init.sid, 'agent_run', { task: 'say ok', sessionId: 'sess-persisted' })
  const runPersistedInner = runPersisted.status === 200 ? innerOf(runPersisted) : { error: 'bad' }
  checks['agent_run 持久化会话 resume + flush + dispose'] = runPersistedInner.sessionId === 'sess-persisted'
    && resumed.includes('sess-persisted') && flushed.includes('sess-persisted') && disposed.includes('sess-persisted')

  const runUnknown = await call(init.sid, 'agent_run', { task: 'say ok', sessionId: 'sess-unknown' })
  checks['agent_run 未知会话明确报错'] = runUnknown.status === 200 && String(innerOf(runUnknown).error ?? '').includes('session not found for resume')

  // ── 增量2: realpath 规范化 ──
  const runNew = await call(init.sid, 'agent_run', { task: 'say ok', cwd: FAKE_CWD })
  const runNewInner = runNew.status === 200 ? innerOf(runNew) : { error: 'bad' }
  checks['agent_run 池新建: meta.cwd 为 realpath 值'] = Boolean(created[0]) && created[0].cwd === FAKE_CWD && runNewInner.sessionId === created[0].id
  checks['agent_run 同步结果回填真实 taskId'] = Boolean(runNewInner.taskId)

  const missingDir = resolve(FAKE_CWD, 'nonexistent-xyz')
  const runMissingCwd = await call(init.sid, 'agent_run', { task: 'say ok', cwd: missingDir })
  const runMissingInner = runMissingCwd.status === 200 ? innerOf(runMissingCwd) : { error: 'bad' }
  checks['目录不存在: realpath 回退 resolve 且不阻断'] = Boolean(runMissingInner.sessionId) && created[1]?.cwd === missingDir

  // ── 增量4: 超时保护(内部 taskTimeoutMs 自动 cancel; timeoutMs 调大让同步路径完整返回) ──
  const runHang = await call(init.sid, 'agent_run', { task: 'hang forever', cwd: HANG_CWD, timeoutMs: 5000 })
  const runHangInner = runHang.status === 200 ? innerOf(runHang) : { error: 'bad' }
  checks['超时自动 cancel: timeout 标记'] = runHangInner.timeout === true && runHangInner.sessionId === created[2]?.id
  checks['超时后 leftovers 给续接引导'] = String(runHangInner.leftovers ?? '').includes('续接')

  // ── 增量5: 异步队列 + task_list / task_cancel ──
  const inbox = await call(init.sid, 'task_inbox', { task: 'say ok async', cwd: FAKE_CWD })
  const inboxInner = inbox.status === 200 ? innerOf(inbox) : { error: 'bad' }
  checks['task_inbox 返回 taskId'] = Boolean(inboxInner.taskId)

  let done
  for (let i = 0; i < 50; i++) {
    await new Promise((r) => setTimeout(r, 50))
    const res = await call(init.sid, 'task_result', { taskId: inboxInner.taskId })
    const inner = innerOf(res)
    if (inner.status === 'done' || inner.status === 'error') { done = inner; break }
  }
  checks['task_inbox 异步执行到 done'] = done?.status === 'done' && Boolean(done.result?.sessionId)

  const cancelDone = await call(init.sid, 'task_cancel', { taskId: inboxInner.taskId })
  checks['task_cancel 已结束任务 no-op'] = innerOf(cancelDone).cancelled === false && innerOf(cancelDone).note === 'already finished'

  const cancelUnknown = await call(init.sid, 'task_cancel', { taskId: 'nope-123' })
  checks['task_cancel 未知任务报错'] = typeof innerOf(cancelUnknown).error === 'string'

  const list = await call(init.sid, 'task_list', {})
  const listInner = innerOf(list)
  checks['task_list 列出任务(含刚完成的 taskId)'] = Array.isArray(listInner.tasks)
    && listInner.tasks.some((t) => t.taskId === inboxInner.taskId && t.status === 'done')
  const inboxTaskRow = listInner.tasks.find((t) => t.taskId === inboxInner.taskId)
  checks['task_list: 任务会话上下文占用非空'] = inboxTaskRow?.context !== null
    && typeof inboxTaskRow?.context?.events === 'number' && typeof inboxTaskRow?.context?.tokens === 'number'
  checks['task_list: 上下文带窗口与占用比'] = inboxTaskRow?.context?.window === 200 && typeof inboxTaskRow?.context?.ratio === 'number'

  // ── 增量6: 外部显式控制会话复用(newSession / session_list / session_close) ──
  const freshRun = await call(init.sid, 'agent_run', { task: 'fresh session', cwd: FAKE_CWD, newSession: true })
  const freshInner = freshRun.status === 200 ? innerOf(freshRun) : { error: 'bad' }
  checks['newSession: 强制全新会话(不池命中旧会话)'] = freshInner.sessionId === created[3]?.id && freshInner.sessionId !== created[0]?.id
  checks['newSession: 旧池会话被退役 dispose'] = disposed.includes(String(created[0]?.id))

  const reuseRun = await call(init.sid, 'agent_run', { task: 'reuse again', cwd: FAKE_CWD })
  checks['缺省: 复用池里的新会话'] = (reuseRun.status === 200 ? innerOf(reuseRun) : {}).sessionId === created[3]?.id

  const sessions = await call(init.sid, 'session_list', {})
  const sessionsInner = innerOf(sessions)
  checks['session_list: 池会话可见'] = Array.isArray(sessionsInner.sessions)
    && sessionsInner.sessions.some((s) => s.sessionId === created[3]?.id && s.source === 'pool')
  checks['session_list: live 会话可见'] = sessionsInner.sessions.some((s) => s.sessionId === 'sess-live' && s.source === 'live')
  checks['session_list: 持久化会话可见'] = sessionsInner.sessions.some((s) => s.sessionId === 'sess-persisted' && s.source === 'persisted')
  const poolRow = sessionsInner.sessions.find((s) => s.sessionId === created[3]?.id)
  const liveRow = sessionsInner.sessions.find((s) => s.sessionId === 'sess-live')
  const persistedRow = sessionsInner.sessions.find((s) => s.sessionId === 'sess-persisted')
  checks['session_list: 池/live 行带上下文占用'] = poolRow?.context !== null && liveRow?.context !== null
    && typeof poolRow?.context?.tokens === 'number' && typeof liveRow?.context?.tokens === 'number'
  checks['session_list: 上下文带窗口与占用比'] = poolRow?.context?.window === 200 && poolRow?.context?.ratio === 0
    && liveRow?.context?.tokens === 30 && liveRow?.context?.ratio === 15
  checks['session_list: 持久化行上下文为 null'] = persistedRow?.context === null

  // ── 增量7: 上下文压缩(session_compact) ──
  const compact = await call(init.sid, 'session_compact', { sessionId: String(created[3]?.id) })
  const compactInner = compact.status === 200 ? innerOf(compact) : { error: 'bad' }
  checks['session_compact: 压缩成功(ok+compactionId+shadowedNodes)'] = compactInner.ok === true
    && compactInner.compactionId === 'cmp-1' && compactInner.shadowedNodes === 3
  checks['session_compact: 调用了 compaction 服务'] = compactCalls.some((c) => c.sessionId === String(created[3]?.id))
  const compactUnknown = await call(init.sid, 'session_compact', { sessionId: 'sess-unknown' })
  checks['session_compact: 未知会话明确报错'] = compactUnknown.status === 200
    && String(innerOf(compactUnknown).error ?? '').includes('session not found')

  const close = await call(init.sid, 'session_close', { sessionId: String(created[3]?.id) })
  checks['session_close: 退役池会话'] = innerOf(close).closed === true && disposed.includes(String(created[3]?.id))
  const closeUnknown = await call(init.sid, 'session_close', { sessionId: 'sess-nope' })
  checks['session_close: 非池会话 no-op'] = innerOf(closeUnknown).closed === false

  // ── 增量8: 运维/审计/模型/工作区 + per-call model ──
  const status = await call(init.sid, 'harness_status', {})
  const statusInner = innerOf(status)
  checks['harness_status: 队列/池水位'] = statusInner.queue?.total >= 1 && statusInner.agentPool?.size >= 1
    && statusInner.config?.taskTimeoutMs === 600 && typeof statusInner.uptimeSec === 'number'

  const read = await call(init.sid, 'session_read', { sessionId: 'sess-live' })
  const readInner = read.status === 200 ? innerOf(read) : { error: 'bad' }
  checks['session_read: 返回事件流'] = readInner.sessionId === 'sess-live' && Array.isArray(readInner.events) && typeof readInner.total === 'number'
  const readUnknown = await call(init.sid, 'session_read', { sessionId: 'sess-unknown' })
  checks['session_read: 未知会话报错'] = String(innerOf(readUnknown).error ?? '').includes('session not found')

  const ws = await call(init.sid, 'workspace_list', {})
  const wsInner = innerOf(ws)
  checks['workspace_list: 列出工作区'] = Array.isArray(wsInner.workspaces)
    && wsInner.workspaces.some((w) => w.id === 'ws-fake' && w.path === FAKE_CWD)

  const modelRun = await call(init.sid, 'agent_run', { task: 'model test', cwd: FAKE_CWD, newSession: true, model: 'custom-model' })
  const modelInner = modelRun.status === 200 ? innerOf(modelRun) : { error: 'bad' }
  checks['agent_run model 参数: 透传到 create'] = modelInner.sessionId === created[4]?.id
    && created[4]?.options?.model === 'custom-model'

  const models = await call(init.sid, 'model_list', {})
  const modelsInner = innerOf(models)
  checks['model_list: 列出所有 provider 的模型'] = Array.isArray(modelsInner.providers)
    && modelsInner.providers.some((r) => r.provider === 'deepseek-official' && r.active === true
      && r.models.some((m) => m.id === 'deepseek-v4-flash'))
  checks['model_list: 含 zai 配置模型'] = modelsInner.providers.some((r) => r.provider === 'zai'
    && r.models.some((m) => m.id === 'glm-5.3'))
  checks['model_list: 目录补全未激活 provider'] = modelsInner.providers.some((r) => r.provider === 'custom-gw' && r.active === false)

  const modelsZai = await call(init.sid, 'model_list', { provider: 'zai' })
  const modelsZaiInner = innerOf(modelsZai)
  checks['model_list: 指定 provider 只列该 provider'] = modelsZaiInner.providers.length === 1
    && modelsZaiInner.providers[0].provider === 'zai' && modelsZaiInner.providers[0].models.length === 2

  const modelsWin = await call(init.sid, 'model_list', { withWindow: true })
  const winRow = innerOf(modelsWin).providers.find((r) => r.provider === 'deepseek-official')
  checks['model_list: withWindow 解析上下文窗口'] = winRow?.models.every((m) => m.contextWindow === 200)

  // ── 增量9: 客户端契约(缺 cwd / 超时转异步 / task_wait) ──
  const noCwd = await call(init.sid, 'agent_run', { task: 'no cwd' })
  checks['agent_run 缺 cwd 明确报错'] = noCwd.status === 200 && String(innerOf(noCwd).error ?? '').includes('cwd is required')

  const conv = await call(init.sid, 'agent_run', { task: 'convert me', cwd: HANG_CWD2, timeoutMs: 100 })
  const convInner = conv.status === 200 ? innerOf(conv) : { error: 'bad' }
  checks['agent_run 超时转异步(返回 taskId)'] = convInner.status === 'async' && Boolean(convInner.taskId)
  checks['转异步响应带进行中进度'] = convInner.progress?.status === 'running' && typeof convInner.progress?.events === 'number'

  const convCancel = await call(init.sid, 'task_cancel', { taskId: convInner.taskId })
  checks['agent_run 转异步后可 task_cancel 取消'] = innerOf(convCancel).cancelled === true

  let convDone
  for (let i = 0; i < 40; i++) {
    await new Promise((r) => setTimeout(r, 50))
    const res = await call(init.sid, 'task_result', { taskId: convInner.taskId })
    const inner = innerOf(res)
    if (inner.status === 'done' || inner.status === 'error') { convDone = inner; break }
  }
  checks['转异步任务最终 done 且 cancelled 标记'] = convDone?.status === 'done' && convDone?.cancelled === true

  const inboxWait = await call(init.sid, 'task_inbox', { task: 'wait me', cwd: FAKE_CWD })
  const waitRes = await call(init.sid, 'task_wait', { taskId: innerOf(inboxWait).taskId, timeoutMs: 3000 })
  const waitInner = waitRes.status === 200 ? innerOf(waitRes) : { error: 'bad' }
  checks['task_wait 阻塞等待到 done'] = waitInner.status === 'done' && Boolean(waitInner.result?.sessionId)
  checks['task_wait 结果带进度(终态)'] = waitInner.progress?.status === 'done' && typeof waitInner.progress?.toolCalls === 'number'
  const waitUnknown = await call(init.sid, 'task_wait', { taskId: 'nope-456' })
  checks['task_wait 未知任务报错'] = typeof innerOf(waitUnknown).error === 'string'

  // ── 增量10: 等锁任务取消 / per-task 超时 / task_list 过滤 / 白名单 ──
  const hold = await call(init.sid, 'task_inbox', { task: 'hold lock', cwd: HANG_CWD3 })
  const holdId = innerOf(hold).taskId
  await new Promise((r) => setTimeout(r, 150)) // A 进入 running 并挂起
  const behind = await call(init.sid, 'task_inbox', { task: 'behind lock', cwd: HANG_CWD3 })
  const behindId = innerOf(behind).taskId
  await new Promise((r) => setTimeout(r, 150)) // B 在 cwd 锁上等待, agent 未就绪
  const cancelBehind = await call(init.sid, 'task_cancel', { taskId: behindId })
  checks['等锁任务可取消(agent 未就绪路径)'] = innerOf(cancelBehind).cancelled === true
  const cancelHold = await call(init.sid, 'task_cancel', { taskId: holdId })
  checks['取消持锁任务释放锁'] = innerOf(cancelHold).cancelled === true
  let holdDone, behindDone
  for (let i = 0; i < 60; i++) {
    await new Promise((r) => setTimeout(r, 50))
    const rh = innerOf(await call(init.sid, 'task_result', { taskId: holdId }))
    const rb = innerOf(await call(init.sid, 'task_result', { taskId: behindId }))
    if (rh.status === 'done' || rh.status === 'error') holdDone = rh
    if (rb.status === 'done' || rb.status === 'error') behindDone = rb
    if (holdDone && behindDone) break
  }
  checks['等锁任务取消后以 error(cancelled)落定'] = behindDone?.status === 'error'
    && String(behindDone?.error ?? '').includes('task cancelled')
  checks['持锁任务取消后正常 done'] = holdDone?.status === 'done'

  const to = await call(init.sid, 'task_inbox', { task: 'per-task timeout', cwd: HANG_CWD4, timeoutMs: 200 })
  let toDone
  for (let i = 0; i < 60; i++) {
    await new Promise((r) => setTimeout(r, 50))
    const inner = innerOf(await call(init.sid, 'task_result', { taskId: innerOf(to).taskId }))
    if (inner.status === 'done' || inner.status === 'error') { toDone = inner; break }
  }
  checks['task_inbox per-task timeoutMs 生效'] = toDone?.status === 'done' && toDone?.result?.timeout === true

  const filt = await call(init.sid, 'task_list', { sessionId: String(created[4]?.id) })
  const filtInner = innerOf(filt)
  checks['task_list 按 sessionId 过滤'] = filtInner.tasks.length >= 1
    && filtInner.tasks.every((t) => t.sessionId === String(created[4]?.id))

  const attachOutside = await call(init.sid, 'attach_session', { sessionId: 'sess-persisted', path: '/tmp' })
  checks['attach_session 越界路径被白名单拒绝'] = attachOutside.status === 200
    && String(innerOf(attachOutside).error ?? '').includes('not allowed')

  // ── 增量3: 启动存量捞回(sessions.list + sessionPersistence.list 两源) ──
  await new Promise((r) => setTimeout(r, 500))
  checks['存量捞回: live 列表会话补挂'] = attachedIds.includes('sess-live2')
  checks['存量捞回: 持久化会话补挂'] = attachedIds.includes('sess-persisted')

  const failed = Object.entries(checks).filter(([, ok]) => !ok)
  for (const [name, ok] of Object.entries(checks)) console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}`)
  console.log('attach_session 路径记录:', JSON.stringify(attachedIds))
  console.log(failed.length === 0 ? 'SMOKE PASS' : `SMOKE FAIL (${failed.length} 项)`)
  disposer()
  await new Promise((r) => setTimeout(r, 100))
  process.exit(failed.length === 0 ? 0 : 1)
} catch (e) {
  console.error('SMOKE ERROR:', e)
  process.exit(1)
}
