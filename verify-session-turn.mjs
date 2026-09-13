// v0.11.0 端到端验证(可复跑): 用真实 McpServer / StreamableHTTP / MCP 客户端驱动插件的工具面,
// 配一个"仿真 agent"(按真实 dsh-agent-loop 的时序追加 turn/start -> user/message -> assistant/message ->
// turn/end 并广播 session/event), 覆盖: session_send 立即返回、session_status 主动查询、session_tail
// 表面事件、session_wait 三种 until、session_cancel、冷读(官方 read 句柄 + zstd 落盘兜底)、interrupted。
//
// 用法: node verify-session-turn.mjs
//   段 5/7 需要本机 ~/.dsh 下存在 REAL_SESSION 指向的持久化会话(真实数据冷读), 换机器时改 REAL_SESSION
//   或忽略这两段; 段 6 自带伪造日志, 不依赖外部数据。
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'
import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

const PORT = 18090
const REAL_SESSION = 'd8bf7cdb-4773-4da8-8ae1-e7c2f5ed2b7f'
const FAKE_HOME = join(process.cwd(), '.tmp-e2e-dsh-home')

let pass = 0, fail = 0
const ok = (cond, label, extra) => {
  if (cond) { pass++; console.log(`  ✓ ${label}`) }
  else { fail++; console.log(`  ✗ ${label}${extra !== undefined ? ` — ${JSON.stringify(extra)}` : ''}`) }
}

// ── 仿真 cordis ctx ────────────────────────────────────────────────────────────
const bus = new Map()
const emit = (name, ...args) => { for (const fn of bus.get(name) ?? []) fn(...args) }
const on = (name, fn) => { const l = bus.get(name) ?? []; l.push(fn); bus.set(name, l); return () => { const i = l.indexOf(fn); if (i >= 0) l.splice(i, 1) } }

/** 一条"真实"的 session 事件流: seq 连续, append 后广播 session/event */
function makeSession(id, cwd) {
  const log = []
  let seq = 0
  const session = {
    id, header: { id, cwd, agentPreset: 'standard' }, log,
    append(type, data) { log.push({ type, seq: seq++, time: Date.now(), data }); emit('session/event', session, log[log.length - 1]); return seq },
  }
  return session
}

const agentsById = new Map()
const pool = new Map() // cwd -> agent

function makeAgent(session, { turnMs = 40 } = {}) {
  const timers = new Set()
  const agent = {
    id: session.id, session, status: 'idle',
    options: { provider: 'deepseek-official', model: 'deepseek-flash' },
    inbox: { nextTurn: [], nextStep: [], append() {}, clear() {} },
    _cancelled: false,
    _turn: 0,
    _pendingText: '',
    followup(message) {
      const text = message?.content?.find((c) => c.type === 'text')?.text ?? ''
      this._pendingText = text
      this.inbox.nextTurn.push(message)
      const t = setTimeout(() => { timers.delete(t); this._drive(turnMs) }, 5)
      timers.add(t)
    },
    _drive(ms) {
      const turn = ++this._turn
      this.inbox.nextTurn.length = 0
      session.append('turn/start', { turn })
      this.status = 'running'
      const push = (fn, delay) => { const t = setTimeout(() => { timers.delete(t); fn() }, delay); timers.add(t) }
      push(() => { session.append('step/start', { turn, step: 1 }) }, ms * 0.2)
      push(() => {
        session.append('user/message', { message: { role: 'user', content: [{ type: 'text', text: this._pendingText.slice(0, 400) }] }, source: { kind: 'plugin', plugin: 'harness-mcp-server' } })
        session.append('assistant/chunk', { chunk: { type: 'text', text: 'x'.repeat(50) } }) // 内部流式事件: tail 必须滤掉
      }, ms * 0.4)
      push(() => {
        if (this._cancelled) return
        session.append('assistant/message', { message: { role: 'assistant', content: [
          { type: 'text', text: 'hi from echo\n{"changes":"echo hi 已执行","verification":"exit 0; 输出 hi","leftovers":"none"}' },
          { type: 'tool_use', name: 'bash' }, // 非文本块: 不能被当成 "(no text blocks)"
        ] } })
      }, ms * 0.7)
      push(() => {
        session.append('step/end', { turn, step: 1 })
        session.append('turn/end', { turn, reason: { kind: this._cancelled ? 'aborted' : 'completed', ...(this._cancelled ? { reason: { kind: 'hook', reason: 'harness-mcp-cancel' } } : {}) } })
        this.status = 'idle'
        this._cancelled = false
      }, ms)
    },
    cancel(cause, options) {
      this._cancelled = true
      this._lastCancel = { cause, options }
      for (const t of timers) clearTimeout(t)
      timers.clear()
      // 真实语义: 中止活动回合 → turn/end reason.kind='aborted'
      if (this.status === 'running') {
        session.append('turn/end', { turn: this._turn, reason: { kind: 'aborted', reason: cause } })
        this.status = 'idle'
      }
      if (options?.keepInbox !== true) this.inbox.nextTurn.length = 0
    },
    async whenIdle() {},
    runMaintenance: (task) => task(new AbortController().signal),
    dispose: async () => { agentsById.delete(session.id) },
  }
  agentsById.set(session.id, agent)
  return agent
}

const agentsSvc = {
  create: async ({ sessionId, meta }) => {
    const session = makeSession(String(sessionId), meta?.cwd)
    const agent = makeAgent(session)
    pool.set(meta?.cwd, agent)
    return { agent, dispose: async () => { agentsById.delete(String(sessionId)); pool.delete(meta?.cwd) } }
  },
  resume: async ({ resumeSessionId }) => {
    const session = makeSession(String(resumeSessionId), process.cwd())
    session.log.push({ type: 'session', seq: -1 })
    const agent = makeAgent(session)
    return { agent, dispose: async () => { agentsById.delete(String(resumeSessionId)) } }
  },
  get: (sid) => agentsById.get(String(sid)),
  list: () => [...agentsById.values()],
}

const services = {
  agents: agentsSvc,
  llm: {
    listProviders: () => [{ id: 'deepseek-official', name: 'DeepSeek' }],
    listModels: async () => [{ id: 'deepseek-flash', name: 'flash' }],
    listConfigurableProviders: () => [],
    resolveModelInfo: async () => ({ context: { contextWindow: 128000 } }),
  },
  agentPresets: { list: async () => [{ id: 'standard', name: 'Standard' }], resolve: async (id) => ({ id }), defaultId: 'standard' },
  workspaceRegistry: { list: () => [], resolveByPath: async () => undefined, create: async () => undefined },
  sessions: { get: (id) => { const a = agentsById.get(String(id)); return a === undefined ? undefined : { header: a.session.header, ...a.session } }, list: () => [], flush: async () => {} },
  tokenMeter: { measure: (session) => ({ logRevision: session.log.length, surfaceTokens: 1234, totalTokens: 2345 }) },
  // 注意: 不提供 sessionPersistence / sessionProjections → 走 zstd 落盘兜底与日志折叠
}

const ctx = {
  tools: { keys: () => ['bash', 'fs_read', 'fs_write'] },
  agents: agentsSvc,
  get: (n) => services[n],
  on: (name, fn) => on(name, fn),
  effect: () => () => {},
}

// ── 启动插件(真实 apply + 真实 HTTP/StreamableHTTP + 真实 MCP 工具面) ────────────
const { apply, VERSION } = await import('./lib/index.js')
await apply(ctx, { http: true, port: PORT, host: '127.0.0.1' })
await new Promise((r) => setTimeout(r, 400))

const client = new Client({ name: 'e2e', version: '0.0.1' })
await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${PORT}/mcp`)))

async function call(tool, args, expectError = false) {
  const res = await client.callTool({ name: tool, arguments: args })
  const text = (res.content ?? []).filter((c) => c.type === 'text').map((c) => c.text).join('\n')
  if (res.isError && !expectError) throw new Error(`${tool} isError: ${text}`)
  if (expectError && !res.isError) throw new Error(`${tool} expected isError: ${text}`)
  try { return JSON.parse(text) } catch { return { raw: text } }
}

console.log(`\n== 0) 工具面 (v${VERSION}) ==`)
const { tools } = await client.listTools()
const names = tools.map((t) => t.name).sort()
const required = ['session_send', 'session_status', 'session_tail', 'session_wait', 'session_cancel']
const removed = ['agent_run', 'task_inbox', 'task_result', 'task_list', 'task_wait', 'task_cancel']
ok(required.every((n) => names.includes(n)), '新增 5 个 session_* 工具', names)
ok(removed.every((n) => !names.includes(n)), '旧 6 个 task/agent_run 工具已同版删除', names)
ok(names.length === 20, '工具总数 = 20(5 新增 + 15 保留; session_close 已删除)', { count: names.length, names })

console.log('\n== 1) session_send 立即返回 + 主动查询到 completed ==')
const t0 = Date.now()
const send = await call('session_send', { cwd: process.cwd(), message: 'echo hi', context: '来自 Hermes 的记忆' })
const sendMs = Date.now() - t0
ok(typeof send.sessionId === 'string' && send.sessionId.length > 0, 'session_send 返回 sessionId', send)
ok(send.status === 'accepted', 'status=accepted', send)
ok(sendMs < 1500, `立即返回(未等待 turn 结束): ${sendMs}ms`, { sendMs })
ok(typeof send.inboxDepth === 'number', 'inboxDepth 存在', send)
const sid = send.sessionId

let status
for (let i = 0; i < 60; i++) {
  status = await call('session_status', { sessionId: sid })
  if (status.lastTurn?.reason?.kind === 'completed') break
  await new Promise((r) => setTimeout(r, 100))
}
ok(status.live === true && status.source === 'live', 'session_status(live) 命中内存日志', status)
ok(status.lastTurn?.turn === 1 && status.lastTurn?.reason?.kind === 'completed', 'lastTurn = {turn:1, completed}', status.lastTurn)
ok(status.openTurn === null, 'openTurn=null', status.openTurn)
ok(status.phase === 'idle', 'phase=idle', status.phase)
ok(status.changes === 'echo hi 已执行', 'changes 提取成功', { changes: status.changes })
ok(status.verification === 'exit 0; 输出 hi', 'verification 提取成功', { verification: status.verification })
ok(status.leftovers === 'none', 'leftovers 提取成功', { leftovers: status.leftovers })
ok(status.context?.events > 0 && status.context?.ratio !== null, 'context(events/ratio) 可用', status.context)
ok(String(status.lastText).startsWith('hi from echo'), 'lastText 只取文本块(忽略 tool_use)', { lastText: status.lastText })

console.log('\n== 2) session_tail 过程明细(LLM 友好) ==')
const tail = await call('session_tail', { sessionId: sid, n: 20 })
ok(tail.source === 'live', 'source=live', tail)
const kinds = tail.events.map((e) => e.type)
ok(kinds.includes('turn/start') && kinds.includes('turn/end'), 'tail 含 turn 边界', kinds)
ok(kinds.includes('assistant/message') && kinds.includes('user/message'), 'tail 含 user/assistant 文本消息', kinds)
ok(!kinds.includes('assistant/chunk') && !kinds.includes('step/start'), 'tail 滤除内部流式/step 事件', kinds)
ok(!tail.events.some((e) => e.text === '(no text blocks)'), 'tail 无 "(no text blocks)" 噪声', tail.events.map((e) => e.text))
ok(tail.events.every((e) => typeof e.text === 'string' && e.text.length <= 2000), 'text 统一 ≤2k', tail.events.map((e) => e.text.length))
ok(tail.events.every((e) => e.turn === 1), '每条带 turn 号', tail.events.map((e) => e.turn))
const surfaceOnly = await call('session_tail', { sessionId: sid, n: 10, filter: 'surface' })
ok(surfaceOnly.events.every((e) => ['user/message', 'assistant/message', 'turn/start', 'turn/end'].includes(e.type)), 'filter=surface 只留消息与 turn 边界', surfaceOnly.events.map((e) => e.type))
const toolsOnly = await call('session_tail', { sessionId: sid, n: 10, filter: 'tools' })
ok(toolsOnly.events.length === 0, 'filter=tools 在无工具调用时为空', toolsOnly.events)
const sinceSeq = await call('session_tail', { sessionId: sid, n: 50, sinceSeq: 2 })
ok(sinceSeq.events.every((e) => (e.seq ?? 0) > 2), 'sinceSeq 过滤生效', sinceSeq.events.map((e) => e.seq))

console.log('\n== 3) session_wait(turn-end) 实测 ==')
const send2 = await call('session_send', { sessionId: sid, message: 'echo hi again' })
const w0 = Date.now()
const wa = await call('session_wait', { sessionId: sid, until: 'turn-end', timeoutMs: 20000, sinceTurn: 2 })
ok(wa.timeout === false, 'turn-end 在超时前落定', wa)
ok(wa.status?.lastTurn?.turn === 2 && wa.status?.lastTurn?.reason?.kind === 'completed', '第 2 个 turn completed', wa.status?.lastTurn)
ok(Date.now() - w0 < 10000, `等待时长合理: ${Date.now() - w0}ms`)
const wb = await call('session_wait', { sessionId: sid, until: 'idle', timeoutMs: 20000 })
ok(wb.timeout === false && wb.status?.phase === 'idle', 'idle 立即满足', wb.status?.phase)
const send3 = await call('session_send', { sessionId: sid, message: 'long-ish turn' })
const wc = await call('session_wait', { sessionId: sid, until: 'input', timeoutMs: 1500 })
ok(wc.timeout === true, 'input 无弹窗 → 超时返回 {timeout:true, status}', { timeout: wc.timeout, phase: wc.status?.phase })
ok(wc.status !== undefined, '超时响应仍带 status', Object.keys(wc))

console.log('\n== 4) session_cancel 实测 ==')
const send4 = await call('session_send', { sessionId: sid, message: 'cancel me' })
await new Promise((r) => setTimeout(r, 15)) // 让 turn/start 落下(status=running)
const ca = await call('session_cancel', { sessionId: sid })
ok(ca.live === true && ca.cancelled === true, 'session_cancel 命中 live 会话', ca)
const st4 = await call('session_status', { sessionId: sid })
ok(st4.lastTurn?.reason?.kind === 'aborted', "cancel 后 turn/end reason.kind='aborted'", st4.lastTurn)
ok(st4.phase === 'idle', 'cancel 后 phase 回 idle', st4.phase)
const cb = await call('session_cancel', { sessionId: 'no-such-session-id' })
ok(cb.live === false && cb.cancelled === false, '非 live 会话 cancel = noop', cb)

console.log('\n== 5) 冷读真实持久化会话(~/.dsh/sessions 落盘 zstd 兜底) ==')
delete process.env.DSH_HOME
const cold = await call('session_status', { sessionId: REAL_SESSION })
ok(cold.live === false && cold.source === 'persisted', '非 live → 冷读持久化日志', { live: cold.live, source: cold.source })
ok(cold.phase === 'idle', 'phase=idle(末 turn 已闭合)', cold.phase)
ok(cold.lastTurn?.turn === 4, 'lastTurn.turn = 4', cold.lastTurn)
ok(cold.lastTurn?.reason?.kind === 'completed', "lastTurn.reason.kind = 'completed'", cold.lastTurn)
ok(cold.logEvents > 2000, 'logEvents 为整份日志条数', { logEvents: cold.logEvents })
console.log('    [真实会话 d8bf7cdb 冷读结果]', JSON.stringify({ phase: cold.phase, lastTurn: cold.lastTurn, changes: cold.changes, verification: cold.verification, leftovers: cold.leftovers, lastText: String(cold.lastText).slice(0, 200) }, null, 2))
ok(cold.context === null, '非 live context=null(无 Session 对象可计量)', cold.context)
ok(typeof cold.note === 'string' && cold.note.includes('cold read'), 'note 标注冷读来源', cold.note)
const coldTail = await call('session_tail', { sessionId: REAL_SESSION, n: 4 })
ok(coldTail.source === 'persisted' && coldTail.events.length === 4, '冷读 session_tail 可用', { n: coldTail.events.length, source: coldTail.source })
ok(coldTail.events.some((e) => e.type === 'turn/end'), '冷读 tail 含 turn/end 边界', coldTail.events.map((e) => e.type))
const coldCancel = await call('session_cancel', { sessionId: REAL_SESSION })
ok(coldCancel.live === false && coldCancel.cancelled === false, '冷会话 cancel = noop', coldCancel)
const coldWait = await call('session_wait', { sessionId: REAL_SESSION, timeoutMs: 30000 })
ok(coldWait.live === false && coldWait.timeout === false, '非 live session_wait 立即返回(不阻塞)', coldWait.live)
ok(coldWait.status?.lastTurn?.turn === 4, '非 live wait 直接带 status', coldWait.status?.lastTurn)
const missing = await call('session_status', { sessionId: 'does-not-exist-0000' }, true)
ok(missing.error !== undefined, '未知会话报结构化错误', missing)

console.log('\n== 6) 未闭合 turn → phase=interrupted(伪造持久化日志) ==')
rmSync(FAKE_HOME, { recursive: true, force: true })
const fakeId = randomUUID()
const dir = join(FAKE_HOME, 'sessions', '--tmp-fake-ws--', fakeId)
mkdirSync(dir, { recursive: true })
const lines = [
  JSON.stringify({ type: 'session', version: 3, id: fakeId, createdAt: Date.now(), cwd: '/tmp/fake-ws', isSeeded: false, delegationDepth: 0 }),
  JSON.stringify({ type: 'turn/start', seq: 0, time: Date.now(), data: { turn: 1 } }),
  JSON.stringify({ type: 'turn/end', seq: 1, time: Date.now(), data: { turn: 1, reason: { kind: 'completed' } } }),
  JSON.stringify({ type: 'turn/start', seq: 2, time: Date.now(), data: { turn: 2 } }),
  JSON.stringify({ type: 'assistant/message', seq: 3, time: Date.now(), data: { message: { role: 'assistant', content: [{ type: 'text', text: 'partial output, no summary json yet' }] } } }),
]
writeFileSync(join(dir, 'session.v3.jsonl'), lines.join('\n') + '\n')
process.env.DSH_HOME = FAKE_HOME
const inter = await call('session_status', { sessionId: fakeId })
ok(inter.phase === 'interrupted', '末尾 turn/start 无 turn/end → phase=interrupted', inter)
ok(inter.openTurn?.turn === 2, 'openTurn = {turn:2}', inter.openTurn)
ok(inter.lastTurn?.turn === 1 && inter.lastTurn?.reason?.kind === 'completed', 'lastTurn 仍是已闭合的 turn 1', inter.lastTurn)
ok(inter.changes === null && inter.verification === null && inter.leftovers === null, '提不到 summary → 三字段 null', { c: inter.changes, v: inter.verification, l: inter.leftovers })
ok(inter.cwd === '/tmp/fake-ws', '冷读 header 的 cwd', inter.cwd)
const interTail = await call('session_tail', { sessionId: fakeId, n: 10 })
ok(interTail.events.length === 4, '伪造日志 tail 读回 4 条表面事件', interTail.events.map((e) => e.type))
delete process.env.DSH_HOME

console.log('\n== 7) 冷读主路径: 官方 sessionPersistence open(id,"read") 尾部窗口 ==')
// 用真实日志的事件喂一个符合宿主 SessionHandle 契约的假句柄, 验证窗口化读取逻辑。
const { execFileSync } = await import('node:child_process')
const { readdirSync } = await import('node:fs')
const realRoot = join(process.env.HOME, '.dsh', 'sessions')
let realFile
for (const p of readdirSync(realRoot)) {
  const d = join(realRoot, p, REAL_SESSION)
  try { for (const f of readdirSync(d)) if (/^session(\.v\d+)?\.jsonl\.zstd$/.test(f)) realFile = join(d, f) } catch { /* not here */ }
}
ok(typeof realFile === 'string', '定位真实持久化日志文件', realFile)
const realRows = execFileSync('zstd', ['-dc', '--', realFile], { maxBuffer: 512 * 1024 * 1024 }).toString('utf8')
  .split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l))
const realEvents = realRows.filter((r) => r.type !== 'session')
ok(realEvents.length > 2000, '真实日志事件数 > 2000', { n: realEvents.length })

const reads = []
let closed = 0
services.sessionPersistence = {
  stat: async () => ({ eventCount: realEvents.length }),
  open: async () => ({
    header: { cwd: '/home/ziqiang/code/gitc-mainsite', agentPreset: 'standard' },
    read: async (offset = 0, length) => {
      reads.push({ offset, length })
      return { events: realEvents.slice(offset, length === undefined ? undefined : offset + length) }
    },
    close: async () => { closed++ },
  }),
  list: async () => [{ id: REAL_SESSION, cwd: '/home/ziqiang/code/gitc-mainsite', agentPreset: 'standard' }],
}
const pSt = await call('session_status', { sessionId: REAL_SESSION })
ok(pSt.source === 'persisted' && pSt.note.includes('source=persistence'), '走 sessionPersistence 主路径(非落盘兜底)', pSt.note)
ok(pSt.lastTurn?.turn === 4 && pSt.lastTurn?.reason?.kind === 'completed', '窗口化读取仍折叠出 turn 4 completed', pSt.lastTurn)
ok(pSt.logEvents === realEvents.length, 'logEvents = stat().eventCount', { logEvents: pSt.logEvents, n: realEvents.length })
ok(reads.length >= 1 && reads[0].offset > 0, '首次读取即取尾部窗口(offset > 0)', reads)
ok(reads.some((r) => r.length >= 2000), '窗口长度受控(≥2000, 不读全量)', reads.slice(0, 3))
ok(closed === 1, 'read 句柄被 close(不留悬挂句柄)', { closed })
const pTail = await call('session_tail', { sessionId: REAL_SESSION, n: 3 })
ok(pTail.source === 'persisted' && pTail.events.length === 3, 'session_tail 复用主路径', { source: pTail.source, n: pTail.events.length })
ok(closed === 2, '每次冷读各自 close', { closed })
// stat 不可用 → 退化为 read(0) 全量
services.sessionPersistence.stat = async () => { throw new Error('no metadata') }
const pSt2 = await call('session_status', { sessionId: REAL_SESSION })
ok(pSt2.lastTurn?.turn === 4, 'stat 缺失时退化为全量读取仍正确', pSt2.lastTurn)
ok(reads[reads.length - 1].offset === 0, '退化路径 read(0)', reads[reads.length - 1])
// open 失败(会话不存在/拒绝) → 落盘兜底
services.sessionPersistence.open = async () => { throw new Error('not found') }
const pSt3 = await call('session_status', { sessionId: REAL_SESSION })
ok(pSt3.note.includes('source=file'), 'open 抛错 → 落盘 zstd 兜底', pSt3.note)
delete services.sessionPersistence

console.log(`\n── 结果: ${pass} passed, ${fail} failed ──`)
await client.close()
rmSync(FAKE_HOME, { recursive: true, force: true })
process.exit(fail === 0 ? 0 : 1)
