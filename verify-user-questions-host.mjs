// Dev-only verification (not shipped, not part of the smoke suite):
// drives the REAL dsh-user-questions host package on the REAL cordis event bus to verify the
// dsh 0.1.5 migration the plugin now depends on:
//   1. 0.1.5 removed UserQuestionService.registerProvider (the old code probed it, found nothing
//      and silently registered no answerer — the reported bug), while ≤0.1.1 copies still have it
//   2. ask() dispatches the Agent-scoped waterfall event 'user-questions/request'
//   3. a plugin-style listener registered with { prepend: true } intercepts that waterfall
//      (outermost-first), and the answer it returns is what ask() resolves with
//   4. NOT calling next() vetoes later listeners (the web GUI's remote listener); calling next()
//      hands the request to them — the degradation path the plugin keeps
//   5. no answerer claiming the request → ask() rejects with code NO_PROVIDER
//   6. ask() honours the request.signal lifetime the plugin listener relies on
//
// Usage: node verify-user-questions-host.mjs
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { homedir } from 'node:os'
import { pathToFileURL } from 'node:url'

const results = []
const check = (scope, name, ok) => results.push([`${scope} ${name}`, ok])
const notes = []

/** Locate installed @deepseek-ai/dsh-user-questions copies (`lib/index.js` + package.json). */
function findInstalls() {
  const found = []
  const push = (dir, source) => {
    const pkgJson = join(dir, 'package.json')
    const entry = join(dir, 'lib', 'index.js')
    if (existsSync(pkgJson) && existsSync(entry)) {
      found.push({
        dir, entry, source,
        // <install root>/node_modules/@deepseek-ai/dsh-user-questions → the root that owns cordis
        root: join(dir, '..', '..', '..'),
        version: JSON.parse(readFileSync(pkgJson, 'utf8')).version,
      })
    }
  }
  const globalV11 = join(homedir(), '.local/share/pnpm/global/v11')
  if (existsSync(globalV11)) {
    for (const hash of readdirSync(globalV11)) push(join(globalV11, hash, 'node_modules/@deepseek-ai/dsh-user-questions'), `pnpm-global:${hash.slice(0, 8)}`)
  }
  push(join(homedir(), '.dsh/profiles/web/node_modules/@deepseek-ai/dsh-user-questions'), 'profile')
  push(join(homedir(), '.dsh/profiles/web/.dsh-module-fallback/node_modules/@deepseek-ai/dsh-user-questions'), 'profile-fallback')
  push(join(process.cwd(), 'node_modules/@deepseek-ai/dsh-user-questions'), 'checkout')
  return found
}

/** Every 0.1.1-era copy still present in the pnpm store (evidence for the removed API). */
function findStoreCopies() {
  const out = []
  const links = join(homedir(), '.local/share/pnpm/store/v11/links/@deepseek-ai')
  if (!existsSync(links)) return out
  for (const outer of readdirSync(links)) {
    for (const ver of readdirSync(join(links, outer))) {
      for (const hash of readdirSync(join(links, outer, ver))) {
        const base = join(links, outer, ver, hash, 'node_modules/@deepseek-ai/dsh-user-questions')
        const pkgJson = join(base, 'package.json')
        const entry = join(base, 'lib/index.js')
        if (existsSync(pkgJson) && existsSync(entry)) {
          out.push({ version: JSON.parse(readFileSync(pkgJson, 'utf8')).version, entry })
        }
      }
    }
  }
  return out
}

/** Run the waterfall-contract checks against one real host install. */
async function verifyInstall(host) {
  const scope = `[${host.version}@${host.source}]`
  const cordisUrl = pathToFileURL(join(host.root, 'node_modules/@deepseek-ai/cordis/lib/index.js')).href
  const { Context } = await import(cordisUrl)
  const uqModule = await import(pathToFileURL(host.entry).href)
  const UserQuestionService = uqModule.UserQuestionService ?? uqModule.default

  check(scope, 'service 不再暴露 registerProvider(旧代码静默跳过注册的根因)', typeof UserQuestionService.prototype.registerProvider === 'undefined')

  const answerOf = (custom) => ({ answers: [{ id: 'q1', selected: [], custom }] })
  // 模拟 web GUI 应答者(dsh-client-ui-user-questions: ctx.remote.$on('user-questions/request'))
  const guiListener = function () { return Promise.resolve(answerOf('gui-claimed')) }

  // ── A. 真实 ask() 走真实 waterfall: prepend 的插件 listener 抢先认领 ──
  {
    const ctx = new Context()
    const service = new UserQuestionService(ctx)
    check(scope, '真宿主可实例化 UserQuestionService 并挂到 ctx.userQuestions', typeof ctx.get('userQuestions')?.ask === 'function')
    const asked = []
    ctx.on('user-questions/request', function (request, next) { asked.push('gui'); return guiListener() })
    ctx.on('user-questions/request', function (request, next) {
      asked.push('mcp')
      return Promise.resolve(answerOf('mcp-claimed'))
    }, { prepend: true })
    const answer = await service.ask({ questions: [{ id: 'q1', question: 'Which DB?' }] })
    check(scope, "真宿主 ask() 派发 'user-questions/request' 到 prepend listener", asked[0] === 'mcp')
    check(scope, 'prepend 认领后不调 next() → GUI listener 不被调用(veto)', asked.length === 1 && asked[0] === 'mcp')
    check(scope, 'ask() resolve 值即 listener 返回值(0.1.5 answer 形态)', answer?.answers?.[0]?.id === 'q1'
      && answer?.answers?.[0]?.custom === 'mcp-claimed' && Array.isArray(answer?.answers?.[0]?.selected))
  }

  // ── B. 降级: prepend listener 决定不接管 → return next() 透传给 GUI 应答者 ──
  {
    const ctx = new Context()
    const service = new UserQuestionService(ctx)
    const asked = []
    ctx.on('user-questions/request', function (request, next) { asked.push('gui'); return guiListener() })
    ctx.on('user-questions/request', function (request, next) { asked.push('delegate'); return next() }, { prepend: true })
    const answer = await service.ask({ questions: [{ id: 'q1', question: 'GUI only?' }] })
    check(scope, 'return next() 透传: 后续 GUI 应答者接管', asked.join(',') === 'delegate,gui')
    check(scope, '透传后 ask() 得到 GUI 的答案', answer?.answers?.[0]?.custom === 'gui-claimed')
  }

  // ── C. 无应答者认领 → ask() 拒绝 NO_PROVIDER(与插件「不接管」时宿主的行为一致) ──
  {
    const bare = new UserQuestionService(new Context())
    let err
    try {
      await bare.ask({ questions: [{ id: 'q1', question: 'nobody?' }] })
    } catch (e) { err = e }
    check(scope, '无应答者认领 → ask() 以 NO_PROVIDER 拒绝', err?.code === 'NO_PROVIDER')
  }

  // ── D. listener 拒绝(abort) → ask() 抛出插件构造的错误(signal 契约不变) ──
  {
    const ctx = new Context()
    const service = new UserQuestionService(ctx)
    const ac = new AbortController()
    ctx.on('user-questions/request', (request) => new Promise((_resolve, reject) => {
      request.signal?.addEventListener('abort', () => reject(new Error('ask_user_question was aborted before the user answered')), { once: true })
    }), { prepend: true })
    const p = service.ask({ questions: [{ id: 'q1', question: 'hang?' }], signal: ac.signal })
    ac.abort()
    let err
    try { await p } catch (e) { err = e }
    check(scope, 'listener 拒绝(abort)时 ask() 抛出插件构造的错误', String(err?.message ?? '').includes('aborted before the user answered'))
  }
}

const installs = findInstalls()
const modern = installs.filter((i) => i.version.startsWith('0.1.5') || Number(i.version.split('.')[1] ?? 0) > 1)
const older = installs.filter((i) => !modern.includes(i))
notes.push(`installs found: ${installs.map((i) => `${i.version}@${i.source}`).join(', ') || '(none)'}`)
if (older.length > 0) notes.push(`  (pre-0.1.5 hosts, which take the legacy registerProvider path: ${older.map((i) => `${i.version}@${i.source}`).join(', ')})`)

const byVersion = new Map()
for (const c of findStoreCopies()) {
  const has = readFileSync(c.entry, 'utf8').includes('registerProvider')
  byVersion.set(c.version, (byVersion.get(c.version) ?? true) && has)
}
notes.push(`pnpm store copies: ${[...byVersion.entries()].map(([v, has]) => `${v}${has ? ' (has registerProvider)' : ' (no registerProvider)'}`).join(', ') || '(none)'}`)
check('', 'pnpm store: ≤0.1.1 旧版副本仍有 registerProvider / 0.1.5 已删除',
  [...byVersion.entries()].every(([v, has]) => (v.startsWith('0.1.5') ? !has : has)))

if (modern.length === 0) {
  for (const n of notes) console.log(`  · ${n}`)
  console.log('SKIP: no dsh-user-questions >= 0.1.5 install found')
  process.exit(0)
}
for (const host of modern) await verifyInstall(host)

for (const n of notes) console.log(`  · ${n}`)
const failed = results.filter(([, ok]) => !ok)
for (const [name, ok] of results) console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name.trim()}`)
console.log(failed.length === 0 ? 'HOST VERIFY PASS' : `HOST VERIFY FAIL (${failed.length} 项)`)
process.exit(failed.length === 0 ? 0 : 1)
