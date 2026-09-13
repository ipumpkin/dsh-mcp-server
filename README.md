# dsh-harness-mcp-server

> Expose [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) agent capabilities as an **MCP server**, letting any MCP client (e.g. [Hermes](https://hermes-agent.nousresearch.com/)) drive Harness to execute real coding tasks.

**Hermes is the brain (pro), Harness is the arms (flash) — 1+1>2.**

[![npm version](https://img.shields.io/npm/v/@chushixixin/dsh-harness-mcp-server)](https://www.npmjs.com/package/@chushixixin/dsh-harness-mcp-server)
[![license](https://img.shields.io/npm/l/@chushixixin/dsh-harness-mcp-server)](./LICENSE)

## Why this exists

Harness ships a powerful agent runtime (tools, LLM, agents, sessions), but it is a **Cordis app**, not something another agent can call. This plugin turns Harness inside-out: it starts a real **MCP server** (StreamableHTTP) *inside* Harness and bridges Harness's core services — `ctx.agents`, `ctx.agentPresets`, `ctx.tools` — so an external "brain" can delegate real work to Harness's "arms".

Since **0.11.0** dispatch is **session + turn**, not a task queue: `session_send` feeds one task as a turn into a session and **returns immediately** — there is no task id, no server-side queue, no TTL, and no synchronous waiting. The client then drives the loop actively: `session_status` for the authoritative phase and structured summary, `session_tail` for process detail, `session_wait` if it wants to block for one short segment. The **session log is the single source of truth**, so state survives a process restart.

```
Hermes (MCP client, brain)
   │  session_send → { sessionId, status: "accepted" }   (HTTP, returns at once)
   ▼
dsh-harness-mcp-server (MCP server, :8090)
   │  ctx.agents.create → mount 'standard' preset → agent.followup() one turn
   ▼
Harness agent (flash) — full toolset: bash, fs, todo, web…
   │  stream of events… turn/start … turn/end (session log)
   ▲
Hermes polls actively: session_status / session_tail (optionally session_wait)
```

## Tools

> **0.11.0 breaking change**: the old task layer was removed with no shim — `agent_run`, `task_inbox`, `task_result`, `task_list`, `task_wait` and `task_cancel` no longer exist. There is no `taskId`, no in-memory queue, no TTL and no queue capacity; dispatch is `session_send` plus the `session_*` tools below.

| Tool | Direction | Purpose |
|------|-----------|---------|
| `echo` | — | Verify MCP connectivity |
| `harness_list_tools` | — | List Harness's registered tool names |
| `harness_status` | Hermes ← Harness | Operations overview: `agentPool` (size / cap / live agents), `mcpInFlightTurns` (MCP-driven turns per session) and the runtime `config` |
| `model_list` | Hermes ← Harness | List every registered provider's models (plus declared-but-inactive providers); `withWindow` adds context windows |
| `mode_list` | Hermes ← Harness | List session **modes**: agent presets (`standard`/`code`/`cordis`/`minimal`…), sandbox access modes (`read-only`/`workspace-write`/`danger-full-access`), approval policies (`ask`/`never`), and named permission presets (bundles, e.g. `workspace-write` = workspace-write + ask); `modes` gives the canonical ids you can pass to `session_send` `mode=` |
| `workspace_list` | Hermes ← Harness | List workspaces and their session groups |
| `session_send` | Hermes → Harness | **The one dispatch entry point**: feed a task as one **turn** into a session and **return immediately**. Params: `sessionId` *or* `cwd` (one required — `sessionId` continues a session, `cwd` reuses/creates that directory's pooled session), `message`, `context?`, `newSession?`, `title?`, `model?`, `provider?`, `preset?`, `mode?`, `sandbox?`, `approval?`. The message template is memory `context` + 【任务】 + "you must end with a one-line summary JSON". Returns `{sessionId, cwd?, status: "accepted", inboxDepth, openTurn, hint}` — it never waits, never times out and never blocks the MCP client |
| `session_status` | Hermes ← Harness | **Authoritative query** (session log = single source of truth): `live`, `source`, `cwd?`, `title?`, `agentStatus?`, `phase` (`idle`/`running`/`waiting_input`/`interrupted`), `openTurn{turn,startedSeq}`, `lastTurn{turn,reason{kind,error?}}`, `prompts[]`, `context` (events/tokens/pressure/window/ratio; live only, `null` when cold), `logEvents`, `changes`/`verification`/`leftovers` (parsed from the assistant text inside the last turn boundary; `null` when not extractable), `lastText`, `note?`. A trailing `turn/start` with no matching `turn/end` reports `phase: "interrupted"` — the owning process is gone, it is **not** `running` |
| `session_tail` | Hermes ← Harness | **On-demand process detail**: only **surface events** — `user`/`assistant` message text, `tool/call` and `tool/result` summaries, `turn/start`/`turn/end` boundaries. Internal streaming events are filtered out and empty assistant messages are skipped (no more "(no text blocks)" noise). Params: `n` (default 5), `sinceSeq?`, `filter?: 'all'\|'surface'\|'tools'`; returns `{seq, type, text (≤2k), turn?}` rows |
| `session_wait` | Hermes ← Harness | **Optional blocking, one segment ≤ 240s**: `until?: 'turn-end'\|'idle'\|'input'` (default `turn-end`), `timeoutMs?` (default 60000, max 240000), `sinceTurn?`. Event-driven with a 500ms fallback poll; on timeout returns `{timeout: true, status}`, when settled `{timeout: false, status}`, and for a non-live session it returns immediately with `{live: false, status}` |
| `session_cancel` | Hermes → Harness | Interrupt the session's current turn (`agent.cancel({kind:'hook', reason:'harness-mcp-cancel'})`); the turn closes with `turn/end` `reason.kind='aborted'`. `keepInbox: true` keeps queued-but-unstarted input (default clears it). A non-live (post-restart) session returns a no-op — read it with `session_status` instead |
| `session_list` | Hermes ← Harness | List resumable sessions (pool / live / persisted) with **context usage** (events / tokens / window / ratio) |
| `session_read` | Hermes ← Harness | Read a session's event stream (text / tool calls / results) for audit or pre-resume review |
| `session_compact` | Hermes → Harness | Compress a session's early history into a model summary (needs a compaction backend, e.g. `dsh-compaction-basic`) |
| `attach_session` | Hermes → Harness | Attach a session to the workspace matching its cwd (respects `workspaceRoots` whitelist) |
| `rename_session` | Hermes → Harness | Rename a session for the session list |

`session_send` returns as soon as the turn is queued; the structured result now comes from `session_status` (the session log is the single source of truth, so this works both for a live session and for one cold-read after a restart):

```json
{
  "sessionId": "...",
  "live": true,
  "source": "live",
  "cwd": "/workspace/project",
  "title": "fix the failing test suite",
  "agentStatus": "idle",
  "phase": "idle",
  "openTurn": null,
  "lastTurn": { "turn": 3, "reason": { "kind": "completed" } },
  "prompts": [],
  "context": { "events": 142, "tokens": 18340, "pressure": 21200, "window": 128000, "ratio": 14.3 },
  "logEvents": 268,
  "changes": "what was changed",
  "verification": "how it was verified",
  "leftovers": "open issues",
  "lastText": "final answer"
}
```

`changes`/`verification`/`leftovers` are extracted with `parseSummary` from the assistant text inside the **last turn boundary**, and are `null` when the agent did not emit the required one-line summary JSON. `openTurn` is non-null exactly while a turn is in flight; `prompts[]` lists approvals/questions waiting for `prompt_respond` (and makes `phase` `waiting_input`). Process detail (message text, tool calls and results) comes from `session_tail`, not from `session_status`. The session's effective mode (preset + sandbox + approval + matching permission-preset name) is reported by `session_list`'s `mode` field.

### Session modes

A DSH session's **mode** is three independent knobs, plus named bundles over them:

| Category | Values | Source |
|----------|--------|--------|
| Agent preset | `standard` (标准模式), `code` (PTC 模式), `cordis` (创造模式), `minimal` (极简模式), … | `ctx.agentPresets` (dsh agent-presets; mounted in setup, recorded in the session header's `agentPreset`) |
| Sandbox access mode | `read-only` / `workspace-write` / `danger-full-access` | per-session override = `sandbox/mode` session-log event (`ctx.sandboxPolicy` default) |
| Approval policy | `ask` / `never` | per-session override = `approval/policy` session-log event (`ctx.approval` default) |
| Permission preset (bundle) | e.g. `workspace-write` = workspace-write + ask, `danger-full-access` = danger-full-access + never | `ctx.permissionPresets` table |

`mode_list` enumerates all of it — with `only: "preset" | "sandbox" | "approval" | "permission"` to filter and `withDetail: true` for extra metadata — and its `modes` array is the canonical id space accepted by `session_send` `mode=`. Creating a session with `mode`/`preset`/`sandbox`/`approval` applies the mode at creation time (forcing a fresh session), so the session runs under it from the first turn and avoids mid-turn privilege escalation; `session_list`'s `mode` snapshot verifies what took effect.

This closes the loop between the client's persistent memory and Harness's coding: memory is fed into each turn as `context`, and `session_status`'s `changes` / `verification` / `leftovers` can be persisted back to the client's memory for the next run.

### Dispatch, waiting & cancel

Dispatch never blocks the MCP client, so there is nothing to time out on the send path:

- **`session_send`** puts the message into the session and returns `{sessionId, status: "accepted", inboxDepth, openTurn}` at once. It never waits, never times out, and has no `timeoutMs` parameter.
- **Poll** with `session_status` — cheap, always available and authoritative: `phase: "running"` while a turn is in flight, `"idle"` once it settled, `"waiting_input"` while an approval/question is pending, and `"interrupted"` when a trailing `turn/start` has no matching `turn/end` (the owning process is gone — never reported as `running`).
- **Or block one short segment** with `session_wait` (`until: 'turn-end' | 'idle' | 'input'`, `timeoutMs` default 60000, max 240000). On timeout it returns `{timeout: true, status}` and you simply poll again — there is no server-side task that can be lost.
- **`session_cancel`** interrupts the current turn at any time (idempotent once the session is idle): `agent.cancel({kind:'hook', reason:'harness-mcp-cancel'})` makes the turn close with `turn/end` `reason.kind='aborted'`; pass `keepInbox: true` to keep queued-but-unstarted input. A non-live session (after a restart) is a no-op — nothing is running.
- **`taskTimeoutMs`** no longer bounds dispatch. It still exists as the cap on maintenance operations such as `session_compact` (default 60 min, `0` disables).

### Session reuse — the caller decides

Sessions are **reused per cwd by default** (one resident pool session per cwd, LRU-capped by `maxAgents`): each new turn appends to the same conversation, avoiding project-context reloads. The flip side is that the conversation history grows with every turn — token cost per call rises, and unrelated tasks share one context. The caller controls this explicitly:

| Mechanism | Behavior |
|-----------|----------|
| `session_send` `sessionId: ...` | Continue that exact session (multi-round feeding / resume after interrupt) |
| `session_send` `newSession: true` | Force a brand-new session for this call; the old pooled session is retired (disposed) but stays persisted and resumable by its `sessionId` |
| `session_send` `model` / `provider` | Per-call model override (applies to new / resumed sessions; a pooled-reuse session keeps its original model) |
| `session_send` `mode` / `preset` / `sandbox` / `approval` | Create the session **in a given mode**: `preset` mounts an agent preset (`standard`/`code`/`cordis`/`minimal`…, recorded in the session header's `agentPreset`); `mode` accepts any `mode_list.modes` id — a permission-preset name (bundle, e.g. `workspace-write` = workspace-write + ask), a sandbox mode, an approval policy, or a preset id; `sandbox`/`approval` override the bundle explicitly. Specifying any of these **forces a brand-new session** (a pooled session cannot safely adopt a new mode), and the sandbox/approval knobs are written as durable `sandbox/mode` / `approval/policy` session-log events so the session runs under that mode from its first turn — no mid-turn privilege escalation. `session_list` carries a `mode` snapshot verifying what took effect. `sandbox`/`approval` are rejected when resuming an existing `sessionId`; `preset` alone is allowed on resume (mounted in setup) |
| New session `title` (optional) | Name the new session (via the `sessionTitle` service's `rename`); **when omitted, a readable name is derived from the task text** (first sentence, ≤ 60 chars, same `rename` path) — every new session has a name out of the box, `session_status` and `session_list` show the `title`, and reusing a session never renames it |
| *(neither)* | Default: reuse the cwd's pooled session |
| `session_list` | See pool / live / persisted sessions (id, cwd, source, title, context usage) to decide what to continue |
| `session_read` | Read a session's transcript (text, tool calls, results) before deciding to continue it |

So the reuse policy is fully external: want cheap repeated tasks in one context — reuse (default); want clean isolation for a big refactor — pass `newSession: true`; want to continue a long task across calls — pass its `sessionId`. There is no hidden auto-rotation that could surprise you.

**Busy-session protection**: LRU eviction and `newSession` never dispose a session that is currently running a turn (`agent.status === 'idle'` guard). A busy session is either left out of eviction (the pool soft-caps and catches up later) or rejected with a `busy` note — running work is never killed behind your back.

### Client contract

- `sessionId` **or** `cwd` is **required** on `session_send` — `sessionId` continues that exact session, `cwd` reuses/creates the pooled session for that directory. Requiring one prevents accidental work in the dsh process directory (`workspace_list` shows available roots).
- `session_send` is fire-and-accept, not run-and-wait: it returns `{sessionId, status: "accepted", inboxDepth, openTurn}` immediately. **There is no `taskId`**, nothing is queued server-side, and there is no dispatch timeout — a long task can never drop your MCP connection.
- The client owns the loop: `session_status` to see whether it finished, `session_tail` for the detail behind it. Use `session_wait` only if you would rather spend one round trip blocking than polling; it blocks at most one 240s segment and then returns `{timeout: true, status}`.
- `phase` is derived from the session log, not from a server-side status field: `running` (open turn), `idle` (last turn closed), `waiting_input` (an approval/question is pending — `prompts[]` carries the ids for `prompt_respond`), `interrupted` (a `turn/start` never got its `turn/end`; the process is gone, so nothing is running).
- **Structured result**: `changes` / `verification` / `leftovers` live on `session_status`, parsed from the last turn's assistant text. The send template asks the agent to end with one line of JSON, e.g. `{"changes":"…","verification":"…","leftovers":"…"}`, which is exactly what `parseSummary` looks for; if the agent does not comply, the fields come back `null`.
- **Process detail**: `session_tail` returns only surface events — message text, `tool/call` and `tool/result` summaries, `turn/start`/`turn/end` boundaries — so `n` counts what you actually want to read. Pass the last `seq` you have seen as `sinceSeq` to follow a running turn incrementally.
- **Cold sessions count too**: after a restart nothing is live, but `session_status`/`session_tail` cold-read the persisted session log, so you can still see the last turn, its summary and its events. `context` is only measurable on live sessions (`null` when cold).
- Errors are returned as `{ "error": ... }` JSON with the MCP `isError` flag set, so clients can distinguish failures from successful calls.

### Approval/question takeover & web UI notices

When MCP intercepts an approval or a question, the web session view shows two collapsed notice rows (`user/message` with `source.form: 'notice'`: taken-over + responded). **Notices use a safe-placement mechanism**: at interception time (`approval/request` / the question provider) the hint is only queued — never written to the session log directly; once that tool finishes, a `tools/post-execute` listener merges the queued hints into the tool result's `additionalContexts`, and the agent-loop appends them as `user/message` events **after the `tool/result` and before the next model request** (the same mechanism `dsh-repeat-tool-reminder` / `dsh-tool-goal` use).

- The rows render through **DSH's native notice presentation**: `source.form: 'notice'` + `summary` survive the `additionalContexts` path intact (the web client's `contextForm` whitelist includes `notice`, so the collapsed row shows the summary and the expanded body shows the text) — exactly like the official plugins. The copy is written as system/status messages (`⏳` taken over / `✅` responded / `❌` failed, with explicit "approval/question taken over / answered by MCP" wording). The collapsed-row header "Context injection" is the web UI's fixed label for every non-recall context row (not changeable from the plugin), but the notice content itself reads as a notice/system prompt, not a low-level call.

- This guarantees a notice can never land between an assistant message carrying `tool_calls` and its `tool/result` — the old behavior (since 0.9.4) appended the `user/message` right at interception, and when that fell between the two, the next model request failed with `An assistant message with tool_calls must be followed by tool messages responding to each tool_call_id` (INVALID_REQUEST), corrupting the session.
- **Sessions already corrupted** by the old code cannot be repaired in place: just **start a new session** — `session_send` with `cwd` and no `sessionId` opens/reuses a fresh pooled session, or pass `newSession: true` for a guaranteed-fresh one; the old session can simply be ignored (the pool retires it by LRU once it is idle), and no other session is affected. This plugin never rewrites historical logs.

### Context usage & compaction

Because sessions grow with reuse, `session_list` and `session_status` report **context occupancy** for every session that is live:

```json
"context": { "events": 142, "tokens": 18340, "pressure": 21200, "window": 128000, "ratio": 14.3 }
```

- `tokens` = current surface tokens, `pressure` = last request+response pressure, both measured through `ctx.tokenMeter.measure(session)` (the same fixed-density heuristic pricing dsh uses).
- `window` = the model's context window, resolved via `ctx.llm.resolveModel(agent.options.provider, agent.options.model)` (cached per `provider:model`; falls back to the `agentDefaultModel` selection, then plugin config).
- `ratio` = `tokens / window` as a percentage (1 decimal) — the number to watch before deciding to compact or start a fresh session.
- Persisted-only rows (log not loaded), sessions whose model window can't be resolved, and environments without the `tokenMeter` service report `"context": null` (or `window`/`ratio` null when only the window is unknown).

When a session's context gets too large, compress it:

- **`session_compact`** (e.g. `{ "sessionId": "..." }`) runs the host's `ctx.compaction.compactNow`: it picks a useful early range, replaces it with one model-generated summary node, and returns `compactionId`, `shadowedNodes`, `shadowedTokenCount`, and the `before`/`after` context occupancy.
- Requires a compaction backend loaded in the host (shipped `dsh-compaction-basic` comes with `dsh-base`); otherwise it returns an explicit error.
- A busy session (currently running a turn) returns a `busy`-classified error; a persisted session is temporarily resumed, compacted, then flushed and released.

## Install

**Requirement: dsh ≥ 0.1.1-rc.2** (this release aligns the peer dependency ranges with the current Harness API; the old rc.6 workaround is no longer needed).

> The `@deepseek-ai/*` packages are declared as **peerDependencies**, so the plugin shares the host's single copies (Cordis services are identity-based; duplicate installs would break `inject`). Install it with the same package manager as your profile workspace.

### Option A — profile bundle (recommended)

```bash
cd ~/.dsh/profiles/web      # or whichever profile you run (dsh-tui / headless / web)
pnpm add @chushixixin/dsh-harness-mcp-server
```

Then add it to the profile's bundle list in `~/.dsh/profiles/web/package.json`:

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

The package ships a `dsh.bundle.patch` manifest (`cordis.yml`), so it mounts automatically at next boot. No `--patch` needed.

### Option B — `--patch` overlay

```bash
dsh web --patch ~/.dsh/profiles/web/node_modules/@chushixixin/dsh-harness-mcp-server/cordis.yml
```

(Repeatable: `--patch a.yml --patch b.yml`.)

### Option C — from source (legacy dsh checkout layout)

Clone this repo inside your Harness workspace under `packages/mcp/harness-mcp-server/` (the pnpm workspace matches `packages/*/*`, two levels deep), then build with `pnpm run build` (standalone `tsc`, no tsdown needed). The browser half (`client.js`) is a hand-written lazy-CJS factory file at the repo root and needs no build step.

## Run

```bash
export DEEPSEEK_API_KEY=...
dsh web      # bundle install (Option A) or --patch (Option B) takes effect on boot
```

The MCP server listens on `127.0.0.1:8090` (StreamableHTTP). Point any MCP client at `http://127.0.0.1:8090/mcp`.

> ⚠️ **Security**: by default the server binds to `127.0.0.1` with **no authentication**, and it exposes **unauthenticated remote code execution** — do **not** bind it to `0.0.0.0` or expose it to the internet/LAN without enabling a token first (plus TLS and a reverse proxy for public exposure). Binding a non-loopback address with no token configured logs a loud warning at startup. A token can be enabled any time from the **MCP Server** settings page (web GUI) or via `authToken`/`authTokens` config.

### Hermes client config

```bash
printf 'n\nY\n' | hermes mcp add harness_plugin --url http://127.0.0.1:8090/mcp
```

When a token is enabled, the client must send `Authorization: Bearer <token>` on every request (configure the header in your MCP client; e.g. Hermes supports custom headers for streamable-HTTP servers).

## Config

Two layers, resolved in order (schema default → entry-config base → user layer):

- **Web settings page** (recommended): **Settings → MCP Server** in the dsh web GUI. Edits `host` / `port` / `authToken` at runtime — staged form, per-field reset back to the entry-config value, revision-fenced writes. Saving hot-applies: the listener rebinds without dropping established MCP sessions.
- **Entry config** (cordis.yml bundle manifest or `--patch` overlay): full surface, including entry-only keys (`provider`, `preset`, `maxAgents`, `authTokens`, …). All optional:

| Key | Default | Meaning |
|-----|---------|---------|
| `http` | `true` | Serve over HTTP (StreamableHTTP) |
| `port` | `8090` | Listen port |
| `host` | `127.0.0.1` | Bind address (exposing requires auth, see below) |
| `provider` | `deepseek-official` | Backend provider for created agents |
| `model` | *(dsh default)* | Model override for created agents (empty = follow dsh settings) |
| `preset` | `standard` | Agent preset mounted in `setup` (recorded in `meta.agentPreset`) |
| `maxAgents` | `8` | Resident agent pool cap (LRU eviction) |
| `taskTimeoutMs` | `3600000` | Timeout for maintenance operations such as `session_compact` (60 min, `0` disables). Dispatch via `session_send` never times out |
| `authToken` | *(none)* | Bearer token — when set, every request must send `Authorization: Bearer <token>`; also editable from the settings page (show/hide + copy) |
| `authTokens` | *(none)* | Additional Bearer tokens (array; any match authorizes) — coexists with `authToken`, handy to give each client its own token |
| `workspaceRoots` | *(none)* | cwd whitelist — when set, tasks may only run under these directories (`attach_session` targets are validated against it too) |

> The settings page needs the dsh **web** surface: it writes through the web settings document and ships as a browser bundle. On surfaces without the settings service (e.g. headless) the host half works unchanged — only the page is absent. If the host row runs but the page never appears in the web settings nav, the client bundle could not be resolved by the deployment: link the installed package into the host installation's `node_modules` (or `~/.dsh/profiles/node_modules`), e.g. `ln -s ~/.dsh/profiles/web/node_modules/@chushixixin/dsh-harness-mcp-server <host-install>/node_modules/@chushixixin/dsh-harness-mcp-server`, then restart.

### cordis.yml (patch format)

```yaml
- insert:
    - id: harness-mcp-server
      name: '@chushixixin/dsh-harness-mcp-server'
      config:
        http: true
        port: 8090
        host: 127.0.0.1        # 默认仅本机; 暴露前必须加认证
        taskTimeoutMs: 3600000
        # authToken: 'your-secret-token'     # 可选: Bearer token 认证(也可在 web 设置页管理)
        # authTokens: ['token-a', 'token-b']  # 可选: 多 token, 任一命中放行
        # workspaceRoots: ['/workspace']      # 可选: cwd 白名单
```

## Positioning

This is best used as a **fallback tool**, not a daily driver: for everyday code edits, drive your primary agent directly. Reach for this when you need **context isolation** (huge refactors that would blow the client's context) or **parallel execution** of unrelated tasks.

- The agent session is **reused per cwd** (avoids re-loading project context on every call — roughly 15–20× cheaper than one-shot `dsh headless`).
- Bash runs sandboxed (`workspace-write`): install `bubblewrap` on the host, or the sandbox will refuse write commands.
- Each new MCP session gets its own `McpServer` + transport (an MCP `McpServer` connects to a single transport).
- Resumed sessions are attached to their cwd workspace (`realpath`-normalized) on startup (orphan reattach) and on first use; `attach_session` is the manual backfill for UI-created sessions.

## License

MIT
