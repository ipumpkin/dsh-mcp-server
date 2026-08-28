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
 *   - pending_prompts     : 列出等待输入的弹窗(审批/提问)——MCP 调用方对 DSH 弹窗不再盲目
 *   - prompt_respond      : 响应弹窗(审批 approve/deny, 提问自由文本), 解除 agent 阻塞继续
 *   - session_set_model   : 给指定会话切换模型(改 agent.options.model, 下个 turn 生效)
 *   - session_inject      : 向指定会话的 agent 队列插入补充指令(steering), 不打断当前工具执行
 *   - attach_session      : 把会话归组到其 cwd 对应的工作区(手动补给站)
 *   - rename_session      : 给已有会话改名
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
import { z } from 'zod';
import { boundContextSummary, createUserMessage } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { scopeOf } from '@deepseek-ai/dsh-scope';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { randomUUID } from 'node:crypto';
import { realpath } from 'node:fs/promises';
import http from 'node:http';
import { resolve } from 'node:path';
/** Cordis 插件名 */
export const name = 'harness-mcp-server';
/** 插件版本(与 package.json 同步; MCP initialize 时上报) */
export const VERSION = "0.9.6";
/**
 * 声明依赖的核心服务。
 * workspaceRegistry/sessionPersistence/sessions 是续接/归组三个增量用到的服务——
 * 漏声明会在真实启动时拿不到服务(本插件曾经踩过, 务必与代码里的 ctx.get 对齐)。
 */
export const inject = ['tools', 'llm', 'agents', 'agentPresets', 'workspaceRegistry', 'sessionPersistence', 'sessions'];
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
    workspaceRoots: [],
};
/** 工具回调统一返回 MCP text content */
function out(content) {
    return { content: [{ type: 'text', text: content }] };
}
/** 错误响应: 结构化 JSON 文本 + isError 标记(MCP 客户端可据此识别失败, 不写回记忆) */
function err(content) {
    return { content: [{ type: 'text', text: content }], isError: true };
}
/**
 * cwd realpath 规范化: 解析符号链接与 .. 段, 使 cwd 能与 workspace.path(存储时为 realpath 规范化值)
 * 精确比对——这是官方 attachSession 强校验通过的前提。目录不存在时回退 resolve 结果, 由调用方告警不阻断。
 */
async function canonicalCwd(raw) {
    try {
        return await realpath(raw);
    }
    catch {
        return resolve(raw);
    }
}
/** 官方 session.create RPC 同款姿势: resolveByPath ?? create, 幂等; 无 workspaceRegistry 时返回 undefined */
async function ensureWorkspace(ctx, canonical) {
    const registry = ctx.get('workspaceRegistry');
    if (!registry)
        return undefined;
    return (await registry.resolveByPath?.(canonical)) ?? (await registry.create?.(canonical));
}
/** 把会话挂名到其 cwd 对应的工作区。attachSession 内部强校验 realpath(header.cwd) 精确等于 workspace.path,
 *  所以 canonical 必须是 header.cwd 的 realpath 规范化值。失败告警不阻断任务(分组是锦上添花)。 */
async function attachToWorkspace(ctx, canonical, sessionId) {
    try {
        const ws = await ensureWorkspace(ctx, canonical);
        if (ws?.attachSession)
            await ws.attachSession(sessionId);
    }
    catch (e) {
        console.warn('[harness-mcp-server] workspace attach failed:', e?.message ?? e);
    }
}
/** 按会话 header 的 cwd(realpath 规范化后)补挂工作区; header 无 cwd 时静默跳过 */
async function attachSessionCwd(ctx, sessionId, cwd) {
    if (cwd === undefined)
        return;
    await attachToWorkspace(ctx, await canonicalCwd(cwd), sessionId);
}
/** 常驻 agent 会话(按 cwd 复用, 省 token: 避免每次全量加载项目上下文) */
const liveAgents = new Map();
/** sessionId → cwd 索引(支持按 session 续接: 指定 sessionId 时定位到对应 cwd 的常驻会话) */
const sessionToCwd = new Map();
/** 每个 cwd 的串行执行锁(防同一 agent 会话被并发 followup 冲突) */
const agentLocks = new Map();
/** 获取(或创建)指定 cwd 的常驻 agent 会话; 传 sessionId 时接管指定会话; 传 title 时给新会话命名。
 *  fresh=true 且未传 sessionId 时: 跳过池命中, 先退役该 cwd 的旧池会话(dispose, 持久化保留), 再新建 ——
 *  这是外部客户端显式控制「是否复用会话」的入口(agent_run/task_inbox 的 newSession 参数)。
 *  modelOpts 提供按次调用的 provider/model 覆盖(只对新建/resume 的会话生效; 池命中的复用会话保持原模型)。 */
async function getAgent(ctx, cwd, sessionId, title, fresh, modelOpts) {
    // 恒解析生效模型(显式覆盖 → 插件配置 → agentDefaultModel): 预设 persona 引用 {{model}},
    // agent.options.model 缺失会让 prompt 组装抛 "has no value for this assembly" 并空跑本轮。
    // 指定 sessionId 时优先采用 session_set_model 记录的会话级覆盖(resume 后依然生效)。
    const sessionOverride = sessionId !== undefined ? sessionModelOverrides.get(sessionId) : undefined;
    const agentOptions = sessionOverride
        ? { provider: sessionOverride.provider ?? modelOpts?.provider ?? runtimeConfig.provider, model: sessionOverride.model }
        : resolveAgentModel(ctx, modelOpts);
    // 指定 sessionId: 接管已有会话(长任务分多轮投喂 / 中断后恢复 / UI 手开的会话)
    if (sessionId) {
        // 先看本进程常驻池(指定 sessionId 时定位到对应 cwd 的常驻会话; 命中 LRU 移到末尾, 保留上游语义)
        const targetCwd = sessionToCwd.get(sessionId);
        if (targetCwd !== undefined) {
            const existing = liveAgents.get(targetCwd);
            if (existing) {
                liveAgents.delete(targetCwd);
                liveAgents.set(targetCwd, existing);
                mcpSessionIds.add(sessionId);
                return existing;
            }
        }
        const sid = SessionId(sessionId);
        // 不在常驻池: 看 live(UI 手开的、别的插件持有的会话), 直接接管、不持有 dispose(归其 owner)
        const live = ctx.agents.get(sid);
        if (live) {
            // live 会话也补挂工作区(幂等): 用户手开的会话若尚未归组, 这里一并挂名
            await attachSessionCwd(ctx, sid, live.session.header.cwd);
            mcpSessionIds.add(sessionId); // 被 MCP 接管即视为 MCP 驱动(审批转达调用方)
            // no-op dispose 兜底: executeTask 只在 disposeAfter 为 true 时调用 dispose
            return { sessionId: sid, handle: { agent: live, dispose: () => Promise.resolve() }, disposeAfter: false };
        }
        // live 也没有: 从持久化会话存储 resume 并接管(进程重启前的会话、LRU 淘汰后被释放的会话)
        let handle;
        try {
            handle = await ctx.agents.resume({
                resumeSessionId: sid,
                agentOptions,
                setup: async (agentCtx) => {
                    // dsh 0.1.1-rc.2 起已修复 rc.6 的 agent ctx 丢 scope 问题(agent-loop 会 createScope);
                    // 保留检测以兼容更旧版本: 无 scope 时跳过挂载(降级为无工具 agent), 不让 resume 整体崩溃。
                    if (scopeOf(agentCtx) === undefined) {
                        console.warn('[harness-mcp-server] agent ctx unscoped (old dsh bug); preset mount skipped — upgrade dsh >= 0.1.1-rc.2 for full tool support');
                        return;
                    }
                    await ctx.agentPresets.mount(agentCtx, runtimeConfig.preset);
                },
            });
        }
        catch (e) {
            // 恢复失败返回明确错误(沿用上游错误风格): 不在常驻池、不是 live、持久化里也没有(或 resume 失败)
            throw new Error(`session not found for resume: ${sessionId} (not live and not persisted; ${e?.message ?? e})`);
        }
        await attachSessionCwd(ctx, sid, handle.agent.session.header.cwd);
        mcpSessionIds.add(sessionId);
        return { sessionId: sid, handle, disposeAfter: true };
    }
    // 显式全新会话: 跳过池命中 —— 先退役该 cwd 的旧池会话(保留持久化, 可凭 sessionId 续接)。
    // 旧会话正在跑任务时不 dispose(不掐任务), 仅从池摘除; 其任务结束后 agent 仍 live, 可凭 sessionId 接管或 session_close。
    if (fresh) {
        const old = liveAgents.get(cwd);
        if (old) {
            liveAgents.delete(cwd);
            sessionToCwd.delete(String(old.sessionId));
            const status = old.handle.agent.status;
            if (status === 'idle') {
                try {
                    await old.handle.dispose();
                }
                catch { /* 退役失败不阻断新建 */ }
            }
        }
        return createPoolAgent(ctx, cwd, title, agentOptions);
    }
    const existing = liveAgents.get(cwd);
    if (existing) {
        // LRU: 命中则移到末尾(最近使用)
        liveAgents.delete(cwd);
        liveAgents.set(cwd, existing);
        // 自愈: 幂等补挂(已在花名册则 no-op; 首次挂名失败的池会话在此被捞回)
        await attachToWorkspace(ctx, await canonicalCwd(cwd), existing.sessionId);
        return existing;
    }
    return createPoolAgent(ctx, cwd, title, agentOptions);
}
/** 新建一个 cwd 的常驻池会话: LRU 淘汰(只淘汰 idle 的) → agents.create(挂 preset) → 入池 → 工作区分组 → 可选命名 */
async function createPoolAgent(ctx, cwd, title, agentOptions) {
    // LRU 淘汰: 超过上限时逐出最久未用的会话 —— 只淘汰 idle 的(agent.status === 'idle');
    // 最旧一批都在忙时**不掐任务**, 允许池暂时超上限(软上限), 等任务落定后由下次淘汰回收。
    while (liveAgents.size >= runtimeConfig.maxAgents) {
        let victimKey;
        for (const [key, rec] of liveAgents) {
            const status = rec.handle.agent.status;
            if (status === 'idle') {
                victimKey = key;
                break;
            }
        }
        if (victimKey === undefined)
            break;
        const old = liveAgents.get(victimKey);
        liveAgents.delete(victimKey);
        if (old) {
            sessionToCwd.delete(String(old.sessionId));
            try {
                await old.handle.dispose();
            }
            catch { /* 忽略 */ }
        }
    }
    const newSessionId = SessionId(randomUUID());
    // cwd 先 realpath 规范化: session header 的 cwd 与 workspace.path 必须精确相等,
    // 否则 attachSession 强校验 reject(只会 create 注册而 UI 仍落未分组)
    const canonical = await canonicalCwd(cwd);
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
                console.warn('[harness-mcp-server] agent ctx unscoped (old dsh bug); preset mount skipped — upgrade dsh >= 0.1.1-rc.2 for full tool support');
                return;
            }
            await ctx.agentPresets.mount(agentCtx, runtimeConfig.preset);
        },
    });
    const rec = { sessionId: newSessionId, handle };
    liveAgents.set(cwd, rec);
    sessionToCwd.set(String(newSessionId), cwd);
    mcpSessionIds.add(String(newSessionId));
    // 分组: 把会话归属到 cwd 对应的工作区(resolveByPath ?? create + attachSession; 可选依赖; headless 环境自动跳过)
    void (async () => {
        try {
            const ws = await ensureWorkspace(ctx, canonical);
            if (ws?.attachSession)
                await ws.attachSession(newSessionId);
        }
        catch (e) {
            console.warn('[harness-mcp-server] workspace attach failed:', String(e));
        }
    })();
    // title 命名(可选): 创建会话后立即命名(走 sessionTitle 服务的 rename)
    if (title) {
        try {
            const session = handle.agent.session;
            const st = ctx.get('sessionTitle');
            st?.rename?.(session, title);
        }
        catch (e) {
            console.warn('[harness-mcp-server] session title set failed:', String(e));
        }
    }
    return rec;
}
/** 同一 cwd 串行执行, 避免并发 followup 同一会话 */
async function withLock(cwd, fn) {
    const prev = agentLocks.get(cwd) ?? Promise.resolve();
    const next = prev.then(fn, fn);
    agentLocks.set(cwd, next.catch(() => { }));
    return next;
}
/** 超时哨兵: 区分「超时打断」与 executeTask 内部的真实异常 */
const TASK_TIMEOUT = Symbol('task-timeout');
/** 从 agent 最终回答里解析 changes/verification/leftovers(从后往前找候选, 更可靠) */
function parseSummary(assistantText) {
    const empty = { changes: '', verification: '', leftovers: '' };
    // 收集所有 {...} 候选(agent 被要求输出一行 summary JSON)
    const candidates = [];
    const re = /\{[\s\S]*?\}/g;
    let m;
    while ((m = re.exec(assistantText)) !== null) {
        candidates.push(m[0]);
    }
    // 从后往前: 最后出现的候选最可能是最终 summary, 逐个尝试解析
    for (let i = candidates.length - 1; i >= 0; i--) {
        try {
            const obj = JSON.parse(candidates[i]);
            const s = (v) => (typeof v === 'string' ? v : '');
            const changes = s(obj.changes) || s(obj.改动);
            const verification = s(obj.verification) || s(obj.验证);
            const leftovers = s(obj.leftovers) || s(obj.遗留) || s(obj.leftover);
            // 只要含任一 summary 字段就采纳, 否则继续尝试更早的候选
            if (changes || verification || leftovers) {
                return { changes, verification, leftovers };
            }
        }
        catch {
            // 非合法 JSON, 继续尝试下一个候选
        }
    }
    return empty;
}
/** 分字段限长, 保证返回的永远是完整合法 JSON(避免 slice(-16000) 截断开头导致非法 JSON) */
function truncateResult(result) {
    return {
        ...result,
        assistantText: result.assistantText.slice(0, 8000),
        toolCalls: result.toolCalls.slice(0, 50).map((c) => ({ ...c, args: c.args.slice(0, 2000) })),
        toolResults: result.toolResults.slice(0, 20).map((r) => r.slice(0, 2000)),
    };
}
/** (provider:model) → 上下文窗口 token 数缓存; 解析失败缓存 null(不反复查询) */
const modelWindowCache = new Map();
/** 经 ctx.llm.resolveModelInfo 解析某 provider/model 的上下文窗口; 不可解析返回 null。
 *  注意 dsh-llm 的 LlmRuntime 服务只暴露 resolveModelInfo(适配器层的 resolveModel 是 LlmAdapter 方法,
 *  服务上不存在) —— 之前的 resolveModel 调用恒 undefined, 导致 window/ratio 恒 null。 */
async function modelWindowOf(ctx, provider, model) {
    if (!provider || !model)
        return null;
    const key = `${provider}:${model}`;
    const cached = modelWindowCache.get(key);
    if (cached !== undefined)
        return cached;
    let window = null;
    try {
        const llm = ctx.get('llm');
        const info = await llm?.resolveModelInfo?.(provider, model);
        window = info?.context?.contextWindow ?? null;
    }
    catch {
        window = null;
    }
    modelWindowCache.set(key, window);
    return window;
}
/** 会话生效的 provider/model: agent.options 优先, 其次 agentDefaultModel 默认选择, 否则插件配置 */
function agentModelOf(ctx, agent) {
    if (agent?.options?.model)
        return { provider: agent.options.provider, model: agent.options.model };
    const def = ctx.get('agentDefaultModel')?.currentSelection?.();
    if (def?.model)
        return def;
    return { provider: runtimeConfig.provider, model: runtimeConfig.model || undefined };
}
/** 完整上下文占用: 事件数 + 表面 token 数 + 最近请求压力 + 模型窗口 + 占用比(百分比, 1 位小数);
 *  tokenMeter 缺失返回 null; 窗口不可解析时 window/ratio 为 null。 */
async function contextUsage(ctx, session, agent) {
    try {
        const m = ctx.get('tokenMeter')?.measure?.(session);
        if (!m)
            return null;
        const events = m.logRevision ?? 0;
        const tokens = m.surfaceTokens ?? 0;
        const pressure = m.totalTokens ?? 0;
        const { provider, model } = agentModelOf(ctx, agent);
        const window = await modelWindowOf(ctx, provider, model);
        const ratio = window && window > 0 ? Math.round((tokens / window) * 1000) / 10 : null;
        return { events, tokens, pressure, window, ratio };
    }
    catch {
        return null;
    }
}
/** 按 sessionId 找 live agent(池优先, 其次 ctx.agents; 都不是返回 undefined) */
function liveAgentFor(ctx, sessionId) {
    if (!sessionId)
        return undefined;
    const cwd = sessionToCwd.get(sessionId);
    const pooled = cwd !== undefined ? liveAgents.get(cwd) : undefined;
    if (pooled)
        return pooled.handle.agent;
    return ctx.agents.get(SessionId(sessionId));
}
/** 任务进行中的步骤信息: 从任务开始点(baseline)之后的日志增量里提取, 供客户端汇报进度;
 *  同时附上任务会话的上下文占用 context(tokenMeter/窗口不可解析时为 null) */
async function taskProgressOf(ctx, item) {
    const info = { status: item.status };
    if (item.status === 'queued') {
        info.context = null;
        return info;
    }
    const agent = liveAgentFor(ctx, item.sessionId);
    const session = agent?.session;
    if (!session?.log) {
        // 会话已退役/未加载: 无法读增量, 只附完成时快照(仍为 null 表示无法测量)
        info.context = item.contextUsage ?? null;
        return info;
    }
    const slice = session.log.slice(item.baseline ?? 0);
    let toolCalls = 0;
    let currentTool;
    let lastText = '';
    for (const ev of slice) {
        const e = ev;
        if (e.type === 'tool/call') {
            toolCalls++;
            currentTool = { name: e.data?.name ?? '?', args: String(e.data?.arguments ?? '').slice(0, 300) };
        }
        else if (e.type === 'assistant/message' || e.type === 'user/message') {
            const text = (e.data?.message?.content ?? []).filter((c) => c.type === 'text' && c.text).map((c) => c.text).join(' ').trim();
            if (text)
                lastText = text.slice(0, 200);
        }
    }
    info.events = slice.length;
    info.toolCalls = toolCalls;
    if (currentTool)
        info.currentTool = currentTool;
    if (lastText)
        info.lastText = lastText;
    // 等待输入感知: 审批(本插件应答链挂起)与提问(本插件 provider 挂起 / GUI 路由的挂起 ask_user_question)
    // → status=waiting_input + prompts[], 供 MCP 调用方感知弹窗并响应(prompt_respond / web GUI)
    const prompts = [];
    for (const pa of pendingApprovals.values()) {
        if (pa.agentId === item.sessionId) {
            prompts.push({ type: 'approval', id: pa.promptId, toolName: pa.toolName, ...(pa.reason !== undefined ? { reason: pa.reason } : {}) });
        }
    }
    for (const pq of pendingQuestions.values()) {
        if (pq.agentId === item.sessionId)
            prompts.push({ type: 'question', id: pq.promptId, questions: pq.questions });
    }
    if (!questionsProviderOurs) {
        const detected = detectPendingAskUser(session);
        if (detected) {
            prompts.push({ type: 'question', id: detected.id, questions: detected.questions, note: 'routed to the web GUI provider; answer in the DSH web UI' });
        }
    }
    if (prompts.length > 0) {
        info.status = 'waiting_input';
        info.prompts = prompts;
    }
    // 上下文占用: 任务会话仍 live 时实时测量(池优先); 已退役/未加载则回退任务完成时快照
    info.context = agent
        ? await contextUsage(ctx, agent.session, agent)
        : (item.contextUsage ?? null);
    return info;
}
/** 从任意事件/消息对象递归收集文本(容错遍历; 供结果提取与 session_read 复用) */
function extractText(obj, out) {
    if (Array.isArray(obj)) {
        obj.forEach((x) => extractText(x, out));
        return;
    }
    if (obj && typeof obj === 'object') {
        const rec = obj;
        if (typeof rec.text === 'string' && rec.text.trim())
            out.push(rec.text);
        if (typeof rec.content === 'string' && rec.content.trim())
            out.push(rec.content);
        for (const v of Object.values(rec))
            extractText(v, out);
    }
}
/** cwd 白名单校验: 配置了 workspaceRoots 时只允许在列出的目录下干活(防路径穿越); 未配置恒放行 */
function cwdAllowed(workdir) {
    if (runtimeConfig.workspaceRoots.length === 0)
        return true;
    return runtimeConfig.workspaceRoots.some((root) => {
        const r = resolve(root);
        return workdir === r || workdir.startsWith(r + '/');
    });
}
/** 解析 agent 生效的 provider/model: 显式覆盖 → 插件配置 → agentDefaultModel 默认选择。
 *  必须恒有 model: 预设 persona 模板(如 standard 的 "powered by the {{model}} model")引用 {{model}} 变量,
 *  该变量取自 agent.options.model —— 缺失时 prompt 组装抛
 *  `prompt variable "{{model}}" has no value for this assembly (section "deployment:persona")`, 本轮空跑。 */
function resolveAgentModel(ctx, modelOpts) {
    const explicit = modelOpts?.model ?? runtimeConfig.model;
    if (explicit)
        return { provider: modelOpts?.provider ?? runtimeConfig.provider, model: explicit };
    const def = ctx.get('agentDefaultModel')?.currentSelection?.();
    const provider = modelOpts?.provider ?? def?.provider ?? runtimeConfig.provider;
    const model = def?.model;
    if (!model) {
        console.warn('[harness-mcp-server] no model resolved (agentDefaultModel service missing?); persona {{model}} may fail to assemble');
    }
    return { provider, model };
}
/** MCP 驱动的会话 id 集(创建/接管即标记): 审批应答者只认领这些会话的审批, 其余交给 web GUI 应答链 */
const mcpSessionIds = new Set();
/** 待响应的审批 prompt(promptId = 审计事件 approval/asked 的 id) */
const pendingApprovals = new Map();
/** 待响应的提问 prompt */
const pendingQuestions = new Map();
/** 提问 provider 是否由本插件持有(false = web GUI 占槽, 提问路由到 GUI) */
let questionsProviderOurs = false;
/** 会话级模型覆盖(sessionId → {provider?, model}): session_set_model 记录, resume 时同样生效 */
const sessionModelOverrides = new Map();
/** 从审批请求的会话事件里取审计 id(倒查最近一条匹配 callId 的 approval/asked, 与 web GUI 应答者同款); 找不到时合成兜底 id */
function approvalPromptIdOf(req) {
    const events = (req.agent.session.events ?? []);
    const decided = new Set();
    for (let i = events.length - 1; i >= 0; i--) {
        const e = events[i];
        if (e?.type === 'approval/decided')
            decided.add(e.data?.id);
        else if (e?.type === 'approval/asked') {
            if (decided.has(e.data?.id))
                continue;
            if ((req.callId ?? null) !== (e.data?.callId ?? null))
                continue;
            if (e.data?.id)
                return String(e.data.id);
        }
    }
    return `approval-${req.toolName}-${Date.now()}`;
}
/** 检测挂起的 ask_user_question 工具调用(web GUI 持有提问 provider 时, 这是感知提问的唯一途径):
 *  倒查最后一条 ask_user_question 的 tool/call, 其后没有 tool/result 即为挂起。 */
function detectPendingAskUser(session) {
    const log = session.log ?? [];
    let callIdx = -1;
    for (let i = log.length - 1; i >= 0; i--) {
        const e = log[i];
        if (e.type === 'tool/call' && e.data?.name === 'ask_user_question') {
            callIdx = i;
            break;
        }
    }
    if (callIdx < 0)
        return undefined;
    for (let i = callIdx + 1; i < log.length; i++) {
        if (log[i].type === 'tool/result')
            return undefined;
    }
    const args = log[callIdx].data?.arguments;
    let questions = [];
    try {
        const parsed = JSON.parse(args ?? '{}');
        questions = (parsed.questions ?? []).map((q) => ({
            id: String(q.id ?? ''),
            question: String(q.question ?? ''),
            ...(q.detail !== undefined ? { detail: q.detail } : {}),
            ...(q.options !== undefined ? { options: (q.options ?? []).map((o) => ({ label: o.label ?? '' })) } : {}),
        }));
    }
    catch { /* 参数不可解析时仅报挂起, 不带原文 */ }
    return { id: `ask-${callIdx}`, questions };
}
/**
 * 按 agent id 挂起的 notice 队列: 拦截时只入队, 绝不直接写会话日志。
 * 修复回归: 旧版 appendPromptNotice 在 approval/request 拦截期直接 append user/message,
 * 若时机落在 assistant 带 tool_calls 的消息与其 tool/result 之间, 会打断消息序列,
 * 使下个模型请求报 'An assistant message with tool_calls must be followed by tool
 * messages responding to each tool_call_id'(INVALID_REQUEST), 会话失效。
 */
const pendingNotices = new Map();
/** 从 agent 鸭子类型取稳定 id(与 mcpSessionIds / 审批应答者同款身份解析) */
function agentIdOf(agent) {
    const a = agent;
    if (a === undefined)
        return undefined;
    const id = a.id ?? a.session?.id;
    return id === undefined ? undefined : String(id);
}
/** 构造一条 form:'notice' 的 plugin 来源 user/message(web UI 折叠提示行专属呈现, 与官方插件同款) */
function noticeUserMessage(text, summary) {
    return createUserMessage({
        content: [{ type: 'text', text }],
        source: {
            kind: 'plugin',
            plugin: 'harness-mcp-server',
            form: 'notice',
            summary: boundContextSummary(summary),
        },
    });
}
/** 【2】web UI 提示(安全落点版): 审批/提问被 MCP 拦截/响应时只把提示入队, 不写会话日志。
 *  工具完成后由 tools/post-execute 监听器把这些 notice 并入该工具结果的 additionalContexts,
 *  交给 agent-loop 在 tool/result 之后、下个模型请求之前追加(官方 dsh-repeat-tool-reminder /
 *  dsh-tool-goal 同款机制)—— 从不在 assistant(tool_calls) 与其 tool/result 之间插入
 *  user/message, 因此不破坏模型消息序列。 */
function queuePromptNotice(agent, text, summary) {
    const agentId = agentIdOf(agent);
    if (agentId === undefined) {
        console.warn('[harness-mcp-server] prompt notice skipped (agent has no id):', summary);
        return;
    }
    const list = pendingNotices.get(agentId) ?? [];
    list.push({ text, summary });
    pendingNotices.set(agentId, list);
}
/** tools/post-execute 安全投递: 该 agent 有挂起 notice 时, 把它们并入 downstream decision 的
 *  additionalContexts(不改动 decision 本身); 没有则原样放行。 */
function flushPromptNotices(agent, downstream) {
    const agentId = agentIdOf(agent);
    if (agentId === undefined)
        return downstream;
    const notices = pendingNotices.get(agentId);
    if (notices === undefined || notices.length === 0)
        return downstream;
    pendingNotices.delete(agentId);
    const contexts = notices.map((n) => noticeUserMessage(n.text, n.summary));
    return { ...downstream, additionalContexts: [...(downstream.additionalContexts ?? []), ...contexts] };
}
/** 核心执行: 组装任务(注入记忆上下文+结构化要求) → agent 执行 → 读结构化结果。
 *  opts.onAgent 在 agent 就绪后回调一次, 供 task_cancel 注册打断钩子;
 *  opts.fresh = true 且未传 sessionId 时强制全新会话(跳过池复用, 见 getAgent);
 *  opts.provider/model 为按次调用的模型覆盖(对新建/resume 会话生效);
 *  opts.timeoutMs 为按次执行超时(覆盖全局 taskTimeoutMs; 0 = 不限制);
 *  opts.shouldAbort 在等锁后/执行前检查, 支持取消"排队/等锁中"的任务(agent 未就绪时 task_cancel 置 cancelled)。 */
async function executeTask(ctx, task, context, cwd, resumeSessionId, title, opts) {
    // 规范化 cwd: realpath 解析符号链接与 .. 段, 避免 /a、/a/.、相对路径、符号链接成为不同 Map key
    // 导致重复创建会话/并发冲突; 同时也是与 workspace.path 精确比对的唯一 canon
    const workdir = await canonicalCwd(cwd ? resolve(cwd) : process.cwd());
    // cwd 白名单: 配置了 workspaceRoots 时, 只允许在列出的目录下干活(防路径穿越)
    if (!cwdAllowed(workdir)) {
        throw new Error(`cwd not allowed (outside workspaceRoots): ${workdir}`);
    }
    // sessionId 用 session 锁, 否则用 cwd 锁——都防同一 agent 会话被并发 followup
    const lockKey = resumeSessionId ? `session:${resumeSessionId}` : workdir;
    return withLock(lockKey, async () => {
        // 取消检查: 任务在等锁期间被 task_cancel(agent 未就绪路径)置了 cancelled → 执行前中止, 不启动 agent
        if (opts?.shouldAbort?.())
            throw new Error('task cancelled');
        const { sessionId, handle, disposeAfter } = await getAgent(ctx, workdir, resumeSessionId, title, opts?.fresh, { provider: opts?.provider, model: opts?.model });
        const baseline = (handle.agent.session.log ?? []).length;
        // 组装完整任务文本: 记忆上下文 + 任务 + 结构化输出要求
        const fullTask = [
            context ? `【记忆/上下文(供参考, 来自 Hermes 大脑)】\n${context}\n` : '',
            `【任务】\n${task}\n`,
            `【完成后必须】用一行 JSON 总结(不要 markdown 代码块包裹, 直接输出这一行):`,
            `{"changes":"改了什么","verification":"怎么验证的","leftovers":"遗留问题"}`,
        ].filter(Boolean).join('\n');
        // 结构化读输出(提前声明, 供执行异常兜底填充)
        const result = {
            taskId: '', sessionId, assistantText: '', toolCalls: [], toolResults: [],
            changes: '', verification: '', leftovers: '',
        };
        // 驱动 agent 执行; 执行/调度层抛出的异常(非超时)转成结构化 error 结果, 不再上抛导致空跑
        try {
            handle.agent.followup(createUserMessage({ content: [{ type: 'text', text: fullTask }], source: { kind: 'plugin', plugin: 'harness-mcp-server' } }));
            // agent 就绪: 通知调用方(供 task_cancel 注册打断钩子)
            opts?.onAgent?.(handle.agent);
            // 超时保护: whenIdle 无限等待会让 MCP 客户端挂死; 到点后 cancel 打断本轮, 回收部分输出
            // (per-task timeoutMs 覆盖全局 taskTimeoutMs)
            const taskTimeout = opts?.timeoutMs ?? runtimeConfig.taskTimeoutMs;
            let timedOut = false;
            let timer;
            try {
                await Promise.race([
                    handle.agent.whenIdle(),
                    new Promise((_resolve, reject) => {
                        if (taskTimeout > 0) {
                            timer = setTimeout(() => { timedOut = true; reject(TASK_TIMEOUT); }, taskTimeout);
                        }
                    }),
                ]);
            }
            catch (e) {
                if (e !== TASK_TIMEOUT)
                    throw e;
                // cancel 丢弃未开始的排队输入, 中止活动回合; 之后 whenIdle 很快落定
                try {
                    handle.agent.cancel({ kind: 'hook', reason: 'harness-mcp-timeout' });
                }
                catch { /* 打断失败不阻断回收 */ }
                await handle.agent.whenIdle();
                result.timeout = true;
            }
            finally {
                if (timer !== undefined)
                    clearTimeout(timer);
            }
        }
        catch (e) {
            // 【1c】agent 执行/调度层异常(如 agent-loop 抛错): 转成结构化 error 结果, 任务以 status=error 结束
            const err = e;
            result.error = {
                errorCode: err?.code ?? 'AGENT_ERROR',
                errorMessage: err?.message ?? String(e),
                errorCategory: 'execution',
            };
        }
        // 结构化读输出
        try {
            const log = (handle.agent.session.log ?? []).slice(baseline);
            for (const e of log) {
                const ev = e;
                if (ev.type === 'assistant/message') {
                    const d = ev.data;
                    const content = d?.message?.content;
                    if (content) {
                        const texts = content.filter((c) => c.type === 'text' && c.text).map((c) => c.text);
                        if (texts.length)
                            result.assistantText += texts.join('\n') + '\n';
                    }
                }
                else if (ev.type === 'tool/call') {
                    const d = ev.data;
                    result.toolCalls.push({
                        name: d?.name ?? '?',
                        args: (d?.arguments ?? JSON.stringify(d?.input ?? null) ?? '').slice(0, 2000),
                    });
                }
                else if (ev.type === 'tool/result') {
                    const texts = [];
                    extractText(ev.data ?? ev, texts);
                    if (texts.length)
                        result.toolResults.push(texts.join('\n').slice(0, 3000));
                }
            }
            // 【1b】模型/执行失败感知: turn/end reason.kind ∈ {error, failed, max-tokens} → 结构化错误。
            // 真实模型接口报错(配额/网络/参数)以 reason.kind='error' 出现(实测 429 QUOTA 会话),
            // 'max-tokens' 为输出上限(模型侧限制); LLM 错误码原样保留, 'UNKNOWN' 视为通用执行失败。
            for (const e of log) {
                const ev = e;
                const reason = ev.data?.reason;
                if (ev.type === 'turn/end' && reason !== undefined && (reason.kind === 'error' || reason.kind === 'failed' || reason.kind === 'max-tokens')) {
                    const err = (reason.kind === 'error' || reason.kind === 'failed') ? reason.error : undefined;
                    const code = err?.code ?? (reason.kind === 'max-tokens' ? 'MAX_TOKENS' : 'UNKNOWN');
                    result.error = {
                        errorCode: code,
                        errorMessage: err?.message ?? (reason.kind === 'max-tokens' ? 'output token ceiling reached' : 'agent turn failed'),
                        errorCategory: code === 'UNKNOWN' ? 'execution' : 'model',
                    };
                    break;
                }
                // 兜底: assistant/chunk finish 报错(未及 turn/end 时)
                if (ev.type === 'assistant/chunk' && ev.data?.chunk?.type === 'finish' && ev.data.chunk.reason?.kind === 'error' && ev.data.chunk.reason.failure) {
                    result.error = {
                        errorCode: ev.data.chunk.reason.failure.code ?? 'MODEL_ERROR',
                        errorMessage: ev.data.chunk.reason.failure.message ?? 'model request failed',
                        errorCategory: 'model',
                    };
                    break;
                }
            }
        }
        catch (e) {
            result.assistantText = `[读输出异常] ${String(e)}`;
        }
        // 解析结构化 summary
        const summary = parseSummary(result.assistantText);
        result.changes = summary.changes;
        result.verification = summary.verification;
        result.leftovers = summary.leftovers;
        // C3 兜底: 模型没按格式吐 JSON 时, 用最近工具结果摘要填充 changes(尽力而为, 不再全空)
        if (!result.changes && !result.verification && !result.leftovers) {
            const last = result.toolResults.slice(-5).join('\n').slice(0, 1500);
            if (last)
                result.changes = `(heuristic from tool output) ${last}`;
        }
        // 超时标注: leftovers 为空时补一句引导, 提示可用 sessionId 续接
        if (result.timeout === true && !result.leftovers) {
            result.leftovers = '任务超时被自动取消, 以上为部分进展; 可用 sessionId 续接继续';
        }
        // 上下文占用快照: 任务结束瞬间(agent 仍 live)测量, 挂到 result 供调用方跟踪占用;
        // tokenMeter 缺失返回 null, 窗口不可解析时 window/ratio 为 null(不阻断结果返回)
        try {
            result.context = await contextUsage(ctx, handle.agent.session, handle.agent);
        }
        catch {
            result.context = null;
        }
        // resume 兜底分支: 尽力 flush 持久化, 再释放我们 resume 出来的句柄(不留给僵尸 live agent)
        if (disposeAfter) {
            try {
                await ctx.get('sessions')?.flush?.(handle.agent.session);
            }
            catch {
                /* flush 失败不阻断结果返回 */
            }
            try {
                await handle.dispose();
            }
            catch {
                /* 释放失败不影响结果 */
            }
        }
        return result;
    });
}
const taskQueue = new Map();
/** taskId → 打断钩子: 任务进入 running 且 agent 就绪后注册, task_cancel 调用后清理 */
const taskCancelHooks = new Map();
/** 找会话 header: live 优先, 其次持久化 list(轻量元数据扫描, 不加载整日志) */
async function findSessionHeader(ctx, sessionId) {
    const sessions = ctx.get('sessions');
    const live = sessions?.get?.(sessionId);
    if (live !== undefined)
        return live.header;
    const persistence = ctx.get('sessionPersistence');
    for (const header of (await persistence?.list?.()) ?? []) {
        if (header.id === sessionId)
            return header;
    }
    return undefined;
}
/**
 * 存量捞回: 启动时把现存未分组的会话补挂到已注册工作区。
 * 条件: header.cwd 的 realpath 等于某已注册 workspace.path, 且该 sessionId 不在其花名册里。
 * 只补挂到"已注册"工作区, 不新建(避免把无关目录刷成新工作区); 单会话失败不影响其余。
 */
async function reattachOrphanSessions(ctx) {
    const registry = ctx.get('workspaceRegistry');
    const byPath = new Map();
    for (const ws of registry?.list?.() ?? [])
        byPath.set(ws.path, ws);
    if (byPath.size === 0)
        return { attached: 0, failed: 0 };
    // live + 持久化 header 合并(live 优先), 按 id 去重
    const headers = new Map();
    const sessions = ctx.get('sessions');
    for (const session of sessions?.list?.() ?? [])
        headers.set(session.header.id, session.header);
    const persistence = ctx.get('sessionPersistence');
    for (const header of (await persistence?.list?.()) ?? []) {
        if (!headers.has(header.id))
            headers.set(header.id, header);
    }
    let attached = 0;
    let failed = 0;
    for (const header of headers.values()) {
        if (header.cwd === undefined)
            continue;
        const canonical = await canonicalCwd(header.cwd);
        const ws = byPath.get(canonical);
        if (ws === undefined || !ws.attachSession)
            continue;
        if (ws.sessionIds.includes(header.id))
            continue;
        try {
            await ws.attachSession(header.id);
            attached++;
            console.log(`[harness-mcp-server] 存量捞回: session ${header.id} -> workspace ${ws.path}`);
        }
        catch (e) {
            failed++;
            console.warn(`[harness-mcp-server] 存量捞回失败 session ${header.id}:`, e?.message ?? e);
        }
    }
    return { attached, failed };
}
/** 在给定 McpServer 上注册工具 */
function registerTools(mcp, ctx) {
    mcp.tool('echo', '回显输入, 验证 MCP server 连通', { text: z.string() }, async ({ text }) => {
        return out(`收到: ${text} @ ${Date.now()}`);
    });
    mcp.tool('harness_list_tools', '列出 Harness 当前注册的所有工具名', {}, async () => {
        const tools = ctx.tools;
        const names = tools && typeof tools.keys === 'function' ? Array.from(tools.keys()) : [];
        return out(JSON.stringify(names));
    });
    // 运维总览: 队列/agent 池/live 会话/运行时配置 —— 外部客户端一眼看清系统水位
    mcp.tool('harness_status', '系统水位总览: 任务队列(排队/执行/完成/失败)、agent 常驻池、live 会话数、运行时配置。', {}, async () => {
        let queued = 0, running = 0, done = 0, error = 0;
        for (const t of taskQueue.values()) {
            if (t.status === 'queued')
                queued++;
            else if (t.status === 'running')
                running++;
            else if (t.status === 'error')
                error++;
            else
                done++;
        }
        const liveCount = (ctx.agents.list?.() ?? []).length;
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
        }, null, 2));
    });
    // 模型目录: 缺省枚举所有已注册 provider 的模型(listProviders), 并补上已声明但未激活的配置 provider;
    // 传 provider 只列该 provider; withWindow=true 时逐模型解析上下文窗口(多一次 llm 查询)
    mcp.tool('model_list', '列出可用模型目录: 缺省枚举所有已注册 provider 的模型(listProviders), 并补上已声明但未激活的配置 provider(active:false); 传 provider 只列该 provider; withWindow=true 时逐模型解析 contextWindow(可能较慢)。', {
        provider: z.string().optional().describe('只列出该 provider 路由的模型(缺省: 全部已注册 provider)'),
        withWindow: z.boolean().optional().describe('true = 逐模型解析 contextWindow'),
    }, async ({ provider, withWindow }) => {
        const llm = ctx.get('llm');
        if (!llm?.listProviders)
            return err(JSON.stringify({ error: 'llm service unavailable' }));
        const registered = llm.listProviders?.() ?? [];
        const directory = llm.listConfigurableProviders?.() ?? [];
        const rows = [];
        const seen = new Set();
        // 指定 provider 时只列它(未注册 → 报错行); 否则遍历全部已注册路由
        const targets = provider ? [{ id: provider, name: provider }] : registered;
        for (const p of targets) {
            seen.add(p.id);
            try {
                const models = (await llm.listModels?.(p.id)) ?? [];
                const listed = await Promise.all(models.map(async (m) => {
                    const row = { id: m.id, name: m.name, description: m.description, inputModalities: m.inputModalities };
                    if (withWindow)
                        row.contextWindow = await modelWindowOf(ctx, p.id, m.id);
                    return row;
                }));
                rows.push({ provider: p.id, providerName: p.name, active: true, total: listed.length, models: listed });
            }
            catch (e) {
                rows.push({ provider: p.id, providerName: p.name, active: true, error: e?.message ?? String(e) });
            }
        }
        // 补全目录(仅在枚举全部时): 已声明但未注册(未激活/未配置)的 provider 也列出, 客户端可见"全部可能配置"
        if (!provider) {
            for (const cp of directory) {
                if (seen.has(cp.provider))
                    continue;
                seen.add(cp.provider);
                rows.push({
                    provider: cp.provider,
                    providerName: cp.displayName,
                    active: false,
                    total: 0,
                    models: [],
                    note: 'declared but not active (configure the provider to activate)',
                });
            }
        }
        return out(JSON.stringify({ total: rows.length, providers: rows }, null, 2));
    });
    // 工作区分组视图: 对齐 UI 侧 dsh-workspace, 便于按项目维度管理会话
    mcp.tool('workspace_list', '列出工作区及其会话分组(dsh-workspace 的花名册), 便于按项目维度管理; workspaceRegistry 未加载时报错。', {}, async () => {
        const registry = ctx.get('workspaceRegistry');
        const list = registry?.list?.() ?? [];
        const workspaces = list.map((w) => ({
            id: w.id, path: w.path, title: w.title,
            sessionCount: w.sessionIds?.length ?? 0,
            sessionIds: (w.sessionIds ?? []).slice(0, 100),
        }));
        return out(JSON.stringify({ total: workspaces.length, workspaces }, null, 2));
    });
    // 读会话事件流: 审计或续接前回顾 Harness 到底做了什么
    mcp.tool('session_read', '读会话的事件流(文本/工具调用/结果), 审计或续接前回顾。池/live 会话直读; 持久化会话临时 resume 读取后 flush 并释放。', {
        sessionId: z.string().describe('要读取的会话 id(池/live/持久化均可)'),
        limit: z.number().int().min(1).max(500).optional().describe('最多返回最近事件数(默认 100)'),
    }, async ({ sessionId, limit }) => {
        let resolved;
        try {
            resolved = await getAgent(ctx, '', sessionId);
        }
        catch (e) {
            return err(JSON.stringify({ error: e?.message ?? String(e) }));
        }
        const agent = resolved.handle.agent;
        try {
            const log = (agent.session.log ?? []);
            // limit 按「表面事件」计: 真实 dsh 日志里 assistant/chunk、reasoning-chunks、step/* 等流式/内部事件
            // 占绝对多数且稀疏夹杂表面事件, 直接 slice 原始日志会让 limit 失效(最后 N 条原始日志常只含 1 条表面事件)。
            // 因此先收集表面事件下标, 再取最近 N 条格式化。
            const surfaceTypes = new Set(['user/message', 'assistant/message', 'tool/call', 'tool/result']);
            const max = limit ?? 100;
            const surfaceIdx = [];
            for (let i = 0; i < log.length; i++) {
                const t = log[i]?.type;
                if (t !== undefined && surfaceTypes.has(t))
                    surfaceIdx.push(i);
            }
            const start = Math.max(0, surfaceIdx.length - max);
            const events = [];
            for (let k = start; k < surfaceIdx.length; k++) {
                const ev = log[surfaceIdx[k]];
                const e = ev;
                const type = e.type;
                if (type === 'user/message' || type === 'assistant/message') {
                    const content = e.data?.message?.content;
                    const text = (content ?? []).filter((c) => c.type === 'text' && c.text).map((c) => c.text).join('\n').slice(0, 4000);
                    events.push({ seq: e.seq, type, text: text || '(no text blocks)' });
                }
                else if (type === 'tool/call') {
                    events.push({ seq: e.seq, type, text: `${e.data?.name ?? '?'}(${String(e.data?.arguments ?? '').slice(0, 2000)})` });
                }
                else if (type === 'tool/result') {
                    const texts = [];
                    extractText(e.data ?? ev, texts);
                    events.push({ seq: e.seq, type, text: texts.join('\n').slice(0, 3000) || '(empty result)' });
                }
            }
            return out(JSON.stringify({ sessionId, total: surfaceIdx.length, logEvents: log.length, returned: events.length, events }, null, 2));
        }
        finally {
            if (resolved.disposeAfter) {
                try {
                    await ctx.get('sessions')?.flush?.(agent.session);
                }
                catch { /* ignore */ }
                try {
                    await resolved.handle.dispose();
                }
                catch { /* ignore */ }
            }
        }
    });
    // 同步执行任务(简单场景: Hermes 下发 → 立即拿结果)。
    // 不传 sessionId 时 cwd 必填(避免误用 dsh 进程目录); 传 timeoutMs 可把长任务转成异步(返回 taskId 供轮询/取消)。
    mcp.tool('agent_run', '同步执行任务(改代码/分析/跑命令), 返回结构化结果, 附任务会话上下文占用 context(events/tokens/pressure/window/ratio)。可传 sessionId 续接已有会话(长任务分多轮投喂); 可传 newSession:true 强制全新会话(不复用池); 可传 model/provider 按次选模型; 传 timeoutMs(毫秒)超时后自动转为异步任务(返回 taskId, 用 task_result/task_cancel/task_wait 跟进)。', {
        task: z.string().describe('要 Harness 执行的自然语言任务'),
        context: z.string().optional().describe('Hermes 记忆/上下文, 注入给 agent 参考'),
        cwd: z.string().optional().describe('工作目录(不传 sessionId 时必填; 可用 workspace_list 查看可用目录)'),
        sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果里的 sessionId 字段)'),
        newSession: z.boolean().optional().describe('true = 强制全新会话(跳过该 cwd 的池复用, 旧会话退役但持久化保留, 仍可凭 sessionId 续接); 缺省 = 复用该 cwd 的常驻会话'),
        model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效; 池复用的会话保持原模型)'),
        provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
        timeoutMs: z.number().int().min(0).optional().describe('同步等待上限毫秒数(默认 taskTimeoutMs=60 分钟; 建议按客户端 HTTP 超时设小, 如 120000; 0 = 不转异步)'),
        title: z.string().optional().describe('新会话的标题(创建时命名, 便于会话列表归档)'),
    }, async ({ task, context, cwd, sessionId, newSession, model, provider, timeoutMs, title }) => {
        if (!cwd && !sessionId) {
            return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }));
        }
        const now = Date.now();
        // TTL 清理: 删除已完成/失败且超时的任务
        for (const [tid, t] of taskQueue) {
            if ((t.status === 'done' || t.status === 'error') && t.finishedAt && now - t.finishedAt > runtimeConfig.taskTtlMs) {
                taskQueue.delete(tid);
            }
        }
        // 队列容量(与 task_inbox 一致): 活动任务超过上限则拒绝
        let active = 0;
        for (const t of taskQueue.values())
            if (t.status === 'queued' || t.status === 'running')
                active++;
        if (active >= runtimeConfig.maxQueue) {
            return err(JSON.stringify({ error: `task queue full (${active}/${runtimeConfig.maxQueue})` }));
        }
        // 统一注册为可查/可取消的任务条目: 同步完成时回填真实 taskId, 超时转异步后同一条目继续
        const id = randomUUID();
        const item = {
            id, task, context: context ?? '', cwd: cwd ?? process.cwd(), status: 'running', createdAt: now,
            ...(sessionId ? { sessionId } : {}),
            ...(newSession ? { newSession: true } : {}),
            ...(model ? { model } : {}),
            ...(provider ? { provider } : {}),
            ...(title ? { title } : {}),
        };
        taskQueue.set(id, item);
        const background = (async () => {
            try {
                item.result = await executeTask(ctx, item.task, item.context, item.cwd, item.sessionId, item.title, {
                    fresh: item.newSession === true,
                    model: item.model,
                    provider: item.provider,
                    timeoutMs: item.timeoutMs,
                    shouldAbort: () => item.cancelled === true,
                    onAgent: (agent) => {
                        // 记录会话与日志起点: 供进度提取(本任务增量)与 task_list 上下文占用
                        item.sessionId = String(agent.session.id);
                        item.baseline = (agent.session.log ?? []).length;
                        taskCancelHooks.set(id, async () => {
                            item.cancelled = true;
                            agent.cancel({ kind: 'hook', reason: 'harness-mcp-task-cancel' });
                        });
                    },
                });
                item.result.taskId = id;
                item.contextUsage = item.result.context ?? null;
                item.sessionId = item.result.sessionId;
                if (item.result.error) {
                    // 模型/执行失败: 任务以 error 结束, client 不再误判成功
                    item.error = `${item.result.error.errorCode}: ${item.result.error.errorMessage}`;
                    item.status = 'error';
                }
                else {
                    item.status = 'done';
                }
                return item.result;
            }
            catch (e) {
                item.error = String(e);
                item.status = 'error';
                return undefined;
            }
            finally {
                taskCancelHooks.delete(id);
                item.finishedAt = Date.now();
            }
        })();
        const waitMs = timeoutMs ?? runtimeConfig.taskTimeoutMs;
        let timer;
        try {
            const result = await Promise.race([
                background,
                new Promise((_resolve, reject) => {
                    if (waitMs > 0)
                        timer = setTimeout(() => reject(TASK_TIMEOUT), waitMs);
                }),
            ]);
            if (!result)
                return err(JSON.stringify({ error: `task failed: ${item.error ?? 'unknown'}` }));
            // 模型/执行失败: 同步路径也以 isError 返回, 附结构化 error + 部分输出供续接
            if (result.error) {
                return err(JSON.stringify({ error: result.error, taskId: id, sessionId: result.sessionId, partial: truncateResult(result) }, null, 2));
            }
            return out(JSON.stringify(truncateResult(result), null, 2));
        }
        catch (e) {
            if (e !== TASK_TIMEOUT)
                throw e;
            // 转异步: 任务在后台继续跑, 客户端用 taskId 轮询/等待/取消; 附带当前进度(含上下文占用)供汇报
            return out(JSON.stringify({
                status: 'async',
                taskId: id,
                progress: await taskProgressOf(ctx, item),
                note: `task still running after ${waitMs}ms; poll via task_result / task_wait, or cancel via task_cancel`,
            }, null, 2));
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
        }
    });
    // 异步 push 任务到队列(Hermes → Harness 任务入口)
    mcp.tool('task_inbox', 'Hermes 把结构化任务(任务+记忆上下文)推入 Harness 队列, 异步执行, 返回 taskId; 任务结束后的上下文占用经 task_result/task_wait/task_list 读取。可传 newSession:true 强制全新会话。', {
        task: z.string().describe('任务内容'),
        context: z.string().optional().describe('Hermes 记忆/上下文, 随任务注入给 agent'),
        cwd: z.string().optional().describe('工作目录'),
        sessionId: z.string().optional().describe('续接已有会话的 sessionId(来自上次 agent_run 结果)'),
        newSession: z.boolean().optional().describe('true = 强制全新会话(不复用该 cwd 的常驻会话); 缺省 = 复用'),
        model: z.string().optional().describe('本次任务使用的模型 id(对新建/resume 会话生效)'),
        provider: z.string().optional().describe('本次任务使用的 provider 路由(默认 deepseek-official)'),
        timeoutMs: z.number().int().min(0).optional().describe('本次任务的执行超时毫秒数(默认 taskTimeoutMs=60 分钟; 超时自动 cancel 并回收部分输出; 0 = 不限制)'),
        title: z.string().optional().describe('新会话的标题(创建时命名)'),
    }, async ({ task, context, cwd, sessionId, newSession, model, provider, timeoutMs, title }) => {
        if (!cwd && !sessionId) {
            return err(JSON.stringify({ error: 'cwd is required when not continuing a session (sessionId); see workspace_list for available roots' }));
        }
        const now = Date.now();
        // TTL 清理: 删除已完成/失败且超时的任务
        for (const [tid, t] of taskQueue) {
            if ((t.status === 'done' || t.status === 'error') && t.finishedAt && now - t.finishedAt > runtimeConfig.taskTtlMs) {
                taskQueue.delete(tid);
            }
        }
        // 队列容量上限: 活动任务(排队+执行中)超过上限则拒绝
        let active = 0;
        for (const t of taskQueue.values())
            if (t.status === 'queued' || t.status === 'running')
                active++;
        if (active >= runtimeConfig.maxQueue) {
            return err(JSON.stringify({ error: `task queue full (${active}/${runtimeConfig.maxQueue})` }));
        }
        const id = randomUUID();
        const item = {
            id, task, context: context ?? '', cwd: cwd ?? process.cwd(), status: 'queued', createdAt: now,
            ...(sessionId ? { sessionId } : {}),
            ...(newSession ? { newSession: true } : {}),
            ...(model ? { model } : {}),
            ...(provider ? { provider } : {}),
            ...(timeoutMs !== undefined ? { timeoutMs } : {}),
            ...(title ? { title } : {}),
        };
        taskQueue.set(id, item);
        // 异步执行(不阻塞 Hermes); agent 就绪后注册打断钩子, 供 task_cancel 主动打断
        void (async () => {
            item.status = 'running';
            try {
                item.result = await executeTask(ctx, item.task, item.context, item.cwd, item.sessionId, item.title, {
                    fresh: item.newSession === true,
                    model: item.model,
                    provider: item.provider,
                    timeoutMs: item.timeoutMs,
                    shouldAbort: () => item.cancelled === true,
                    onAgent: (agent) => {
                        // 记录会话与日志起点: 供进度提取(本任务增量)与 task_list 上下文占用
                        item.sessionId = String(agent.session.id);
                        item.baseline = (agent.session.log ?? []).length;
                        taskCancelHooks.set(id, async () => {
                            item.cancelled = true;
                            agent.cancel({ kind: 'hook', reason: 'harness-mcp-task-cancel' });
                        });
                    },
                });
                item.result.taskId = id;
                item.contextUsage = item.result.context ?? null;
                item.sessionId = item.result.sessionId; // 回填实际使用的会话, 供 task_list 附上下文占用/后续续接
                if (item.result.error) {
                    // 模型/执行失败: 任务以 error 结束, client 不再误判成功
                    item.error = `${item.result.error.errorCode}: ${item.result.error.errorMessage}`;
                    item.status = 'error';
                }
                else {
                    item.status = 'done';
                }
            }
            catch (e) {
                item.error = String(e);
                item.status = 'error';
            }
            finally {
                taskCancelHooks.delete(id);
                item.finishedAt = Date.now();
            }
        })();
        // 队列已接收: context 恒 null(任务未开始, 无会话可测); 执行中/结束后的占用经 task_result/task_wait/task_list 读取
        return out(JSON.stringify({ taskId: id, status: 'queued', context: null }));
    });
    // 取回任务结果(结构化 changes/verification/leftovers; 未完成时带进行中进度)
    mcp.tool('task_result', '取回 task_inbox 提交任务的结构化结果(changes/verification/leftovers); 未完成时返回 progress 供汇报进度; 附任务会话上下文占用 context(events/tokens/pressure/window/ratio)。', { taskId: z.string().describe('task_inbox 返回的 taskId') }, async ({ taskId }) => {
        const item = taskQueue.get(taskId);
        if (!item)
            return err(JSON.stringify({ error: `task not found: ${taskId}` }));
        return out(JSON.stringify({
            taskId: item.id,
            status: item.status,
            error: item.error,
            cancelled: item.cancelled === true || undefined,
            context: item.contextUsage ?? null,
            progress: await taskProgressOf(ctx, item),
            result: item.result ? truncateResult(item.result) : undefined,
        }, null, 2));
    });
    // 阻塞等待任务完成: 一次往返替代 N 次轮询(服务端每 500ms 查一次, 到 timeoutMs 或任务落定返回);
    // 无论完成还是超时, 都附带 progress(进行中的步骤/已用工具/最新文本)供客户端汇报进度
    mcp.tool('task_wait', '阻塞等待一个任务完成/失败后返回其结果(服务端等待, 一次往返替代多次轮询); 超过 timeoutMs 返回当前状态与 progress(正在执行的步骤/工具调用/上下文占用)。客户端 HTTP 超时应大于 timeoutMs。', {
        taskId: z.string().describe('task_inbox / agent_run(转异步)返回的 taskId'),
        timeoutMs: z.number().int().min(100).max(600000).optional().describe('等待上限毫秒数(默认 60000; 上限 10 分钟)'),
    }, async ({ taskId, timeoutMs }) => {
        const item = taskQueue.get(taskId);
        if (!item)
            return err(JSON.stringify({ error: `task not found: ${taskId}` }));
        const wait = timeoutMs ?? 60000;
        const deadline = Date.now() + wait;
        while (item.status === 'queued' || item.status === 'running') {
            const remain = deadline - Date.now();
            if (remain <= 0)
                break;
            await new Promise((r) => setTimeout(r, Math.min(500, remain)));
        }
        return out(JSON.stringify({
            taskId: item.id,
            status: item.status,
            error: item.error,
            cancelled: item.cancelled === true || undefined,
            context: item.contextUsage ?? null,
            progress: await taskProgressOf(ctx, item),
            result: item.result ? truncateResult(item.result) : undefined,
        }, null, 2));
    });
    // 列出最近任务: 供批量轮询与队列观察(task_result 只能逐个查); 可按 sessionId 过滤
    mcp.tool('task_list', '列出最近的任务(taskId/状态/目录/时间/会话上下文/进行中进度), 便于批量轮询与观察队列; 按 createdAt 倒序, 可按 sessionId 过滤。', {
        limit: z.number().int().min(1).max(200).optional().describe('最多返回条数(默认 20)'),
        sessionId: z.string().optional().describe('只返回使用了该会话的任务'),
    }, async ({ limit, sessionId }) => {
        const n = limit ?? 20;
        const items = [...taskQueue.values()]
            .filter((t) => !sessionId || t.sessionId === sessionId)
            .sort((a, b) => b.createdAt - a.createdAt)
            .slice(0, n);
        const tasks = [];
        for (const t of items) {
            // 任务所用会话仍 live 时附上实时上下文占用(池优先); 已退役/未加载则回退任务完成时快照(仍为 null 表示无法测量)
            const agent = liveAgentFor(ctx, t.sessionId);
            const context = agent ? await contextUsage(ctx, agent.session, agent) : (t.contextUsage ?? null);
            const progress = t.status === 'queued' || t.status === 'running' ? await taskProgressOf(ctx, t) : undefined;
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
            });
        }
        return out(JSON.stringify({ total: taskQueue.size, tasks }, null, 2));
    });
    // 打断一个 queued/running 任务: 与超时保护共用 agent.cancel 路径
    mcp.tool('task_cancel', '打断一个 queued/running 的任务(cancel agent 当前回合, 回收部分输出); 已结束的任务为幂等 no-op。', { taskId: z.string().describe('task_inbox 返回的 taskId') }, async ({ taskId }) => {
        const item = taskQueue.get(taskId);
        if (!item)
            return err(JSON.stringify({ error: `task not found: ${taskId}` }));
        if (item.status === 'done' || item.status === 'error') {
            return out(JSON.stringify({ taskId, status: item.status, cancelled: false, note: 'already finished' }));
        }
        const cancel = taskCancelHooks.get(taskId);
        if (!cancel) {
            // agent 未就绪(等锁/排队中): 置 cancelled 标记, executeTask 在锁释放后执行前检查并中止
            item.cancelled = true;
            return out(JSON.stringify({ taskId, status: item.status, cancelled: true, note: 'cancel requested before agent started; will abort on start' }));
        }
        try {
            await cancel();
        }
        catch (e) {
            return err(JSON.stringify({ error: `cancel failed: ${e?.message ?? String(e)}` }));
        }
        return out(JSON.stringify({ taskId, status: item.status, cancelled: true }));
    });
    // 给已有会话改名(走 sessionTitle 服务, 便于会话列表归档)
    mcp.tool('rename_session', '给已有会话改名(走 sessionTitle 服务的 rename), 便于会话列表归档区分。', {
        sessionId: z.string().describe('要改名的会话 id(来自 agent_run 结果里的 sessionId 字段)'),
        title: z.string().describe('新标题'),
    }, async ({ sessionId, title }) => {
        try {
            const sessions = ctx.get('sessions');
            const session = sessions?.get?.(sessionId);
            if (!session)
                return err(JSON.stringify({ error: `session not found: ${sessionId}` }));
            const st = ctx.get('sessionTitle');
            if (!st?.rename)
                return err(JSON.stringify({ error: 'sessionTitle service unavailable' }));
            const snapshot = st.rename(session, title);
            return out(JSON.stringify({ ok: true, sessionId, title: snapshot?.title ?? title }));
        }
        catch (e) {
            return err(JSON.stringify({ error: String(e) }));
        }
    });
    // 会话清单: 让外部客户端看清可续接的会话及其上下文占用, 决定续接哪个 sessionId / 是否开新会话 / 是否压缩
    mcp.tool('session_list', '列出可续接的会话(常驻池 / live / 持久化三层去重, 池优先), 含上下文占用 events/tokens/pressure/window/ratio(经 tokenMeter 测量 + llm 模型窗口); 持久化层未加载日志为 null。', {}, async () => {
        const rows = new Map();
        // 常驻池(本插件持有, 优先级最高; 上下文可直接测量)
        for (const [cwd, rec] of liveAgents) {
            rows.set(String(rec.sessionId), { cwd, source: 'pool', context: await contextUsage(ctx, rec.handle.agent.session, rec.handle.agent) });
        }
        // live 会话(ctx.agents.list(); 可读标题)
        const titleSvc = ctx.get('sessionTitle');
        const liveAgentsList = ctx.agents.list?.() ?? [];
        for (const agent of liveAgentsList) {
            const id = String(agent.session.id);
            const prev = rows.get(id);
            rows.set(id, {
                cwd: agent.session.header?.cwd ?? prev?.cwd,
                source: prev?.source ?? 'live',
                title: prev?.title ?? titleSvc?.get?.(agent.session)?.title,
                context: prev?.context ?? await contextUsage(ctx, agent.session, agent),
            });
        }
        // 持久化(未在上两层出现的会话; 日志未加载, 上下文未知)
        const persistence = ctx.get('sessionPersistence');
        for (const h of (await persistence?.list?.()) ?? []) {
            const id = String(h.id);
            if (!rows.has(id))
                rows.set(id, { cwd: h.cwd, source: 'persisted', context: null });
        }
        const sessions = [...rows.entries()]
            .map(([sessionId, info]) => ({ sessionId, ...info }))
            .sort((a, b) => (a.cwd ?? '').localeCompare(b.cwd ?? ''));
        return out(JSON.stringify({ total: sessions.length, sessions }, null, 2));
    });
    // 显式退役池会话: 外部主动释放常驻句柄(会话保留在持久化, 仍可凭 sessionId 续接)
    mcp.tool('session_close', '显式退役一个常驻池会话(dispose 句柄并移出池; 会话保留在持久化, 仍可凭 sessionId 续接)。只能关闭本插件池里的会话; live/persisted 会话归其创建者所有, 返回 no-op。', {
        sessionId: z.string().describe('要退役的会话 id(来自 session_list 或 agent_run 结果的 sessionId 字段)'),
    }, async ({ sessionId }) => {
        let targetCwd;
        for (const [cwd, rec] of liveAgents) {
            if (String(rec.sessionId) === sessionId) {
                targetCwd = cwd;
                break;
            }
        }
        if (targetCwd === undefined) {
            return out(JSON.stringify({ sessionId, closed: false, note: 'not in pool (pool only; live/persisted sessions are owned by their creator)' }));
        }
        const rec = liveAgents.get(targetCwd);
        if (!rec)
            return out(JSON.stringify({ sessionId, closed: false, note: 'already closed' }));
        // 忙会话不掐: 正在跑任务的会话拒绝退役, 引导先取消任务
        const status = rec.handle.agent.status;
        if (status !== 'idle') {
            return out(JSON.stringify({ sessionId, cwd: targetCwd, closed: false, note: 'busy: session has a running task; use task_list / task_cancel first' }));
        }
        liveAgents.delete(targetCwd);
        sessionToCwd.delete(sessionId);
        try {
            await rec.handle.dispose();
        }
        catch (e) {
            return err(JSON.stringify({ error: `dispose failed: ${e?.message ?? String(e)}` }));
        }
        return out(JSON.stringify({ sessionId, cwd: targetCwd, closed: true, note: 'session persisted; resumable by sessionId' }));
    });
    // 上下文压缩: 把会话早期历史压成一段模型摘要(走官方 ctx.compaction.compactNow; 需宿主加载 compaction 后端)
    mcp.tool('session_compact', '把会话的早期历史压缩成一段模型摘要(走 ctx.compaction 的 compactNow; 需宿主已加载 compaction 后端如 dsh-compaction-basic)。压缩后上下文占用大幅下降, 被替换的细节仍保留在持久化日志里。会话忙碌(正在跑任务)时返回 busy 错误。', {
        sessionId: z.string().describe('要压缩的会话 id(池/live/持久化均可; 非 live 会临时 resume, 压缩后释放)'),
    }, async ({ sessionId }) => {
        const engine = ctx.get('compaction');
        if (!engine?.compactNow) {
            return err(JSON.stringify({ error: 'compaction service unavailable (is dsh-compaction-basic loaded?)' }));
        }
        // 三级解析会话(池 → live → 持久化 resume); resume 出的句柄在结束后 flush+dispose
        let resolved;
        try {
            resolved = await getAgent(ctx, '', sessionId);
        }
        catch (e) {
            return err(JSON.stringify({ error: e?.message ?? String(e) }));
        }
        const agent = resolved.handle.agent;
        const agentCtx = {
            session: agent.session,
            options: { provider: runtimeConfig.provider, ...(runtimeConfig.model ? { model: runtimeConfig.model } : {}) },
            runMaintenance: (task) => agent.runMaintenance(task),
        };
        const before = await contextUsage(ctx, agent.session, agent);
        const controller = new AbortController();
        let timer;
        try {
            const result = await (runtimeConfig.taskTimeoutMs > 0
                ? Promise.race([
                    engine.compactNow(agentCtx, controller.signal),
                    new Promise((_resolve, reject) => {
                        timer = setTimeout(() => { controller.abort(); reject(TASK_TIMEOUT); }, runtimeConfig.taskTimeoutMs);
                    }),
                ])
                : engine.compactNow(agentCtx, controller.signal));
            const r = result;
            const summaryText = (r.summary ?? []).filter((b) => b.type === 'text' && b.text).map((b) => b.text).join('\n').slice(0, 2000);
            const after = await contextUsage(ctx, agent.session, agent);
            return out(JSON.stringify({
                ok: true, sessionId,
                compactionId: r.compactionId,
                summarySeq: r.summarySeq, endSeq: r.endSeq,
                shadowedNodes: r.shadowedSeqs?.length ?? 0,
                shadowedTokens: r.shadowedTokenCount,
                before, after,
                summary: summaryText,
            }, null, 2));
        }
        catch (e) {
            if (e === TASK_TIMEOUT) {
                return err(JSON.stringify({ error: `compaction timed out after ${runtimeConfig.taskTimeoutMs}ms` }));
            }
            const err2 = e;
            return err(JSON.stringify({
                error: `compact failed${err2.code ? ` (${err2.code})` : ''}: ${err2?.message ?? String(e)}`,
                busy: err2.name === 'ManualCompactionError' && err2.code === 'busy',
            }));
        }
        finally {
            if (timer !== undefined)
                clearTimeout(timer);
            if (resolved.disposeAfter) {
                try {
                    await ctx.get('sessions')?.flush?.(agent.session);
                }
                catch { /* ignore */ }
                try {
                    await resolved.handle.dispose();
                }
                catch { /* ignore */ }
            }
        }
    });
    // 感知: 列出等待输入的弹窗(审批/提问)。审批一律转达调用方, 用 prompt_respond 响应; 未响应前任务挂起。
    mcp.tool('pending_prompts', '列出当前等待输入的弹窗(审批/提问)。审批=权限审批待决策(approve/deny); 提问=agent 的澄清问题(自由文本回答)。可用 prompt_respond 响应; 未响应前任务保持挂起。', {
        sessionId: z.string().optional().describe('只列出该会话的弹窗(缺省: 全部 MCP 会话)'),
    }, async ({ sessionId }) => {
        const prompts = [];
        for (const pa of pendingApprovals.values()) {
            if (!sessionId || pa.agentId === sessionId) {
                prompts.push({ sessionId: pa.agentId, type: 'approval', id: pa.promptId, toolName: pa.toolName, ...(pa.reason !== undefined ? { reason: pa.reason } : {}) });
            }
        }
        for (const pq of pendingQuestions.values()) {
            if (!sessionId || pq.agentId === sessionId)
                prompts.push({ sessionId: pq.agentId, type: 'question', id: pq.promptId, questions: pq.questions });
        }
        // web GUI 持有提问 provider 时, MCP 会话里挂起的 ask_user_question 调用仍可感知(应答在 GUI)
        if (!questionsProviderOurs) {
            for (const sid of (sessionId ? [sessionId] : [...mcpSessionIds])) {
                const agent = liveAgentFor(ctx, sid);
                const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined;
                if (detected) {
                    prompts.push({ sessionId: sid, type: 'question', id: detected.id, questions: detected.questions, note: 'routed to the web GUI provider; answer in the DSH web UI' });
                }
            }
        }
        return out(JSON.stringify({ total: prompts.length, prompts }, null, 2));
    });
    // 响应: 解除等待中的弹窗。审批 approve→allowed-once(一次性授权) / deny→rejected; 提问→自由文本回答。
    mcp.tool('prompt_respond', '响应等待中的弹窗: 审批用 decision=approve|deny(approve 为一次性授权, 绝不自动放行——每次审批都必须显式决策); 提问用 answer 自由文本。响应后 agent 解除阻塞继续执行。', {
        sessionId: z.string().describe('弹窗所属会话 id'),
        promptId: z.string().describe('pending_prompts / progress 返回的 prompt id'),
        decision: z.enum(['approve', 'deny']).optional().describe('审批类弹窗的决策(approve=放行一次, deny=拒绝)'),
        answer: z.string().optional().describe('提问类弹窗的自由文本回答'),
    }, async ({ sessionId, promptId, decision, answer }) => {
        const pa = pendingApprovals.get(promptId);
        if (pa !== undefined) {
            if (pa.agentId !== sessionId)
                return err(JSON.stringify({ error: `prompt ${promptId} belongs to session ${pa.agentId}, not ${sessionId}` }));
            if (decision !== 'approve' && decision !== 'deny') {
                return err(JSON.stringify({ error: 'approval prompts require decision=approve|deny' }));
            }
            const outcome = decision === 'approve' ? 'allowed-once' : 'rejected';
            pa.resolve(outcome);
            return out(JSON.stringify({ ok: true, promptId, type: 'approval', resolved: outcome }, null, 2));
        }
        const pq = pendingQuestions.get(promptId);
        if (pq !== undefined) {
            if (pq.agentId !== sessionId)
                return err(JSON.stringify({ error: `prompt ${promptId} belongs to session ${pq.agentId}, not ${sessionId}` }));
            if (answer === undefined || answer === '')
                return err(JSON.stringify({ error: 'question prompts require answer text' }));
            const answered = pq.questions.map((q) => ({ id: q.id, selected: [], custom: answer }));
            pq.resolve({ answers: answered });
            return out(JSON.stringify({ ok: true, promptId, type: 'question', answered: answered.length }, null, 2));
        }
        // 未挂起: 若是 GUI 路由的挂起提问则给出明确指引
        const agent = liveAgentFor(ctx, sessionId);
        const detected = agent !== undefined ? detectPendingAskUser(agent.session) : undefined;
        if (detected !== undefined && detected.id === promptId && !questionsProviderOurs) {
            return err(JSON.stringify({ error: 'this question is routed to the web GUI provider; answer it in the DSH web UI (the MCP-side provider slot is owned by the GUI in this deployment)' }));
        }
        return err(JSON.stringify({ error: `prompt not found: ${promptId}` }));
    });
    // 切换会话模型: 改 agent.options.model(agent-loop 每轮 buildRequest 实时读取), 下个 turn 生效;
    // 持久化会话临时 resume 并记录会话级覆盖, 后续 resume 同样生效
    mcp.tool('session_set_model', '给指定会话切换模型(改 agent.options.model, 下一个 turn 生效, 不打断当前执行)。池/live 直改; 持久化会话临时 resume 并记录覆盖(之后 resume 仍生效)。模型 id 参考 model_list(deepseek-v4-flash/pro、glm-5.3/flash 等)。', {
        sessionId: z.string().describe('会话 id(池/live/持久化均可)'),
        model: z.string().describe('目标模型 id'),
        provider: z.string().optional().describe('目标 provider 路由(缺省保持当前)'),
    }, async ({ sessionId, model, provider }) => {
        let resolved;
        try {
            resolved = await getAgent(ctx, '', sessionId);
        }
        catch (e) {
            return err(JSON.stringify({ error: e?.message ?? String(e) }));
        }
        const agent = resolved.handle.agent;
        const opts = agent.options;
        const old = { provider: opts?.provider, model: opts?.model };
        if (opts) {
            if (provider !== undefined)
                opts.provider = provider;
            opts.model = model;
        }
        sessionModelOverrides.set(sessionId, { provider: provider ?? old.provider, model });
        if (resolved.disposeAfter) {
            try {
                await ctx.get('sessions')?.flush?.(agent.session);
            }
            catch { /* ignore */ }
            try {
                await resolved.handle.dispose();
            }
            catch { /* ignore */ }
        }
        return out(JSON.stringify({
            ok: true, sessionId,
            oldModel: old.model ?? '(unset)', newModel: model,
            oldProvider: old.provider ?? '(unset)', newProvider: provider ?? old.provider ?? '(unset)',
            note: 'takes effect from the next turn; recorded as session override for future resumes',
        }, null, 2));
    });
    // 向运行中会话插入补充指令(steering): 走 DSH agent.inbox(append, 持久化), 下个 turn/step 边界读取, 不打断当前工具
    mcp.tool('session_inject', '向指定会话的 agent 队列插入一条补充指令/上下文(steering 消息): 下个 turn 边界处理, 不打断当前正在执行的工具(参考 DSH agent.inbox / agent/inbox/spliced)。正在执行的任务会在下一步读到; 空闲会话的消息排队等待下个任务。', {
        sessionId: z.string().describe('会话 id(池/live/持久化均可)'),
        message: z.string().describe('要插入的补充指令/上下文文本'),
        target: z.enum(['next-turn', 'next-step']).optional().describe('插入位置(默认 next-turn = 队尾)'),
    }, async ({ sessionId, message, target }) => {
        let resolved;
        try {
            resolved = await getAgent(ctx, '', sessionId);
        }
        catch (e) {
            return err(JSON.stringify({ error: e?.message ?? String(e) }));
        }
        const agent = resolved.handle.agent;
        const inbox = agent.inbox;
        if (!inbox?.append) {
            if (resolved.disposeAfter) {
                try {
                    await resolved.handle.dispose();
                }
                catch { /* ignore */ }
            }
            return err(JSON.stringify({ error: 'agent inbox unavailable' }));
        }
        try {
            const msg = createUserMessage({ content: [{ type: 'text', text: message }], source: { kind: 'plugin', plugin: 'harness-mcp-server' } });
            inbox.append(target ?? 'next-turn', msg);
        }
        catch (e) {
            if (resolved.disposeAfter) {
                try {
                    await resolved.handle.dispose();
                }
                catch { /* ignore */ }
            }
            return err(JSON.stringify({ error: `inject failed: ${e?.message ?? String(e)}` }));
        }
        if (resolved.disposeAfter) {
            try {
                await ctx.get('sessions')?.flush?.(agent.session);
            }
            catch { /* ignore */ }
            try {
                await resolved.handle.dispose();
            }
            catch { /* ignore */ }
        }
        return out(JSON.stringify({
            ok: true, sessionId, target: target ?? 'next-turn',
            note: 'queued; processed at the next turn/step boundary without interrupting the current tool',
        }, null, 2));
    });
    // 手动归组补给站: 官方 UI 没有"移动会话到工作区"功能, 本工具供随时归组
    mcp.tool('attach_session', '把会话归组到工作区(补给站: 官方 UI 无移动会话功能)。path 缺省用该会话 header 的 cwd; 归组依赖官方 attachSession 的强校验——realpath(header.cwd) 必须与工作区路径精确相等, 不匹配会返回官方报错。', {
        sessionId: z.string().describe('要归组的会话 id(live 或已持久化)'),
        path: z.string().optional().describe('目标工作区目录(缺省: 会话 header 的 cwd)'),
    }, async ({ sessionId, path }) => {
        const sid = SessionId(sessionId);
        const header = await findSessionHeader(ctx, sid);
        if (header === undefined) {
            return err(JSON.stringify({ error: `session not found: ${sessionId}(live 与持久化里都没有)` }));
        }
        const target = path ?? header.cwd;
        if (target === undefined) {
            return err(JSON.stringify({ error: `session ${sessionId} 的 header 没有 cwd, 官方 attachSession 无法校验, 不能归组` }));
        }
        try {
            const canonical = await realpath(target); // 目标必须是存在的目录, 否则 ENOENT
            // 白名单一致化: 配置了 workspaceRoots 时, 归组目标同样受目录白名单约束
            if (!cwdAllowed(canonical)) {
                return err(JSON.stringify({ error: `path not allowed (outside workspaceRoots): ${canonical}` }));
            }
            const ws = await ensureWorkspace(ctx, canonical);
            if (!ws?.attachSession)
                return err(JSON.stringify({ error: 'workspaceRegistry unavailable' }));
            if (ws.sessionIds.includes(sid)) {
                return out(JSON.stringify({ sessionId, workspaceId: ws.id, workspacePath: ws.path, attached: false, note: 'already attached' }));
            }
            await ws.attachSession(sid);
            return out(JSON.stringify({ sessionId, workspaceId: ws.id, workspacePath: ws.path, attached: true }));
        }
        catch (e) {
            return err(JSON.stringify({ error: `attach failed: ${e?.message ?? String(e)}` }));
        }
    });
}
/**
 * 插件入口: 启动 MCP server(StreamableHTTP, 跨网), 通过 ctx 桥接 Harness 能力。
 */
export async function apply(ctx, config = {}) {
    // 初始化运行时配置(覆盖默认值)
    if (config.provider)
        runtimeConfig.provider = config.provider;
    if (config.model)
        runtimeConfig.model = config.model;
    if (config.preset)
        runtimeConfig.preset = config.preset;
    if (config.maxQueue !== undefined)
        runtimeConfig.maxQueue = config.maxQueue;
    if (config.taskTtlMs !== undefined)
        runtimeConfig.taskTtlMs = config.taskTtlMs;
    if (config.maxAgents !== undefined)
        runtimeConfig.maxAgents = config.maxAgents;
    if (config.taskTimeoutMs !== undefined)
        runtimeConfig.taskTimeoutMs = config.taskTimeoutMs;
    if (config.authToken)
        runtimeConfig.authToken = config.authToken;
    if (config.workspaceRoots)
        runtimeConfig.workspaceRoots = config.workspaceRoots;
    const port = config.port ?? 8090;
    // 安全默认: 仅监听本机。暴露公网/局域网前必须自行加认证+反代+TLS(见 README 警告)
    const host = config.host ?? '127.0.0.1';
    console.log('[harness-mcp-server] apply called, port=', port);
    // ── 审批应答者: MCP 会话的审批弹窗一律转达调用方, 绝不自动放行 ──
    // prepend 抢在 web GUI 应答者之前认领 MCP 会话的审批; 非 MCP 会话 next() 交给 GUI 应答链。
    // approve → 'allowed-once'(一次性授权), deny → 'rejected', 任务取消/超时 → signal abort → 'cancelled'。
    const onApprovalRequest = (req, next) => {
        if (req.signal?.aborted)
            return Promise.resolve('cancelled');
        // 防御: agent.id 为权威; 个别实现只挂 session.id 时兜底
        const agentId = String(req.agent.id ?? req.agent.session?.id);
        if (!mcpSessionIds.has(agentId))
            return next();
        const promptId = approvalPromptIdOf(req);
        return new Promise((resolve) => {
            let settled = false;
            const settle = (outcome) => {
                if (settled)
                    return;
                settled = true;
                pendingApprovals.delete(promptId);
                req.signal?.removeEventListener('abort', onAbort);
                resolve(outcome);
                // 【2】web UI 提示(入队, 工具完成后安全落点): 弹窗已被 MCP 响应
                queuePromptNotice(req.agent, `✅ 审批 ${promptId} 已由 MCP 侧响应: ${outcome}`, `MCP 响应审批: ${outcome}`);
            };
            const onAbort = () => settle('cancelled');
            pendingApprovals.set(promptId, { promptId, agentId, toolName: req.toolName, reason: req.reason, resolve: settle });
            req.signal?.addEventListener('abort', onAbort, { once: true });
            // 【2】web UI 提示(入队, 工具完成后安全落点): 该审批已被 MCP 拦截接管
            queuePromptNotice(req.agent, `⏳ 该审批（${req.toolName}${req.reason !== undefined ? `：${req.reason}` : ''}）已由 MCP 侧接管处理中，请在 Hermes/客户端响应（prompt ${promptId}）`, `MCP 接管审批: ${req.toolName}`);
        });
    };
    ctx.on('approval/request', onApprovalRequest, { prepend: true });
    ctx.on('tools/post-execute', async (exec, _result, next) => {
        const downstream = await next();
        return flushPromptNotices(exec.agent, downstream);
    });
    // ── 提问 provider: 单槽能力缝; web GUI 已占用时降级(提问路由到 GUI, MCP 仍可感知但需在 GUI 应答) ──
    const userQuestions = ctx.get('userQuestions');
    if (userQuestions?.registerProvider) {
        try {
            userQuestions.registerProvider({
                ask: (request) => {
                    const r = request;
                    const promptId = `q-${randomUUID()}`;
                    return new Promise((resolve, reject) => {
                        let settled = false;
                        const settle = (fn) => {
                            if (settled)
                                return;
                            settled = true;
                            pendingQuestions.delete(promptId);
                            r.signal?.removeEventListener('abort', onAbort);
                            fn();
                        };
                        const onAbort = () => settle(() => reject(new Error('ask_user_question was aborted before the user answered')));
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
                                if (r.agent !== undefined)
                                    queuePromptNotice(r.agent, `✅ 提问 ${promptId} 已由 MCP 侧回答`, 'MCP 回答提问');
                                resolve(answer);
                            }),
                            reject: (e) => settle(() => {
                                if (r.agent !== undefined)
                                    queuePromptNotice(r.agent, `❌ 提问 ${promptId} 已取消/失败: ${e?.message ?? String(e)}`, 'MCP 取消提问');
                                reject(e);
                            }),
                        });
                        r.signal?.addEventListener('abort', onAbort, { once: true });
                        // 【2】web UI 提示(入队, 工具完成后安全落点): 该提问已被 MCP 拦截接管
                        if (r.agent !== undefined) {
                            const first = r.questions[0];
                            queuePromptNotice(r.agent, `⏳ 该提问（${first?.question ?? '…'}）已由 MCP 侧接管处理中，请在 Hermes/客户端响应（prompt ${promptId}）`, 'MCP 接管提问');
                        }
                    });
                },
            });
            questionsProviderOurs = true;
            console.log('[harness-mcp-server] user-questions provider registered (question prompts answerable via prompt_respond)');
        }
        catch {
            questionsProviderOurs = false;
            console.warn('[harness-mcp-server] user-questions provider already registered (web GUI); question prompts route to the GUI and remain visible via progress/pending_prompts');
        }
    }
    const servers = new Map();
    const transports = new Map();
    const server = http.createServer(async (req, res) => {
        // Bearer token 认证(配置了 authToken 时强制所有请求校验)
        if (runtimeConfig.authToken) {
            const auth = req.headers['authorization'];
            if (auth !== `Bearer ${runtimeConfig.authToken}`) {
                res.writeHead(401, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32001, message: 'Unauthorized' }, id: null }));
                return;
            }
        }
        const sessionId = req.headers['mcp-session-id'] ?? undefined;
        const existing = sessionId ? transports.get(sessionId) : undefined;
        // 已有 session: GET/POST/DELETE 都路由到对应 transport(支持 SSE 流 + 会话终止)
        if (existing) {
            if (req.method === 'GET' || req.method === 'POST' || req.method === 'DELETE') {
                await existing.handleRequest(req, res);
                return;
            }
            res.writeHead(405, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32600, message: 'Method not allowed' }, id: null }));
            return;
        }
        // 新 session 初始化(仅 POST 且无 session id)
        if (req.method === 'POST' && !sessionId) {
            const mcp = new McpServer({ name: 'harness', version: VERSION });
            registerTools(mcp, ctx);
            const transport = new StreamableHTTPServerTransport({
                sessionIdGenerator: () => randomUUID(),
                onsessioninitialized: (sid) => {
                    transports.set(sid, transport);
                    servers.set(sid, mcp);
                },
            });
            // 会话关闭时清理映射(避免临时 key 泄漏 + 无效会话累积)
            transport.onclose = () => {
                const sid = transport.sessionId;
                if (sid) {
                    transports.delete(sid);
                    servers.delete(sid);
                }
            };
            await mcp.connect(transport);
            await transport.handleRequest(req, res);
            return;
        }
        // 未知 session → 404(不新建 transport, 避免遗留对象)
        if (sessionId) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32001, message: 'Session not found' }, id: null }));
            return;
        }
        // 无 session 的非初始化请求 → 400
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', error: { code: -32600, message: 'Invalid request' }, id: null }));
    });
    server.listen(port, host, () => {
        console.log(`[harness-mcp-server] MCP server listening on ${host}:${port}`);
    });
    server.on('error', (e) => {
        console.error('[harness-mcp-server] HTTP server error:', e.message);
    });
    // 存量捞回: 启动后异步补挂未分组会话, 不阻塞启动; 全程兜底 try/catch 防 unhandled rejection
    void (async () => {
        try {
            const r = await reattachOrphanSessions(ctx);
            console.log(`[harness-mcp-server] 存量捞回完成: attached=${r.attached} failed=${r.failed}`);
        }
        catch (e) {
            console.warn('[harness-mcp-server] 存量捞回异常:', e?.message ?? e);
        }
    })();
    // 标准 cordis 生命周期: 用 ctx.effect 注册清理(卸载时关 server + 清空全部映射/会话/队列)
    ctx.effect(() => {
        return () => {
            server.close();
            transports.clear();
            servers.clear();
            liveAgents.clear();
            sessionToCwd.clear();
            agentLocks.clear();
            taskQueue.clear();
            taskCancelHooks.clear();
            mcpSessionIds.clear();
            sessionModelOverrides.clear();
            pendingNotices.clear();
            for (const pa of pendingApprovals.values())
                pa.resolve('cancelled');
            pendingApprovals.clear();
            for (const pq of pendingQuestions.values())
                pq.reject(new Error('harness-mcp-server unloaded'));
            pendingQuestions.clear();
        };
    }, 'harness-mcp-server');
}
