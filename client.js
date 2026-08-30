window.__ModuleLoader__.load({
	id: "@chushixixin/dsh-harness-mcp-server",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		var React = require("react");
		var e = React.createElement;

		// ── 设置页: 「MCP Server」独立主配置页(settings.section) ──
		// Host 半区注册 settings 命名空间 'harness-mcp-server'(host/port/authToken),
		// 本页以独立分区挂进设置导航; 写入经 settings scope(revision 设栅), 保存即热生效。
		// 暂存式表单: 草稿只在点「保存」时写; 每字段可单独「重置」回组合层(入口 config/patch)。
		var NAMESPACE = "harness-mcp-server";
		var PAGE_ORDER = 25;
		var FIELDS = [
			{ field: "host", label: "监听地址 (host)", password: false, hint: "127.0.0.1 = 仅本机(默认); 0.0.0.0 = 本机所有网卡(暴露局域网, 建议同时启用 token)" },
			{ field: "port", label: "端口 (port)", password: false, hint: "1-65535 整数, 默认 8090" },
			{ field: "authToken", label: "Bearer Token (authToken)", password: true, secret: true, hint: "留空 = 无认证(默认, 仅本机时安全); 非空 = 所有 MCP 请求须带 Authorization: Bearer <token>" },
		];

		function parseValue(spec, text) {
			if (spec.field === "port") {
				var t = text.trim();
				if (!/^\d+$/.test(t)) return undefined;
				var n = parseInt(t, 10);
				if (!(n >= 1 && n <= 65535)) return undefined;
				return n;
			}
			return text;
		}

		function formatValue(spec, value) {
			if (value === undefined || value === null) return "";
			return String(value);
		}

		var ScopeContext = React.createContext(undefined);

		function useScopeSnapshot(scope) {
			var [snap, setSnap] = React.useState(function () { return scope ? scope.getSnapshot() : undefined; });
			React.useEffect(function () {
				if (scope === undefined) return undefined;
				setSnap(scope.getSnapshot());
				return scope.subscribe(function () { setSnap(scope.getSnapshot()); });
			}, [scope]);
			return snap;
		}

		var INPUT_STYLE = {
			width: "100%", maxWidth: 480, boxSizing: "border-box", fontSize: 12,
			padding: "5px 10px", borderRadius: 6,
			border: "1px solid var(--dsh-border, rgba(127,127,127,0.35))",
			background: "transparent", color: "inherit",
		};
		var BTN_STYLE = {
			fontSize: 12, padding: "3px 12px", borderRadius: 6,
			border: "1px solid var(--dsh-border, rgba(127,127,127,0.35))",
			background: "transparent", color: "inherit",
		};

		function McpServerCard() {
			var scope = React.useContext(ScopeContext);
			var snapshot = useScopeSnapshot(scope);
			var [drafts, setDrafts] = React.useState({});
			var [saving, setSaving] = React.useState(false);
			var [failed, setFailed] = React.useState(false);
			var [showSecret, setShowSecret] = React.useState(false);
			var [copied, setCopied] = React.useState("");

			var ready = snapshot !== undefined && snapshot.status === "ready";
			var writable = ready && snapshot.writable;

			var stage = function (field, text) {
				setDrafts(function (d) {
					var next = Object.assign({}, d);
					next[field] = text;
					return next;
				});
				setFailed(false);
			};

			var stagedWrites = function () {
				if (snapshot === undefined || snapshot.value === undefined) return [];
				var writes = [];
				for (var i = 0; i < FIELDS.length; i++) {
					var spec = FIELDS[i];
					var draft = drafts[spec.field];
					if (draft === undefined) continue;
					if (draft === formatValue(spec, snapshot.value[spec.field])) continue;
					var parsed = parseValue(spec, draft);
					if (parsed === undefined) return undefined; // 非法草稿阻塞保存
					writes.push({ field: spec.field, value: parsed });
				}
				return writes;
			};

			var invalid = (function () {
				if (snapshot === undefined || snapshot.value === undefined) return false;
				for (var i = 0; i < FIELDS.length; i++) {
					var spec = FIELDS[i];
					var draft = drafts[spec.field];
					if (draft === undefined) continue;
					if (draft === formatValue(spec, snapshot.value[spec.field])) continue;
					if (parseValue(spec, draft) === undefined) return true;
				}
				return false;
			})();

			var writes0 = stagedWrites();
			var dirty = writes0 === undefined ? true : writes0.length > 0;

			var save = function () {
				var writes = stagedWrites();
				if (scope === undefined || snapshot === undefined || writes === undefined || writes.length === 0) return;
				setSaving(true);
				setFailed(false);
				var p = Promise.resolve();
				writes.forEach(function (w) {
					p = p.then(function () {
						return scope.set(w.field, w.value);
					});
				});
				p.then(
					function () {
						setSaving(false);
						setDrafts({});
					},
					function () {
						setSaving(false);
						setFailed(true);
					},
				);
			};

			var resetField = function (field) {
				if (scope === undefined) return;
				setSaving(true);
				scope.unset(field).then(
					function () {
						setSaving(false);
						setDrafts(function (d) {
							var next = Object.assign({}, d);
							delete next[field];
							return next;
						});
					},
					function () {
						setSaving(false);
						setFailed(true);
					},
				);
			};

			if (!ready) {
				return e("div", { style: { padding: "8px 0", opacity: 0.6 } }, "设置命名空间尚未就绪(等待宿主应答)…");
			}

			var value = snapshot.value || {};
			var user = snapshot.user || {};
			var authOn = !!(value.authToken && value.authToken.length > 0);
			var rows = FIELDS.map(function (spec) {
				var draft = drafts[spec.field];
				var text = draft !== undefined ? draft : formatValue(spec, value[spec.field]);
				var overridden = Object.prototype.hasOwnProperty.call(user, spec.field);
				var inputType = spec.secret ? (showSecret ? "text" : "password") : "text";
				var inputRow = e("div", { style: { display: "flex", gap: 6, alignItems: "stretch" } },
					e("input", {
						type: inputType,
						value: text,
						spellCheck: false,
						onChange: function (ev) { stage(spec.field, ev.target.value); },
						disabled: !writable || saving,
						style: Object.assign({}, INPUT_STYLE, { maxWidth: spec.secret ? 340 : 480 }),
					}),
					spec.secret ? e("button", {
						title: showSecret ? "隐藏 token" : "显示 token",
						onClick: function () { setShowSecret(function (v) { return !v; }); },
						style: Object.assign({}, BTN_STYLE, { padding: "3px 8px", cursor: "pointer", whiteSpace: "nowrap" }),
					}, showSecret ? "隐藏" : "显示") : null,
					spec.secret ? e("button", {
						title: "复制当前生效的 token",
						onClick: function () {
							var token = String(value.authToken ?? "");
							if (!token) { setCopied("empty"); return; }
							var done = function () { setCopied("ok"); };
							var fail = function () { setCopied("fail"); };
							if (navigator.clipboard && navigator.clipboard.writeText) {
								navigator.clipboard.writeText(token).then(done, function () {
									try {
										// 剪贴板 API 不可用时回退: 临时挂载 textarea 选中复制
										var tmp = document.createElement("textarea");
										tmp.value = token;
										tmp.style.position = "fixed";
										tmp.style.opacity = "0";
										document.body.appendChild(tmp);
										tmp.select();
										var ok = document.execCommand("copy");
										document.body.removeChild(tmp);
										(ok ? done : fail)();
									} catch (err) { fail(); }
								});
							} else {
								fail();
							}
						},
						style: Object.assign({}, BTN_STYLE, { padding: "3px 8px", cursor: "pointer", whiteSpace: "nowrap" }),
					}, "复制") : null,
				);
				return e("div", { key: spec.field, style: { marginBottom: 14 } },
					e("label", { style: { display: "block", fontSize: 12, fontWeight: 600, marginBottom: 3 } },
						spec.label,
						overridden ? e("span", { style: { marginLeft: 6, fontSize: 11, opacity: 0.65, fontWeight: 400 } }, "(已覆盖)") : null,
					),
					inputRow,
					e("div", { style: { fontSize: 11, opacity: 0.55, marginTop: 3 } },
						spec.secret && copied === "ok" ? e("span", { style: { opacity: 1, color: "inherit", fontWeight: 600 } }, "已复制到剪贴板 · ") : null,
						spec.secret && copied === "fail" ? e("span", null, "复制失败, 请手动选择复制 · ") : null,
						spec.secret && copied === "empty" ? e("span", null, "当前无 token(未启用认证) · ") : null,
						spec.hint,
						overridden ? e("button", {
							onClick: function () { resetField(spec.field); },
							disabled: !writable || saving,
							style: { fontSize: 11, marginLeft: 8, padding: "0 6px", cursor: "pointer", background: "transparent", color: "inherit", border: "1px solid var(--dsh-border, rgba(127,127,127,0.35))", borderRadius: 4 },
						}, "重置") : null,
					),
				);
			});

			return e("div", { style: { fontSize: 12, lineHeight: 1.6, maxWidth: 560 } },
				e("div", { style: { marginBottom: 12, opacity: 0.75 } },
					"当前生效: ", String(value.host), ":", String(value.port),
					" · 鉴权: ", authOn ? "已启用" : "未启用",
					!writable ? " · (当前文档只读)" : "",
				),
				rows,
				e("div", { style: { display: "flex", gap: 8, alignItems: "center", marginTop: 4 } },
					e("button", {
						onClick: save,
						disabled: !writable || saving || !dirty || invalid,
						style: Object.assign({}, BTN_STYLE, { cursor: !writable || saving || !dirty || invalid ? "default" : "pointer" }),
					}, saving ? "保存中…" : "保存"),
					dirty && !saving ? e("button", {
						onClick: function () { setDrafts({}); setFailed(false); },
						style: Object.assign({}, BTN_STYLE, { border: "none", cursor: "pointer", opacity: 0.7 }),
					}, "放弃修改") : null,
					invalid ? e("span", { style: { opacity: 0.7 } }, "有非法草稿(port 须为 1-65535 整数)") : null,
					failed ? e("span", { style: { opacity: 0.7 } }, "上次保存未生效, 已回读宿主值") : null,
				),
				e("div", { style: { marginTop: 14, fontSize: 11, opacity: 0.5 } },
					"保存即时生效(热重绑监听, 不断开已建立的 MCP 会话)。多 token 可经入口配置 authTokens 数组追加; 本页 authToken 与其并存校验。",
				),
			);
		}

		function CardRoot(props) {
			return e(ScopeContext.Provider, { value: props.scope }, e(McpServerCard));
		}

		var apply = function (ctx) {
			// Host 半区注册命名空间后, 浏览器半区经 settingsScope binder 绑定同名 scope
			var binder = ctx.get("settingsScope");
			if (binder === undefined) return;
			var scope = binder.bind({ namespace: NAMESPACE });
			var slots = ctx.get("slots");
			if (slots === undefined) return;
			// 独立主配置页: 设置导航里的「MCP Server」分区
			slots.inject("settings.section", function () {
				slots.register(
					{ name: "settings.section", id: "harness-mcp-server", order: PAGE_ORDER, label: "MCP Server" },
					function () { return e(CardRoot, { scope: scope }); },
				);
			});
		};

		exports.apply = apply;
		return module.exports;
	}
});
