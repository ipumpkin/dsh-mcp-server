window.__ModuleLoader__.load({
	id: "@ipumpkin/dsh-mcp-server",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
		var React = require("react");
		var e = React.createElement;
		var primitives = require("@deepseek-ai/dsh-client-ui-primitives");

		// ── 设置页: 设置导航里的独立「MCP Server」整页(settings.section) ──
		// Host 半区注册 settings 命名空间 'harness-mcp-server'(host/port/authToken),
		// 本页经 ctx.configForms.get(命名空间) 绑定共享表单, 保存即热生效。
		//
		// dsh 0.2.0 兼容: 旧的 settingsScope binder 已移除, 改为 ui-settings 提供的
		// configForms 服务 + primitives 的 SettingsFormModel/SettingsForm/SettingsValueField。
		// 整页注册进 settings.section(形状同 ui-agent-preset / ui-settings-account 的注册),
		// 该槽不传 view/form, 分节组件自绘标题并自建表单; plugins.item 只是官方插件在
		// 「插件」页里的卡片列表槽, 整页不走它。
		// 并用 configForms.whileServed 保证宿主未提供命名空间时不注册该分区。
		var NAMESPACE = "harness-mcp-server";
		/** 本页字典命名空间。 */
		var NS = "settings.harness-mcp-server";
		/** 客户端半区硬依赖服务(cordis inject 等待语义)。 */
		var INJECT = ["slots", "locale", "configForms"];
		var PAGE_ORDER = 25;
		/** authToken 的字段名与单字段写入路径。 */
		var AUTH_TOKEN_FIELD = "authToken";

		/** 英文文案。 */
		var en = {
			title: "MCP Server",
			description: "Expose Harness agent capabilities over a local MCP server (host, port, bearer token).",
			host: "Listen host",
			hostHint: "127.0.0.1 = loopback only (default); 0.0.0.0 = all interfaces (exposes the LAN, enable a token).",
			port: "Port",
			portHint: "Integer 1-65535, default 8090.",
			authToken: "Bearer token",
			authTokenHint: "Stored outside the settings file; leave blank to keep the current token. A configured token requires Authorization: Bearer <token> on every MCP request.",
			authTokenSet: "A token is configured.",
			authTokenUnset: "No token is configured.",
			overridden: "Overridden",
			reset: "Reset to default",
			invalid: "Enter a valid value, or leave blank to use the default.",
			readOnly: "This deployment stores settings read-only.",
			unavailable: "This plugin is not loaded, so it cannot be configured right now.",
			save: "Save",
			saving: "Saving…",
			saveFailed: "The deployment did not accept these values; they were left for you to correct.",
		};
		/** 简体中文文案。 */
		var zh = {
			title: "MCP Server",
			description: "经本地 MCP server 暴露 Harness 能力(监听地址、端口、Bearer token)。",
			host: "监听地址 (host)",
			hostHint: "127.0.0.1 = 仅本机(默认); 0.0.0.0 = 本机所有网卡(暴露局域网, 建议同时启用 token)。",
			port: "端口 (port)",
			portHint: "1-65535 整数, 默认 8090。",
			authToken: "Bearer Token (authToken)",
			authTokenHint: "不写入设置文件; 留空表示保持当前 token(空草稿不会清除已配置的 token)。已配置 token 时, 所有 MCP 请求须带 Authorization: Bearer <token>。",
			authTokenSet: "已配置 token。",
			authTokenUnset: "未配置 token。",
			overridden: "已覆盖",
			reset: "恢复默认",
			invalid: "请填合法值; 留空表示使用默认值。",
			readOnly: "本部署的设置为只读。",
			unavailable: "该插件当前未加载, 暂时无法配置。",
			save: "保存",
			saving: "保存中…",
			saveFailed: "本部署没有接受这些值, 已保留供你修改。",
		};

		/**
		 * 共享设置表单渲染的文案。
		 * @param t - 本页字典读取器。
		 * @returns SettingsForm 需要的标签。
		 */
		function formLabels(t) {
			return {
				unavailable: t("unavailable"),
				readOnly: t("readOnly"),
				saveFailed: t("saveFailed"),
				save: t("save"),
				saving: t("saving"),
			};
		}

		/** 整页标题样式。 */
		var SECTION_TITLE_STYLE = { margin: "0 0 4px", fontSize: 14, fontWeight: 600 };
		/** 整页说明行样式。 */
		var SECTION_DESC_STYLE = { margin: "0 0 14px", maxWidth: 560, fontSize: 12, opacity: 0.7 };

		/**
		 * 渲染「MCP Server」设置整页: 自绘标题与说明行, 后接设置表单。
		 * settings.section 的 owner props 不含 view(那是 plugins.item 卡片列表用的),
		 * 因此没有摘要分支, 本页恒为表单页。
		 * @param props - 文案、表单快照与其动作。
		 * @returns 标题、说明行与表单。
		 */
		function McpServerSection(props) {
			var t = props.t;
			var state = props.useMcpServerSection(function (snapshot) { return snapshot; });
			var disabled = !state.writable;
			function field(id, key, label, hint, numeric) {
				var st = state[key];
				return e(primitives.SettingsValueField, {
					id: "harness-mcp-" + id,
					label: label,
					hint: hint,
					numeric: numeric === true,
					overriddenLabel: t("overridden"),
					resetLabel: t("reset"),
					invalidLabel: t("invalid"),
					disabled: disabled,
					text: st.text,
					overridden: st.overridden,
					invalid: st.invalid,
					onEdit: function (text) { props.edit(key, text); },
					onReset: function () { props.resetField(key); },
				});
			}
			return e("div", { style: { fontSize: 12, lineHeight: 1.6 } },
				e("h2", { style: SECTION_TITLE_STYLE }, t("title")),
				e("p", { style: SECTION_DESC_STYLE }, t("description")),
				e(primitives.SettingsForm, {
					labels: formLabels(t),
					state: state,
					onSave: props.save,
					onDiscard: props.discard,
				},
					field("host", "host", t("host"), t("hostHint"), false),
					field("port", "port", t("port"), t("portHint"), true),
					// authToken 是只写密钥控件: 值不回传(role('secret') 被 redact), 草稿恒从空白开始,
					// 空草稿不写 ⇒ 不会抹掉已配置的 token; configured 取自 namespace 的 secrets 边车。
					e(primitives.SettingsSecretField, {
						id: "harness-mcp-authToken",
						label: t("authToken"),
						hint: t("authTokenHint"),
						configured: state.authTokenConfigured,
						stateLabel: state.authTokenConfigured ? t("authTokenSet") : t("authTokenUnset"),
						text: state.authToken.text,
						disabled: disabled,
						onEdit: function (text) { props.edit(AUTH_TOKEN_FIELD, text); },
					}),
				),
			);
		}

		/** 「MCP Server」整页的暂存式表单, 绑定到 harness-mcp-server 命名空间。 */
		var McpServerSectionController = class {
			/**
			 * @param scope - 该命名空间的共享配置表单(SettingsFormScope: getSnapshot/subscribe/mutate)。
			 * @param describe - settings 描述镜像, 读取 namespace 的 secrets 边车以判断 token 是否已配置。
			 */
			constructor(scope, describe) {
				this.describe = describe;
				this.form = new primitives.SettingsFormModel(scope, [
					primitives.settingsTextField("host"),
					primitives.settingsNumberField("port"),
				], [
					// authToken 是只写密钥控件: 草稿为空时 SettingsFormModel 不产生写入(保持现值);
					// 写入只发单字段 path set —— 整节 replace/write 会把被 redact 的其它字段抹平。
					{
						field: AUTH_TOKEN_FIELD,
						write: function (text) {
							return scope.mutate([{ op: "set", path: [AUTH_TOKEN_FIELD], value: text }]);
						},
					},
				]);
				this.store = this.form.bind(() => this.projection());
				// secrets 边车随 settings 描述镜像更新(写入答案被折叠进镜像), 订阅以刷新「已配置」徽标。
				this.unsubscribe = describe.subscribe(() => { this.store.set(this.projection()); });
			}
			/** authToken 是否已配置: secrets 边车中 path=['authToken'] 的 set。 */
			tokenConfigured() {
				var view = this.describe.getSnapshot().view;
				if (view === undefined) return false;
				for (var i = 0; i < view.namespaces.length; i++) {
					var ns = view.namespaces[i];
					if (ns.ns !== NAMESPACE) continue;
					var slots = ns.secrets || [];
					for (var j = 0; j < slots.length; j++) {
						if (slots[j].path.length === 1 && slots[j].path[0] === AUTH_TOKEN_FIELD) {
							return slots[j].set === true;
						}
					}
					return false;
				}
				return false;
			}
			projection() {
				return {
					...this.form.shell(),
					host: this.form.field("host"),
					port: this.form.field("port"),
					authToken: this.form.field(AUTH_TOKEN_FIELD),
					authTokenConfigured: this.tokenConfigured(),
				};
			}
			/** 构造槽位注册注入的面。 */
			inject() {
				return { hooks: { mcpServerSection: this.store }, ...this.form.actions() };
			}
			/** 释放表单订阅。 */
			dispose() {
				this.unsubscribe();
				this.form.dispose();
			}
		};

		/**
		 * 挂载设置页。
		 * @param ctx - 浏览器插件上下文。
		 */
		function apply(ctx) {
			var t = ctx.locale.bind(NS);
			ctx.effect(() => ctx.locale.register(NS, { zh, en }), "harness-mcp-server: dictionaries");
			var controller = new McpServerSectionController(
				ctx.configForms.get(NAMESPACE),
				ctx.configForms.describe(),
			);
			ctx.effect(() => () => controller.dispose(), "harness-mcp-server: form subscriptions");
			// 整页入口: settings.section 是设置导航里的分区(plugins.item 是「插件」页的卡片列表槽)。
			ctx.effect(() => ctx.configForms.whileServed([NAMESPACE], () => ctx.slots.inject("settings.section", () => ctx.slots.register({
				name: "settings.section",
				id: "mcp-server",
				order: PAGE_ORDER,
				label: () => t("title"),
				locale: NS,
				inject: () => controller.inject(),
			}, McpServerSection))), "harness-mcp-server: page");
		}

		exports.apply = apply;
		exports.inject = INJECT;
		return module.exports;
	}
});
