# DSL Import and Slim Code/Tool Support Plan

## 背景判断

最近的提交已经把方向从“在 model runtime 里维护一套旧 Slim 运行时”转向了更清晰的边界：DSL import 负责识别、规范化和装配 workflow graph；Slim 负责把 Dify 插件协议接到 Graphon 的运行时协议上。`03f1db5 refactor(dsl): replace legacy slim llm support` 新增了 `graphon.dsl` 入口、`SlimDslNodeFactory`、Slim client、LLM/tool/code 适配；`7832af9 fix: preserve tool runtime compatibility` 则说明 tool 节点运行时必须继续通过稳定协议和不透明 handle 交互，避免把 Dify 运行时细节压进 Graphon 核心节点。

Dify 源码也支持这个判断。Dify 的 app DSL import 主要完成 YAML 校验、版本状态、workflow graph 持久化和 dependencies 检查，不把凭证作为 DSL 的稳定部分。运行时上，code 节点通过 Dify sandbox `/v1/sandbox/run` 执行，tool 节点通过 provider id 找到插件与 provider，再把调用委托给 tool engine 或 plugin daemon。`dify-plugin-daemon` 的 slim 模式已经提供了本地/远端统一 action：`invoke_tool`、`get_tool_runtime_parameters`、`get_ai_model_schemas`、`invoke_llm` 等；`dify-plugin-sdks` 的 Python SDK 则定义了 tool manifest、参数 form、runtime credentials 和 ToolInvokeMessage 形态。

因此，后续开发不应把目标设成“复刻完整 Dify 后端”，而应设成：在 Graphon 内保留轻量、可测试、可嵌入的 DSL 执行子集；在 Slim 边界尽量复用 Dify 插件协议；对暂不支持的 Dify 特性给出明确 inspect 结果和错误。

## 总体方向

DSL import 保持两层 API：`inspect()` 只解析能力、依赖和可加载状态；`loads()` 才构建 GraphEngine。导入阶段只做必要的兼容规范化，例如去掉 `custom-note`、补 edge 类型、规范 model provider，不做数据库迁移、凭证解密、插件安装 UI 或 workspace 绑定。

Slim 适配保持“plugin_unique_identifier + plugin-local provider”作为运行时身份。不要把 `provider_id` 的最后一段当成全局唯一值，也不要在 Graphon 自己维护另一套插件 manifest 解析规则。依赖解析以 Dify DSL 的 `dependencies` 为主，缺失时可以提供清晰错误或后续的 inspect-only 建议。

Code/tool 节点优先做到 Dify workflow happy path，而不是一次性覆盖所有节点和所有 Dify 平台能力。Code 节点以 sandbox 兼容为核心；tool 节点以 plugin tool 的参数解析、凭证传入、动态参数和消息转换为核心。文件、OAuth、selector、workflow-as-tool、MCP 等能力可以分阶段补齐。

## Code 节点方向

Code 节点继续使用 Dify sandbox 兼容协议，不在 Graphon 进程内直接执行用户代码。当前 `SandboxCodeExecutor` 已经对齐 Python/JavaScript runner、base64 输入、`<<RESULT>>` 输出解析、科学计数字符串回转数字和 sandbox 错误处理，后续应把这条线做稳。

优先级应放在三件事上：

1. 保持与 Dify `CodeExecutor` transformer 的行为一致，包括 Python `main(**inputs)`、JavaScript `main(inputs_obj)`、输出必须是对象、错误转为节点失败。
2. 用 Graphon 现有 `CodeNodeLimits` 和 `CodeNodeData.outputs` 做结果边界，不让 DSL import 绕过节点本身的类型与大小约束。
3. 明确 Jinja2 的定位：它是 Dify template transform 的 sandbox-backed renderer，不是当前 Graphon code 节点的可导入语言。若未来要支持 Dify 前端的 JSON/Jinja 编辑体验，也应作为单独兼容层处理。

Code 节点不需要引入 Slim daemon。它依赖的是 sandbox 服务配置，例如 endpoint、API key、timeout 和结果限制，这些已经适合放在 `DslCodeSettings`。

## Tool 节点方向

Tool 节点应围绕 Dify plugin tool 协议推进，而不是围绕某个具体工具写适配。核心路径是：

1. 从 DSL node data 读取 `provider_id`、`provider_name`、`tool_name`、`tool_parameters`、`tool_configurations`、`plugin_unique_identifier`。
2. 从 DSL `dependencies` 和凭证配置解析出 plugin unique identifier、plugin-local provider、credential type、credential values。
3. 通过 Slim client 调用 `extract` 获取 manifest，优先使用 manifest 中的静态参数；若工具声明 runtime parameters，则调用 `get_tool_runtime_parameters`。
4. 让 Graphon `ToolNode` 继续负责 LLM 参数的变量解析；Slim runtime 负责 form 参数的变量池解析、类型转换和传给 daemon 的最终 payload。
5. 把 SDK/daemon 返回的 ToolInvokeMessage 转成 Graphon 的 `ToolRuntimeMessage`，保持 text/json/link/image/blob/log/variable/retriever resource 等消息的语义。

近期最值得补强的是文件语义。Dify SDK 会把 file/files 参数和 blob/file 消息映射到 Dify file identity 与文件引用；Graphon 当前 Slim tool runtime 对 file message 和 tool file manager 仍是明确不支持。后续应先设计 Graphon 独立可用的 file reference 入口，再把 Dify 的 `dify_model_identity`、`file_marker`、blob chunk 和远端 URL 形态接进来。

Tool 运行时协议要继续保持兼容：`ToolNodeRuntimeProtocol`、`ToolRuntimeHandle` 和 `node_execution_id` 是 Graphon 与宿主运行时之间的边界。Slim 适配不应要求 ToolNode 了解插件 daemon、tenant、credential store 或 marketplace。

## DSL Import 方向

导入能力按“可执行最小子集”推进。当前可加载节点集合以 start/end/answer/if-else/template-transform/code/llm/tool 为主是合理的；其它 Dify 节点先通过 `inspect()` 标记 unsupported，并给出 node type 列表。这样比半支持更可控。

依赖处理应对齐 Dify：

- 新 DSL 优先读取 `dependencies`。
- 旧 DSL 或缺失依赖时，可以后续增加 best-effort dependency inference，但不能静默猜错插件版本。
- 凭证不要从 DSL graph 中恢复。Graphon 使用外部 `DslCredentials`，并支持按 plugin/provider/tool 维度匹配。

规范化要克制。Graphon 可以修正 Dify graph 里与执行无关或容易兼容的内容，但不应在 import 阶段重写节点语义。凡是需要 Dify workspace、数据库、安装状态、OAuth token 或用户上下文的能力，都应留给 Slim runtime 或宿主应用注入。

## 分阶段路线

第一阶段：稳住现有子集。补充 Dify 导出 DSL fixture，覆盖 `inspect()` 的 loadable/unsupported/dependencies 行为，以及 code/tool 的最小可执行路径。此阶段目标是“导入失败可解释，导入成功可运行”。

第二阶段：补齐 tool happy path。完善 plugin id/provider 匹配、runtime parameters、form 参数类型转换、credential type、daemon local/remote 行为差异和常见消息类型。以 `dify-plugin-sdks` 示例插件和 `dify-plugin-daemon` slim action 为测试依据。

第三阶段：处理文件和 selector。先定义 Graphon 侧 file reference 与 tool file manager 的宿主协议，再支持 file/files 参数、blob/file 输出、model-selector/app-selector、dynamic-select。不要把这些散落成一次性字段补丁。

第四阶段：扩大 DSL 覆盖。根据真实 Dify workflow 样本增加节点类型，例如 HTTP request、document extractor、variable assigner、knowledge retrieval 等。每扩一个节点，都要同时明确 DSL 字段、运行时依赖、Graphon 节点能力和 unsupported fallback。

第五阶段：形成兼容性矩阵。用文档和测试记录 Dify DSL 版本、支持节点、支持插件 action、需要外部服务的能力和明确不支持的能力。后续改动先更新矩阵，再改运行时。

## 不做的事

不在 Graphon DSL import 中复刻 Dify 的 app 数据库导入、workspace 权限、插件安装 UI、OAuth 流程或 dataset 解密逻辑。

不在 Graphon 进程内执行用户 code 节点。

不把 Dify provider slug 当作全局唯一标识；Slim 调用始终以 plugin unique identifier 定位插件，以 provider slug 定位插件内部 provider。

不为了“看起来支持”而吞掉未知节点、未知参数或未知消息类型。未知能力应在 inspect 或运行时错误中清楚暴露。

## 参考源码

- Graphon: `src/graphon/dsl/importer.py`
- Graphon: `src/graphon/dsl/node_factory.py`
- Graphon: `src/graphon/dsl/code_runtime.py`
- Graphon: `src/graphon/dsl/tool_runtime.py`
- Graphon: `src/graphon/nodes/tool/tool_node.py`
- Dify: `/root/g/dify/main/api/services/app_dsl_service.py`
- Dify: `/root/g/dify/main/api/core/helper/code_executor/code_executor.py`
- Dify: `/root/g/dify/main/api/core/workflow/node_runtime.py`
- Dify daemon: `/root/g/dify-plugin-daemon/main/pkg/slim`
- Dify SDK: `/root/g/dify-plugin-sdks/main/src/dify_plugin/entities/tool.py`
- Dify SDK: `/root/g/dify-plugin-sdks/main/src/dify_plugin/core/plugin_executor.py`
