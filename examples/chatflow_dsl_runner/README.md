# Chatflow DSL Runner

Canonical pattern for executing a **Dify Studio exported chatflow / workflow DSL** through `graphon.dsl.loads` from an external Python process.

Unlike the sibling `examples/slim_llm` example — which carries its own simplified `graph.yml` — this runner takes a real Dify export YAML as input, performs static inspection, and streams the run to stdout.

Two bundled fixtures are provided:

| Fixture | Shape | Runs out of the box? |
| --- | --- | --- |
| `chatflow_dsl_simple.yml` | `start → llm → answer` | **Yes.** Only requires the `langgenius/openai` plugin and a model key. |
| `chatflow_dsl_agent.yml` | `start → llm → answer` + `llm → agent → answer` | **No** — see [Architecture & limitations](#architecture--limitations). Requires a running Dify control plane. |

## What this demo shows

| Capability | Where in the code |
| --- | --- |
| Static DSL inspection before run | `diagnose()` calls `graphon.dsl.inspect` |
| Loading + executing a DSL | `run_workflow()` calls `graphon.dsl.loads` |
| Live streaming of LLM / Answer chunks | `NodeRunStreamChunkEvent` handler |
| Lifecycle visibility (per-node start / fail; graph succeed / fail) | event loop |
| Agent strategy log forwarding | `NodeRunAgentLogEvent` handler |
| Credential isolation (no env vars for secrets) | `credentials.json` |
| Slim binary auto-discovery | `setup_slim_binary()` |

## Requirements

- Everything in the root `README.md` (Python 3.12/3.13, `uv`, `make`).
- A built `dify-plugin-daemon-slim` binary — either on `PATH`, pointed at via `SLIM_BINARY_PATH`, or dropped into this directory as `./slim`. Build it from [`langgenius/dify-plugin-daemon`](https://github.com/langgenius/dify-plugin-daemon):

  ```bash
  go build -o /path/to/dify-plugin-daemon-slim ./cmd/slim
  ```

- Plugin packages declared in a DSL's top-level `dependencies:` are auto-downloaded from `marketplace.dify.ai` on first use (cached under the directory specified in `credentials.json::slim.plugin_folder`).
  - `chatflow_dsl_simple.yml` declares `langgenius/openai` — auto-downloaded.
  - `chatflow_dsl_agent.yml` declares `langgenius/openai` likewise. The `langgenius/agent` strategy plugin is referenced on the agent node itself, and `langgenius/json_process` is referenced inside the agent node's `tools` parameter. See [Architecture & limitations](#architecture--limitations) for how those are expected to be installed.

## Run

```bash
cd examples/chatflow_dsl_runner
cp credentials.example.json credentials.json
# edit credentials.json: fill in your openai_api_key (and openai_api_base if you proxy the API)

# If your slim binary is not on PATH:
export SLIM_BINARY_PATH=/abs/path/to/dify-plugin-daemon-slim
# OR drop a symlink in this directory:
# ln -s /abs/path/to/dify-plugin-daemon-slim ./slim

# Out-of-the-box: simple start → llm → answer chatflow
python3 main.py chatflow_dsl_simple.yml "Hello, please introduce yourself."

# Full fixture (see "Architecture & limitations" below — this one needs a running Dify Server + plugin daemon):
python3 main.py chatflow_dsl_agent.yml "Hello, please introduce yourself."

# Or pass any other Dify chatflow YAML:
python3 main.py /path/to/your-chatflow.yml "Hello, please introduce yourself."
```

### Quick reference: the canonical 4-line core

After credential setup and DSL inspection, the entire DSL-driven run is:

```python
engine = loads(
    dsl_text,
    credentials=credentials,
    workflow_id="chatflow-dsl-runner",
    start_inputs={"query": query},
)
for event in engine.run():
    ...  # consume events
```

Everything else in `main.py` is **decoration** — diagnostics, streaming, credential plumbing. The integration surface is exactly those 4 lines.

## Output shape

```
╭─ DSL inspection ────────
│ kind:    app
│ status:  loadable
│ deps:    1 plugin(s)
│           - langgenius/openai:0.4.0@<digest>
╰─────────────────

> Graph run started
  > [start] User Input
  > [llm] LLM
Hello! I am ... (streamed chunks)
  > [answer] Response

[OK] Graph run succeeded

── Final answer ─────────
Hello! I am ...
```

## Architecture & limitations

`graphon` is a **pure execution engine** — it knows about nodes, edges, the slim plugin runtime, and how to stream events. It deliberately does **not** know about tenants, workspaces, model-provider catalogues, or quota: those are control-plane concerns owned by Dify Server.

That separation has a load-bearing consequence for the `agent` node.

### Why `chatflow_dsl_simple.yml` runs out of the box

A plain `llm` node calls a model-provider plugin (e.g. `langgenius/openai`) through the slim runtime. The plugin's `invoke_llm` action is a single forward call: stdin → plugin → LLM API → stdout. No callbacks into Dify Server are required, so local slim mode is sufficient:

```
graphon ── stdin/stdout NDJSON ──▶ slim binary ──▶ openai plugin ──▶ LLM
```

### Why `chatflow_dsl_agent.yml` does **not** run out of the box

The `langgenius/agent` plugin implements its strategy logic in Python, and inside that logic it does `self.session.model.llm.invoke(...)` to call the LLM. That call is a **nested plugin invocation** — the agent plugin asks the runtime to dispatch another plugin (the model provider) on its behalf. In production Dify this is implemented by:

```
agent plugin ──▶ daemon backwards_invocation
            ──▶ POST http://dify-api:5001/inner/api/invoke/llm
            ──▶ Dify Server validates tenant + provider config
            ──▶ Dify Server dispatches openai plugin via daemon
            ──▶ response streams back up the chain
```

The 5001 inner API is part of Dify Server, and it requires a **tenant context** (provider credentials are scoped per workspace). Standing up that whole control plane just to drive the agent node from a graphon-only runner means:

1. Run `dify-plugin-daemon` (not just the slim binary) at e.g. `:5002`.
2. Run Dify Server at `:5001` with a configured tenant + OpenAI provider.
3. Install both `langgenius/agent` and `langgenius/openai` into the daemon for that tenant.
4. Some path for backwards-invocation requests originating from the agent plugin to reach Dify Server with a valid tenant context.

The fourth point is where the architectural collision sits. The natural implementation — teaching the slim runtime to forward `tenant_id` / `user_id` — would leak Dify's business-side identity model into a tenant-agnostic execution engine, which is **explicitly rejected** here.

The correct long-term fix is described in [RFC #102](https://github.com/langgenius/graphon/issues/102): re-shape the daemon so plugin code can perform nested invocations without depending on Dify Server's inner API. **Until that lands, the agent fixture is included as a structural reference — it is what a real Dify Studio export of a chatflow with an agent looks like — not as something `main.py` can drive end-to-end on its own.**

## What is and is not supported

The upstream `graphon.dsl` importer accepts:

- `kind: app` Dify exports where `app.mode in {workflow, advanced-chat}`
- `kind: graph` simplified DSL (see `examples/slim_llm/graph.yml`)

Supported **node types** (built into `SlimDslNodeFactory`, including this fork's added `agent` routing):

`start`, `end`, `answer`, `llm`, `if-else`, `template-transform`, `code`, `tool`, `agent`

**Not supported by this demo** (the inspector will flag them):

- `knowledge-retrieval`, `datasource` — RAG path not in scope.
- `app.mode in {chat, completion, agent-chat}` — these are config-only, no executable graph exists. The inspector rejects them.

## Integrating into your own product

The pattern in `main.py` is what you want for an SDK-style integration:

1. Keep `credentials.json` schema simple and explicit — pass it whole to `loads`. No environment-variable spelunking.
2. Hold on to the `GraphEngine` object — you can call `engine.run()` in a thread and bridge events to an SSE stream / websocket / async queue.
3. Use `inspect()` *before* `loads()` to give the operator/admin a readable plan and refuse misconfigurations early.
4. Match each event in the run loop and translate it to your product's surface (chat bubble, log line, billing meter, trace span).

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `Missing credentials.json` | You didn't `cp credentials.example.json credentials.json` |
| `SLIM_BINARY_PATH points to a missing file` | Path you set does not exist or is not executable |
| `dify-plugin-daemon-slim is not available in PATH` | Build the slim binary and either add it to PATH or set `SLIM_BINARY_PATH` |
| `Variable #sys.files# not found` | Your DSL's LLM node references `sys.files` but `main.py` only seeds `query`. `main.py` seeds an empty `files` list by default; if your DSL needs more system vars, extend `_DEFAULT_START_INPUTS`. |
| `DSL not loadable. ... Unsupported node types: ...` | The named node is not supported (e.g. `knowledge-retrieval`, `datasource`), or the DSL's `app.mode` is config-only. |
| `tenant not found` / `Provider ... does not exist` / agent node hangs | You're running `chatflow_dsl_agent.yml` without a Dify Server control plane. See [Architecture & limitations](#architecture--limitations) — this fixture is not runnable in local slim mode. |
| Plugin download takes minutes on first run | Marketplace download + `uv` env init for the plugin's Python venv. Subsequent runs hit the cache. |

## Layout

```
chatflow_dsl_runner/
├── README.md                       # this file
├── main.py                         # CLI entrypoint
├── chatflow_dsl_simple.yml         # runnable fixture: start → llm → answer
├── chatflow_dsl_agent.yml          # reference fixture with agent node (see limitations)
├── credentials.example.json        # config template (commit-safe)
├── credentials.json                # your real keys (gitignored)
└── __init__.py
```
