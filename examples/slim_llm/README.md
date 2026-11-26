<!-- knowledge
last_checked: "2026-11-26T01:12:45Z"
-->
# Slim LLM Example

The YAML and Python examples run the same three steps.

```text
start -> llm -> answer
```

## Source and setup

The YAML example passes its document to `loads()`. The Python example connects
the nodes with `GraphBuilder` and passes the result of `build()` to `load_graph()`.
See [graph.yml](graph.yml), [dsl.py](dsl.py), and [code.py](code.py).

[settings.py](settings.py) handles example configuration. The [Slim client](../../src/graphon/dsl/slim/client.py)
defines daemon configuration and binary lookup. Use
[credentials.example.json](credentials.example.json) as the credential schema.
Complete [repository setup](../../CONTRIBUTING.md#development-setup) first.

## Prepare

```bash
cd examples/slim_llm
cp credentials.example.json credentials.json
```

Fill in the model credentials in `credentials.json`. For a remote daemon,
set `slim.mode` to `remote` and supply `slim.daemon_addr` and
`slim.daemon_key`. The example defaults to local mode. See the
[Git ignore rule](../../.gitignore) and
[PR file check](../../.github/workflows/pr.yml) for local-configuration exclusions.

Provide a Slim executable through `SLIM_BINARY_PATH`, place an executable named
`slim` in this directory, or put `dify-plugin-daemon-slim` on `PATH`. Both local
and remote modes require this executable.

## DSL Import

```bash
uv run python dsl.py
uv run python dsl.py "Reply with only the word Graphon."
```

## Code Construction

```bash
uv run python code.py
uv run python code.py "Reply with only the word Graphon."
```

Both examples invoke a real model. Use `--help` to check command
usage without credentials or a Slim executable.
