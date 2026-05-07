# Slim LLM Example

This directory has two versions of the same LLM workflow:

```text
start -> llm -> answer
```

## Files

- `graph.yml`: the DSL graph
- `dsl.py`: imports `graph.yml` with `graphon.dsl.loads()`
- `code.py`: builds the graph with Python code
- `settings.py`: shared credentials and Slim setup
- `credentials.example.json`: credentials template
- `credentials.json`: your local credentials

## Prepare

```bash
cd examples/slim_llm
cp credentials.example.json credentials.json
```

Fill in `credentials.json`.

## DSL Import

```bash
python3 dsl.py
python3 dsl.py "Reply with only the word Graphon."
```

## Code Construction

```bash
python3 code.py
python3 code.py "Reply with only the word Graphon."
```

For local mode, keep `slim.mode` as `local`. Put `dify-plugin-daemon-slim` in
`PATH`, set `SLIM_BINARY_PATH`, or place a `slim` binary in this directory.

For remote mode, set `slim.mode` to `remote`, then fill in `daemon_addr` and
`daemon_key`.

`credentials.json` is local-only and should not be committed.
