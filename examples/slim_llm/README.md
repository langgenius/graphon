<!-- knowledge
last_checked: "2026-09-09T21:56:06Z"
-->
# Slim LLM Example

This directory has two versions of the same LLM workflow:

```text
start -> llm -> answer
```

## Source and setup

[graph.yml](graph.yml) defines the workflow; [dsl.py](dsl.py) imports it and
[code.py](code.py) constructs it directly. [settings.py](settings.py) handles
example configuration; the [Slim client](../../src/graphon/dsl/slim/client.py)
defines daemon configuration and binary lookup. Use
[credentials.example.json](credentials.example.json) as the credential schema.
Complete [repository setup](../../CONTRIBUTING.md#development-setup) first.

## Prepare

```bash
cd examples/slim_llm
cp credentials.example.json credentials.json
```

Fill in `credentials.json`. See the [Git ignore rule](../../.gitignore) and
[PR file check](../../.github/workflows/pr.yml) for local-configuration exclusions.

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

Both examples invoke a real model. Use [settings.py](settings.py) to choose local
or remote daemon setup and resolve configuration errors.
