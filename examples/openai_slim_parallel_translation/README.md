# OpenAI Slim Parallel Translation Example

This example runs a fan-out / fan-in workflow:

`start -> 3 llm -> end`

The `start` node accepts `content`, three LLM nodes translate it into Chinese,
English, and Japanese in parallel, and the `end` node exposes those three
translations as structured outputs.

## Files

- `workflow.py`: runnable example script
- `.env.example`: template configuration
- `.env`: local configuration file for this example only

## Run

1. Change into this directory:

```bash
cd examples/openai_slim_parallel_translation
```

2. Copy the template:

```bash
cp .env.example .env
```

3. Fill in the required values in `.env`.

4. Run the example:

```bash
python3 workflow.py "Graph execution is a coordination problem."
```

The example streams each language section as it becomes available and then
prints the final structured outputs.

If you want to disable streaming and only print the final structured outputs:

```bash
python3 workflow.py --no-stream "Graph execution is a coordination problem."
```

## Notes

- `workflow.py` shares the same bootstrap and Slim runtime
  support as the minimal example, but it is self-contained in its own example
  directory.
- No `slim` executable is bundled in this example directory. Provide
  `dify-plugin-daemon-slim` via `PATH` or set `SLIM_BINARY_PATH` in `.env` if
  you keep it elsewhere.
- Path-like variables in `.env` are resolved relative to this example
  directory, not relative to your shell's current working directory.
