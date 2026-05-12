"""Run a Dify chatflow DSL through ``graphon.dsl`` with diagnostics and streaming.

This is the canonical downstream-integrator pattern: feed a Dify Studio
exported workflow / advanced-chat YAML to ``graphon.dsl.loads`` and observe
events. The script is intentionally short — most of the work is delegated to
upstream graphon.

Run::

    cd examples/chatflow_dsl_runner
    cp credentials.example.json credentials.json
    # fill in the keys you need (the bundled fixture uses Alibaba Tongyi)

    python3 main.py /path/to/your-chatflow.yml "Hello, please introduce yourself."

The script performs three steps:

1. **Inspect** the DSL with ``graphon.dsl.inspect`` for a static plan
   (document kind, plugin dependencies, load status). Aborts early with a
   readable diagnostic when the DSL is not loadable (unsupported node type,
   missing plugin declaration, etc.).
2. **Load** the DSL with ``graphon.dsl.loads`` into a ``GraphEngine``.
3. **Run** the engine and react to its event stream:

   - ``GraphRunStartedEvent`` / ``GraphRunSucceededEvent`` / ``GraphRunFailedEvent``
     mark the workflow lifecycle.
   - ``NodeRunStartedEvent`` / ``NodeRunSucceededEvent`` / ``NodeRunFailedEvent``
     mark each node.
   - ``NodeRunStreamChunkEvent`` carries LLM/Answer chunks — written to
     stdout as they arrive so the user sees streaming output.
   - ``NodeRunAgentLogEvent`` reports Agent strategy inner steps (forwarded
     from the slim ``invoke_agent_strategy`` action once an ``AgentNode``
     implementation is in place upstream).

Credential isolation: keys live only in ``credentials.json`` (gitignored via
the repo-wide ``examples/*/credentials.json`` rule). The script never reads
ambient environment variables for API keys.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from graphon.dsl import inspect, loads
from graphon.dsl.entities import LoadStatus
from graphon.graph_events.agent import NodeRunAgentLogEvent
from graphon.graph_events.graph import (
    GraphRunFailedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
)

HERE = Path(__file__).resolve().parent
CREDENTIALS_FILE = HERE / "credentials.json"
CREDENTIALS_EXAMPLE_FILE = HERE / "credentials.example.json"
LOCAL_SLIM_BINARY = HERE / "slim"
DEFAULT_QUERY = "Hello! Please introduce yourself in one short sentence."

# advanced-chat DSLs commonly reference ``{{#sys.files#}}`` in their LLM
# / agent prompt templates even when no upload happens. The importer maps
# every ``start_inputs`` entry into ``("sys", key)`` selectors, so seeding
# an empty ``files`` list satisfies the template lookup without changing
# the DSL itself.
_DEFAULT_START_INPUTS: dict[str, Any] = {"files": []}


def load_credentials(path: Path = CREDENTIALS_FILE) -> dict[str, Any]:
    """Read and lightly normalize the credentials JSON.

    Relative paths under ``slim.plugin_folder`` / ``slim.plugin_root`` are
    resolved against this script's directory so the file is portable.

    Returns:
        The parsed credentials dict ready to pass to ``graphon.dsl.loads``.

    Raises:
        FileNotFoundError: If ``credentials.json`` does not exist.
        TypeError: If the JSON root is not an object.
    """
    if not path.is_file():
        msg = (
            f"Missing {path.name}. Copy {CREDENTIALS_EXAMPLE_FILE.name} "
            f"to {path.name} and fill in the required values."
        )
        raise FileNotFoundError(msg)

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"{path.name} must contain a JSON object."
        raise TypeError(msg)

    credentials: dict[str, Any] = dict(raw)
    slim_raw = credentials.get("slim")
    if isinstance(slim_raw, dict):
        slim_settings: dict[str, Any] = dict(slim_raw)
        for key in ("plugin_folder", "plugin_root"):
            value = slim_settings.get(key)
            if isinstance(value, str) and value:
                slim_settings[key] = _resolve_relative_path(value)
        credentials["slim"] = slim_settings
    return credentials


def _resolve_relative_path(value: str) -> str:
    path = Path(value).expanduser()
    if path.is_absolute():
        return str(path)
    return str((HERE / path).resolve())


def setup_slim_binary() -> None:
    """Point ``SLIM_BINARY_PATH`` at a local ``slim`` file if present.

    Matches the convention used by ``examples/slim_llm``: drop a copy or
    symlink of ``dify-plugin-daemon-slim`` next to ``main.py`` and the
    script picks it up. An explicit ``SLIM_BINARY_PATH`` env var still wins.
    """
    if os.environ.get("SLIM_BINARY_PATH"):
        return
    if LOCAL_SLIM_BINARY.is_file():
        os.environ["SLIM_BINARY_PATH"] = str(LOCAL_SLIM_BINARY)


def _emit(line: str = "") -> None:
    """Write a line to stdout. Centralized so all CLI output looks the same."""
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def diagnose(dsl_text: str) -> None:
    """Statically inspect a DSL before execution; abort on non-loadable plans."""
    plan = inspect(dsl_text)

    _emit("╭─ DSL inspection ────────")
    _emit(f"│ kind:    {plan.document.kind.value}")
    _emit(f"│ status:  {plan.load_status.value}")
    _emit(f"│ deps:    {len(plan.dependencies)} plugin(s)")
    for dep in plan.dependencies:
        identifier = dep.plugin_unique_identifier or "(no identifier)"
        _emit(f"│           - {identifier}")
    if plan.load_reason:
        _emit(f"│ reason:  {plan.load_reason}")
    _emit("╰─────────────────")

    if plan.load_status is not LoadStatus.LOADABLE:
        _emit()
        _emit("Cannot load this DSL. Common causes:")
        _emit("  - Unsupported node types (e.g. 'knowledge-retrieval' /")
        _emit("    'datasource' — the RAG path is not in scope here).")
        _emit("  - The DSL app.mode is config-only (chat / completion /")
        _emit("    agent-chat) and has no executable graph.")
        _emit("  - A declared plugin dependency cannot be resolved.")
        sys.exit(1)


def _format_node_label(event: NodeRunStartedEvent) -> str:
    title = event.node_title or event.node_id
    return f"[{event.node_type}] {title}"


def run_workflow(dsl_path: Path, query: str) -> str | None:
    """Load + run a DSL, streaming events to stdout. Return the answer string."""
    dsl_text = dsl_path.read_text(encoding="utf-8")
    diagnose(dsl_text)

    setup_slim_binary()
    credentials = load_credentials()

    engine = loads(
        dsl_text,
        credentials=credentials,
        workflow_id="chatflow-dsl-runner",
        start_inputs={**_DEFAULT_START_INPUTS, "query": query},
    )

    _emit()
    final_answer: str | None = None
    for event in engine.run():
        if isinstance(event, GraphRunStartedEvent):
            _emit("> Graph run started")
        elif isinstance(event, NodeRunStartedEvent):
            _emit(f"  > {_format_node_label(event)}")
        elif isinstance(event, NodeRunStreamChunkEvent):
            sys.stdout.write(event.chunk)
            sys.stdout.flush()
        elif isinstance(event, NodeRunAgentLogEvent):
            _emit(f"\n    * agent log [{event.label}] {event.status}")
        elif isinstance(event, NodeRunFailedEvent):
            _emit(f"\n  ! node {event.node_id} failed: {event.error}")
        elif isinstance(event, GraphRunSucceededEvent):
            answer = event.outputs.get("answer")
            if isinstance(answer, str):
                final_answer = answer
            _emit("\n[OK] Graph run succeeded")
        elif isinstance(event, GraphRunFailedEvent):
            _emit(f"\n[FAIL] Graph run failed: {event.error}")
            sys.exit(2)

    return final_answer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Dify chatflow DSL through graphon.dsl.loads",
    )
    parser.add_argument(
        "dsl_path",
        type=Path,
        help="Path to a Dify-exported chatflow / workflow DSL YAML.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=DEFAULT_QUERY,
        help=f"User input passed into the start node (default: {DEFAULT_QUERY!r}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    answer = run_workflow(args.dsl_path, args.query)
    if answer:
        _emit("\n── Final answer ─────────")
        _emit(answer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
