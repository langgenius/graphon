from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path

EXAMPLE_DIR = Path(__file__).resolve().parent
REPO_ROOT = EXAMPLE_DIR.parents[1]
LOCAL_SRC_DIR = REPO_ROOT / "src"
LOCAL_VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
DEFAULT_DSL_FILE = EXAMPLE_DIR / "workflow.yml"
DEFAULT_CREDENTIAL_FILE = EXAMPLE_DIR / "cred.json"
DEFAULT_CREDENTIAL_EXAMPLE_FILE = EXAMPLE_DIR / "cred.json.example"
DEFAULT_SLIM_BINARY = EXAMPLE_DIR / "slim"
DEFAULT_SLIM_PLUGIN_FOLDER = EXAMPLE_DIR / ".slim" / "plugins"
BOOTSTRAP_ENV_VAR = "GRAPHON_DSL_OPENAI_EXAMPLE_BOOTSTRAPPED"
RUNTIME_MODULES = ("pydantic", "yaml")
DEFAULT_QUERY = "Reply with only the word Graphon."
SLIM_PYTHON_ENV_INIT_TIMEOUT = 600
SLIM_MAX_EXECUTION_TIMEOUT = 600


def bootstrap_local_python() -> None:
    if os.environ.get(BOOTSTRAP_ENV_VAR) == "1":
        return
    if all(importlib.util.find_spec(module) is not None for module in RUNTIME_MODULES):
        return
    if not LOCAL_VENV_PYTHON.is_file():
        return

    env = dict(os.environ)
    env[BOOTSTRAP_ENV_VAR] = "1"
    os.execve(  # noqa: S606
        str(LOCAL_VENV_PYTHON),
        [str(LOCAL_VENV_PYTHON), str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )


bootstrap_local_python()

if importlib.util.find_spec("graphon") is None and str(LOCAL_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(LOCAL_SRC_DIR))

# ruff: noqa: E402
from graphon.dsl import loads
from graphon.graph_events.graph import GraphRunSucceededEvent


def load_credentials(path: Path = DEFAULT_CREDENTIAL_FILE) -> dict[str, object]:
    if not path.is_file():
        msg = (
            f"Credential file is required: {path}. "
            f"Copy {DEFAULT_CREDENTIAL_EXAMPLE_FILE.name} to {path.name} "
            "and fill it in."
        )
        raise FileNotFoundError(msg)

    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = f"Credential file must contain a JSON object: {path}"
        raise TypeError(msg)

    return {
        **dict(loaded),
        "slim": {
            "plugin_folder": str(DEFAULT_SLIM_PLUGIN_FOLDER),
            "python_env_init_timeout": SLIM_PYTHON_ENV_INIT_TIMEOUT,
            "max_execution_timeout": SLIM_MAX_EXECUTION_TIMEOUT,
        },
    }


def run_workflow(query: str, *, print_dsl: bool) -> str:
    os.environ["SLIM_BINARY_PATH"] = str(DEFAULT_SLIM_BINARY)

    dsl = DEFAULT_DSL_FILE.read_text(encoding="utf-8")
    if print_dsl:
        sys.stdout.write(dsl)
        sys.stdout.flush()

    engine = loads(
        dsl,
        credentials=load_credentials(),
        workflow_id="example-dsl-openai-slim",
        start_inputs={"query": query},
    )
    events = list(engine.run())
    final_event = events[-1] if events else None
    if not isinstance(final_event, GraphRunSucceededEvent):
        msg = f"Workflow did not succeed; final event: {type(final_event).__name__}"
        raise TypeError(msg)

    answer = final_event.outputs.get("answer")
    if not isinstance(answer, str):
        msg = "Workflow succeeded but did not produce a string answer."
        raise TypeError(msg)

    return answer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a DSL-imported OpenAI workflow with Slim.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=None,
        help="User input passed into the Start node.",
    )
    parser.add_argument(
        "--print-dsl",
        action="store_true",
        help="Print workflow.yml before running it.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    query = args.query if args.query is not None else DEFAULT_QUERY
    answer = run_workflow(query, print_dsl=args.print_dsl)
    sys.stdout.write(f"{answer}\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
