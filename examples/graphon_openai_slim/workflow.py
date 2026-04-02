"""Minimal Graphon `start -> LLM -> output` workflow example for Slim.

Run from this directory:

    python3 workflow.py "Explain Graphon in one short sentence."

The script automatically loads `examples/graphon_openai_slim/.env`.
Existing environment variables take precedence over `.env` values.

Required environment variables:
- `OPENAI_API_KEY`
- `SLIM_PLUGIN_ID`

Optional environment variables:
- `SLIM_BINARY_PATH` points at a custom `dify-plugin-daemon-slim` binary
- `SLIM_PROVIDER` defaults to `openai`
- `SLIM_PLUGIN_FOLDER` defaults to the repository `.slim/plugins` cache
- `SLIM_PLUGIN_ROOT` points at an already unpacked local plugin directory
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import IO

EXAMPLE_DIR = Path(__file__).resolve().parent
REPO_ROOT = EXAMPLE_DIR.parents[1]
LOCAL_SRC_DIR = REPO_ROOT / "src"
LOCAL_VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
DEFAULT_ENV_FILE = EXAMPLE_DIR / ".env"
BOOTSTRAP_ENV_VAR = "GRAPHON_EXAMPLE_BOOTSTRAPPED"
RUNTIME_MODULES = ("pydantic", "httpx", "yaml")


def bootstrap_local_python() -> None:
    if os.environ.get(BOOTSTRAP_ENV_VAR) == "1":
        return
    if all(importlib.util.find_spec(module) is not None for module in RUNTIME_MODULES):
        return
    if not LOCAL_VENV_PYTHON.is_file():
        return

    env = dict(os.environ)
    env[BOOTSTRAP_ENV_VAR] = "1"
    os.execve(
        str(LOCAL_VENV_PYTHON),
        [str(LOCAL_VENV_PYTHON), str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )


bootstrap_local_python()

if importlib.util.find_spec("graphon") is None and str(LOCAL_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(LOCAL_SRC_DIR))

# ruff: noqa: E402
from graphon.entities.graph_init_params import GraphInitParams
from graphon.file.enums import FileType
from graphon.file.models import File
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels import InMemoryChannel
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_events.node import NodeRunStreamChunkEvent
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.entities.message_entities import (
    PromptMessage,
    PromptMessageRole,
)
from graphon.model_runtime.slim import (
    SlimConfig,
    SlimLocalSettings,
    SlimPreparedLLM,
    SlimProviderBinding,
    SlimRuntime,
)
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.llm import (
    LLMNode,
    LLMNodeChatModelMessage,
    LLMNodeData,
    ModelConfig,
)
from graphon.nodes.llm.entities import ContextConfig
from graphon.nodes.start import StartNode
from graphon.nodes.start.entities import StartNodeData
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.input_entities import VariableEntity, VariableEntityType

ALLOWED_ENV_VARS: dict[str, str] = {
    "OPENAI_API_KEY": "",
    "SLIM_PLUGIN_ID": "",
    "SLIM_BINARY_PATH": "",
    "SLIM_PROVIDER": "openai",
    "SLIM_PLUGIN_FOLDER": "../../.slim/plugins",
    "SLIM_PLUGIN_ROOT": "",
}
PATH_ENV_VARS = {
    "SLIM_BINARY_PATH",
    "SLIM_PLUGIN_FOLDER",
    "SLIM_PLUGIN_ROOT",
}
STREAM_SELECTOR = ("llm", "text")


def load_default_env_file() -> None:
    if DEFAULT_ENV_FILE.is_file():
        load_env_file(DEFAULT_ENV_FILE)


def load_env_file(path: Path) -> None:
    env_dir = path.resolve().parent
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            msg = f"Invalid .env line {line_number} in {path}: {raw_line}"
            raise ValueError(msg)

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            msg = f"Invalid .env key on line {line_number} in {path}"
            raise ValueError(msg)
        if key not in ALLOWED_ENV_VARS:
            msg = f"Unsupported .env key {key!r} on line {line_number} in {path}"
            raise ValueError(msg)

        os.environ.setdefault(
            key,
            normalize_env_value(
                key,
                strip_optional_quotes(value.strip()),
                base_dir=env_dir,
            ),
        )


def strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def normalize_env_value(name: str, value: str, *, base_dir: Path) -> str:
    if name not in PATH_ENV_VARS or not value:
        return value

    path_value = Path(value).expanduser()
    if not path_value.is_absolute():
        path_value = (base_dir / path_value).resolve()
    else:
        path_value = path_value.resolve()
    return str(path_value)


class PassthroughPromptMessageSerializer:
    def serialize(
        self,
        *,
        model_mode: LLMMode,
        prompt_messages: Sequence[PromptMessage],
    ) -> object:
        _ = model_mode
        return list(prompt_messages)


class TextOnlyFileSaver:
    def save_binary_string(
        self,
        data: bytes,
        mime_type: str,
        file_type: FileType,
        extension_override: str | None = None,
    ) -> File:
        _ = data, mime_type, file_type, extension_override
        msg = "This example only supports text responses."
        raise RuntimeError(msg)

    def save_remote_url(self, url: str, file_type: FileType) -> File:
        _ = url, file_type
        msg = "This example only supports text responses."
        raise RuntimeError(msg)


def require_env(name: str) -> str:
    value = env_value(name)
    if value:
        return value
    msg = f"{name} is required."
    raise ValueError(msg)


def env_value(name: str) -> str:
    raw_value = os.environ.get(name)
    if raw_value is not None:
        return raw_value.strip()
    return normalize_env_value(
        name,
        ALLOWED_ENV_VARS[name],
        base_dir=EXAMPLE_DIR,
    ).strip()


def optional_path(name: str) -> Path | None:
    value = env_value(name)
    return Path(value).expanduser() if value else None


def build_runtime() -> tuple[SlimRuntime, str]:
    provider = env_value("SLIM_PROVIDER")
    plugin_folder = Path(env_value("SLIM_PLUGIN_FOLDER")).expanduser()
    plugin_root = optional_path("SLIM_PLUGIN_ROOT")

    runtime = SlimRuntime(
        SlimConfig(
            bindings=[
                SlimProviderBinding(
                    plugin_id=require_env("SLIM_PLUGIN_ID"),
                    provider=provider,
                    plugin_root=plugin_root,
                ),
            ],
            local=SlimLocalSettings(folder=plugin_folder),
        ),
    )
    return runtime, provider


def build_graph(
    *,
    provider: str,
    prepared_llm: SlimPreparedLLM,
    graph_init_params: GraphInitParams,
    graph_runtime_state: GraphRuntimeState,
) -> Graph:
    start_node = StartNode(
        node_id="start",
        config={
            "id": "start",
            "data": StartNodeData(
                title="Start",
                variables=[
                    VariableEntity(
                        variable="query",
                        label="Query",
                        type=VariableEntityType.PARAGRAPH,
                        required=True,
                    ),
                ],
            ),
        },
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
    )

    llm_node = LLMNode(
        node_id="llm",
        config={
            "id": "llm",
            "data": LLMNodeData(
                title="LLM",
                model=ModelConfig(
                    provider=provider,
                    name="gpt-5.4",
                    mode=LLMMode.CHAT,
                ),
                prompt_template=[
                    LLMNodeChatModelMessage(
                        role=PromptMessageRole.SYSTEM,
                        text="You are a concise assistant.",
                    ),
                    LLMNodeChatModelMessage(
                        role=PromptMessageRole.USER,
                        text="{{#start.query#}}",
                    ),
                ],
                context=ContextConfig(enabled=False),
            ),
        },
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
        model_instance=prepared_llm,
        llm_file_saver=TextOnlyFileSaver(),
        prompt_message_serializer=PassthroughPromptMessageSerializer(),
    )

    output_node = AnswerNode(
        node_id="output",
        config={
            "id": "output",
            "data": AnswerNodeData(
                title="Output",
                answer="{{#llm.text#}}",
            ),
        },
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
    )

    return (
        Graph
        .new()
        .add_root(start_node)
        .add_node(llm_node)
        .add_node(output_node)
        .build()
    )


def write_stream_chunk(event: object, *, stream_output: IO[str]) -> bool:
    if not isinstance(event, NodeRunStreamChunkEvent):
        return False
    if tuple(event.selector) != STREAM_SELECTOR or not event.chunk:
        return False

    stream_output.write(event.chunk)
    stream_output.flush()
    return True


def _execute_workflow(
    query: str,
    *,
    stream_output: IO[str] | None = None,
) -> tuple[str, bool]:
    load_default_env_file()
    runtime, provider = build_runtime()
    workflow_id = "example-start-llm-output"
    graph_init_params = GraphInitParams(
        workflow_id=workflow_id,
        graph_config={"nodes": [], "edges": []},
        run_context={},
        call_depth=0,
    )
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.time(),
    )
    graph_runtime_state.variable_pool.add(("start", "query"), query)

    prepared_llm = SlimPreparedLLM(
        runtime=runtime,
        provider=provider,
        model_name="gpt-5.4",
        credentials={"openai_api_key": require_env("OPENAI_API_KEY")},
        parameters={},
    )
    graph = build_graph(
        provider=provider,
        prepared_llm=prepared_llm,
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
    )
    engine = GraphEngine(
        workflow_id=workflow_id,
        graph=graph,
        graph_runtime_state=graph_runtime_state,
        command_channel=InMemoryChannel(),
    )

    streamed = False
    for event in engine.run():
        if stream_output is not None and write_stream_chunk(
            event,
            stream_output=stream_output,
        ):
            streamed = True

    answer = graph_runtime_state.get_output("answer")
    if not isinstance(answer, str):
        msg = "Workflow did not produce a text answer."
        raise TypeError(msg)
    if stream_output is not None and streamed and not answer.endswith("\n"):
        stream_output.write("\n")
        stream_output.flush()
    return answer, streamed


def run_workflow(query: str) -> str:
    answer, _ = _execute_workflow(query)
    return answer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a minimal start -> LLM -> output workflow with Slim.",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default="Explain Graphon in one short sentence.",
        help="User input passed into the Start node.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    answer, streamed = _execute_workflow(args.query, stream_output=sys.stdout)
    if not streamed:
        sys.stdout.write(f"{answer}\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
