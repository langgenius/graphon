"""Minimal `start -> LLM -> output` workflow example for Slim.

Run from the repository root with `PYTHONPATH=src`, for example:

    PYTHONPATH=src \
    OPENAI_API_KEY=... \
    SLIM_PLUGIN_ID=... \
    python3 example/workflow.py "Explain Graphon in one short sentence."

The script automatically loads a `.env` file from the current working
directory or the repository root. Existing environment variables take
precedence over `.env` values.

Required environment variables:
- `OPENAI_API_KEY`
- `SLIM_PLUGIN_ID`

Optional environment variables:
- `SLIM_BINARY_PATH` points at a custom `dify-plugin-daemon-slim` binary
- `SLIM_PROVIDER` defaults to `openai`
- `SLIM_PLUGIN_FOLDER` defaults to `.slim/plugins`
- `SLIM_PLUGIN_ROOT` points at an already unpacked local plugin directory
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from pathlib import Path

from graphon.entities.graph_init_params import GraphInitParams
from graphon.file.enums import FileType
from graphon.file.models import File
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels import InMemoryChannel
from graphon.graph_engine.graph_engine import GraphEngine
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
    "SLIM_PLUGIN_FOLDER": ".slim/plugins",
    "SLIM_PLUGIN_ROOT": "",
}


def load_default_env_file() -> None:
    for path in env_file_candidates():
        if path.is_file():
            load_env_file(path)
            return


def env_file_candidates() -> list[Path]:
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]
    unique_candidates: list[Path] = []
    seen_paths: set[Path] = set()
    for candidate in candidates:
        resolved_candidate = candidate.resolve()
        if resolved_candidate in seen_paths:
            continue
        seen_paths.add(resolved_candidate)
        unique_candidates.append(candidate)
    return unique_candidates


def load_env_file(path: Path) -> None:
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            raise ValueError(f"Invalid .env line {line_number} in {path}: {raw_line}")

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid .env key on line {line_number} in {path}")
        if key not in ALLOWED_ENV_VARS:
            raise ValueError(
                f"Unsupported .env key {key!r} on line {line_number} in {path}"
            )

        os.environ.setdefault(key, strip_optional_quotes(value.strip()))


def strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


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
        raise RuntimeError("This example only supports text responses.")

    def save_remote_url(self, url: str, file_type: FileType) -> File:
        _ = url, file_type
        raise RuntimeError("This example only supports text responses.")


def require_env(name: str) -> str:
    value = env_value(name)
    if value:
        return value
    raise ValueError(f"{name} is required.")


def env_value(name: str) -> str:
    return os.environ.get(name, ALLOWED_ENV_VARS[name]).strip()


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
                )
            ],
            local=SlimLocalSettings(folder=plugin_folder),
        )
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
        id="start",
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
                    )
                ],
            ),
        },
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
    )

    llm_node = LLMNode(
        id="llm",
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
        id="output",
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


def run_workflow(query: str) -> str:
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

    for _ in engine.run():
        pass

    answer = graph_runtime_state.get_output("answer")
    if not isinstance(answer, str):
        raise RuntimeError("Workflow did not produce a text answer.")
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
    print(run_workflow(args.query))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
