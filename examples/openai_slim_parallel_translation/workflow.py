"""Parallel translation workflow built with the sequential WorkflowBuilder API.

Run from this directory:

    python3 workflow.py \
        "Graph execution is a coordination problem."

The script automatically loads `.env` from this example directory.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

EXAMPLE_FILE = Path(__file__).resolve()
EXAMPLE_DIR = EXAMPLE_FILE.parent
REPO_ROOT = EXAMPLE_FILE.parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples import openai_slim_support

openai_slim_support.prepare_example_imports(EXAMPLE_FILE)

# ruff: noqa: E402
from graphon.entities.graph_init_params import GraphInitParams
from graphon.graph.graph import Graph
from graphon.graph_engine.command_channels import InMemoryChannel
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_events.node import NodeRunStreamChunkEvent
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.slim import SlimPreparedLLM
from graphon.nodes.end.entities import EndNodeData
from graphon.nodes.llm import LLMNodeData, ModelConfig
from graphon.nodes.start.entities import StartNodeData
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.workflow_builder import WorkflowBuilder, paragraph_input, system, user

TARGET_LANGUAGES: tuple[tuple[str, str, str], ...] = (
    ("translate_zh", "Chinese", "chinese"),
    ("translate_en", "English", "english"),
    ("translate_ja", "Japanese", "japanese"),
)
STREAM_LABEL_BY_SELECTOR = {
    (node_id, "text"): language_name
    for node_id, language_name, _output_name in TARGET_LANGUAGES
}


@dataclass(slots=True)
class TranslationStreamWriter:
    stream_output: IO[str]
    seen_selectors: set[tuple[str, str]] = field(default_factory=set)
    active_selector: tuple[str, str] | None = None

    def write_event(self, event: object) -> bool:
        if not isinstance(event, NodeRunStreamChunkEvent):
            return False

        selector = tuple(event.selector)
        label = STREAM_LABEL_BY_SELECTOR.get(selector)
        if label is None:
            return False

        if selector not in self.seen_selectors:
            self.stream_output.write(f"{label}: ")
            self.seen_selectors.add(selector)
            self.active_selector = selector
        elif self.active_selector is None:
            self.active_selector = selector

        if event.chunk:
            self.stream_output.write(event.chunk)
        if event.is_final:
            self.stream_output.write("\n")
            self.active_selector = None

        self.stream_output.flush()
        return bool(event.chunk) or event.is_final


def build_graph(
    *,
    provider: str,
    prepared_llm: SlimPreparedLLM,
    graph_init_params: GraphInitParams,
    graph_runtime_state: GraphRuntimeState,
) -> Graph:
    workflow = WorkflowBuilder(
        graph_init_params=graph_init_params,
        graph_runtime_state=graph_runtime_state,
        prepared_llm=prepared_llm,
    )

    start = workflow.root(
        "start",
        StartNodeData(
            title="Start",
            variables=[paragraph_input("content", required=True)],
        ),
    )

    model = ModelConfig(
        provider=provider,
        name="gpt-5.4",
        mode=LLMMode.CHAT,
    )

    translation_nodes = []
    for node_id, language_name, _output_name in TARGET_LANGUAGES:
        translation_nodes.append(
            start.then(
                node_id,
                LLMNodeData(
                    title=f"Translate to {language_name}",
                    model=model,
                    prompt_template=[
                        system(
                            "Translate the following content to ",
                            language_name,
                            ". Return only the translated text.",
                        ),
                        user(start.ref("content")),
                    ],
                ),
            ),
        )

    output = translation_nodes[0].then(
        "output",
        EndNodeData(
            title="Output",
            outputs=[
                node.ref("text").output(output_name)
                for node, (_, _, output_name) in zip(
                    translation_nodes,
                    TARGET_LANGUAGES,
                    strict=True,
                )
            ],
        ),
    )

    for node in translation_nodes[1:]:
        node.connect(output)

    return workflow.build()


def write_stream_chunk(
    event: object,
    *,
    stream_writer: TranslationStreamWriter,
) -> bool:
    return stream_writer.write_event(event)


def _execute_workflow(
    content: str,
    *,
    stream_output: IO[str] | None = None,
) -> tuple[dict[str, str], bool]:
    openai_slim_support.load_default_env_file(EXAMPLE_DIR)
    runtime, provider = openai_slim_support.build_runtime(example_dir=EXAMPLE_DIR)
    workflow_id = "parallel-translation-workflow"
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
    graph_runtime_state.variable_pool.add(("start", "content"), content)

    prepared_llm = SlimPreparedLLM(
        runtime=runtime,
        provider=provider,
        model_name="gpt-5.4",
        credentials={
            "openai_api_key": openai_slim_support.require_env(
                "OPENAI_API_KEY",
                example_dir=EXAMPLE_DIR,
            ),
        },
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
    stream_writer = (
        TranslationStreamWriter(stream_output=stream_output)
        if stream_output is not None
        else None
    )
    for event in engine.run():
        if stream_writer is not None and write_stream_chunk(
            event,
            stream_writer=stream_writer,
        ):
            streamed = True

    outputs: dict[str, str] = {}
    for _node_id, _language_name, output_name in TARGET_LANGUAGES:
        value = graph_runtime_state.get_output(output_name)
        if not isinstance(value, str):
            msg = f"Workflow did not produce output {output_name!r}."
            raise TypeError(msg)
        outputs[output_name] = value

    return outputs, streamed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a parallel translation workflow built with WorkflowBuilder.",
    )
    parser.add_argument(
        "content",
        nargs="?",
        default="Graph execution is a coordination problem.",
        help="Input content to translate.",
    )
    parser.add_argument(
        "--stream",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stream translations as they are produced.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stream_output = sys.stdout if args.stream else None
    if args.stream:
        sys.stdout.write("Streaming translations:\n")
        sys.stdout.flush()
    outputs, streamed = _execute_workflow(args.content, stream_output=stream_output)
    if args.stream and streamed:
        sys.stdout.write("\n")
    sys.stdout.write("Structured outputs:\n")
    for _node_id, language_name, output_name in TARGET_LANGUAGES:
        sys.stdout.write(f"- {language_name}: {outputs[output_name]}\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
