"""Minimal Graphon `start -> LLM -> output` workflow example for Slim.

Run from this directory:

    python3 workflow.py "Explain Graphon in one short sentence."

The script automatically loads `.env` from this example directory.
Existing environment variables take precedence over `.env` values.
"""

from __future__ import annotations

import argparse
import sys
import time
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
from graphon.graph_engine.command_channels import InMemoryChannel
from graphon.graph_engine.graph_engine import GraphEngine
from graphon.graph_events.node import NodeRunStreamChunkEvent
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.slim import SlimPreparedLLM, SlimRuntime
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.llm import LLMNodeData, ModelConfig
from graphon.nodes.start.entities import StartNodeData
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.workflow_builder import (
    WorkflowBuilder,
    WorkflowRuntime,
    WorkflowSpec,
    paragraph_input,
    system,
    template,
    user,
)

STREAM_SELECTOR = ("llm", "text")


def build_runtime() -> tuple[SlimRuntime, str]:
    runtime, provider = openai_slim_support.build_runtime(example_dir=EXAMPLE_DIR)
    return runtime, provider


def build_workflow(*, provider: str) -> WorkflowSpec:
    workflow = WorkflowBuilder()
    start = workflow.root(
        "start",
        StartNodeData(
            title="Start",
            variables=[paragraph_input("query", required=True)],
        ),
    )
    llm = start.then(
        "llm",
        LLMNodeData(
            title="LLM",
            model=ModelConfig(
                provider=provider,
                name="gpt-5.4",
                mode=LLMMode.CHAT,
            ),
            prompt_template=[
                system("You are a concise assistant."),
                user(start.ref("query")),
            ],
        ),
    )
    llm.then(
        "output",
        AnswerNodeData(
            title="Output",
            answer=template(llm.ref("text")),
        ),
    )
    return workflow.build()


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
    openai_slim_support.load_default_env_file(EXAMPLE_DIR)
    runtime, provider = build_runtime()
    workflow_id = "example-start-llm-output"
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.time(),
    )
    graph_runtime_state.variable_pool.add(("start", "query"), query)

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
    graph = build_workflow(provider=provider).materialize(
        WorkflowRuntime(
            workflow_id=workflow_id,
            graph_runtime_state=graph_runtime_state,
            prepared_llm=prepared_llm,
        ),
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
