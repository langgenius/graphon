from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.slim_llm.settings import (
    DEFAULT_QUERY,
    OPENAI_MODEL,
    OPENAI_PLUGIN_ID,
    OPENAI_PROVIDER,
    load_credentials,
    openai_credentials,
    slim_client_config,
    use_local_slim_binary,
)
from graphon.dsl.slim import DslSlimPreparedLLM
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


class PromptSerializer:
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


def run(query: str) -> str:
    use_local_slim_binary()
    credentials = load_credentials()
    workflow_id = "slim-llm-code-example"
    graph_state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time.time())
    graph_state.variable_pool.add(("start", "query"), query)
    graph_init = GraphInitParams(
        workflow_id=workflow_id,
        graph_config={"nodes": [], "edges": []},
        run_context={},
        call_depth=0,
    )

    graph = build_graph(
        graph_init=graph_init,
        graph_state=graph_state,
        llm=DslSlimPreparedLLM(
            config=slim_client_config(credentials),
            plugin_id=OPENAI_PLUGIN_ID,
            provider=OPENAI_PROVIDER,
            model_name=OPENAI_MODEL,
            credentials=openai_credentials(credentials),
            parameters={},
        ),
    )
    engine = GraphEngine(
        workflow_id=workflow_id,
        graph=graph,
        graph_runtime_state=graph_state,
        command_channel=InMemoryChannel(),
    )

    list(engine.run())
    answer = graph_state.get_output("answer")
    if not isinstance(answer, str):
        msg = "Workflow finished without a string answer."
        raise TypeError(msg)
    return answer


def build_graph(
    *,
    graph_init: GraphInitParams,
    graph_state: GraphRuntimeState,
    llm: DslSlimPreparedLLM,
) -> Graph:
    start = StartNode(
        node_id="start",
        data=StartNodeData(title="Start"),
        graph_init_params=graph_init,
        graph_runtime_state=graph_state,
    )
    llm_node = LLMNode(
        node_id="llm",
        data=LLMNodeData(
            title="LLM",
            model=ModelConfig(
                provider=OPENAI_PROVIDER,
                name=OPENAI_MODEL,
                mode=LLMMode.CHAT,
            ),
            prompt_template=[
                LLMNodeChatModelMessage(
                    role=PromptMessageRole.USER,
                    text="{{#start.query#}}",
                ),
            ],
            context=ContextConfig(enabled=False),
        ),
        graph_init_params=graph_init,
        graph_runtime_state=graph_state,
        model_instance=llm,
        llm_file_saver=TextOnlyFileSaver(),
        prompt_message_serializer=PromptSerializer(),
    )
    answer = AnswerNode(
        node_id="answer",
        data=AnswerNodeData(title="Answer", answer="{{#llm.text#}}"),
        graph_init_params=graph_init,
        graph_runtime_state=graph_state,
    )

    return Graph.new().add_root(start).add_node(llm_node).add_node(answer).build()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", default=DEFAULT_QUERY)
    args = parser.parse_args()

    sys.stdout.write(f"{run(args.query)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
