from __future__ import annotations

import time
from collections.abc import Mapping
from typing import cast

from graphon.entities.base_node_data import BaseNodeData
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.nodes.base.entities import OutputVariableType, VariableSelector
from graphon.nodes.base.node import Node
from graphon.nodes.end.end_node import EndNode
from graphon.nodes.end.entities import EndNodeData
from graphon.nodes.llm import LLMNodeData, ModelConfig
from graphon.nodes.llm.runtime_protocols import PreparedLLMProtocol
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.template_transform.entities import TemplateTransformNodeData
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from graphon.nodes.variable_aggregator.entities import VariableAggregatorNodeData
from graphon.nodes.variable_aggregator.variable_aggregator_node import (
    VariableAggregatorNode,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.template_rendering import Jinja2TemplateRenderer
from graphon.variables.input_entities import VariableEntityType
from graphon.workflow_builder import (
    NodeMaterializationContext,
    NodeOutputRef,
    WorkflowBuilder,
    WorkflowRuntime,
    completion_prompt,
    input_variable,
    paragraph_input,
    system,
    template,
    user,
)


class _EchoTemplateRenderer(Jinja2TemplateRenderer):
    def render_template(self, template: str, variables: Mapping[str, object]) -> str:
        return template.replace("{{ content }}", str(variables["content"]))


def test_llm_node_data_defaults_context_to_disabled() -> None:
    node_data = LLMNodeData(
        model=ModelConfig(
            provider="mock",
            name="mock-chat",
            mode=LLMMode.CHAT,
        ),
        prompt_template=[system("Translate this text.")],
    )

    assert node_data.context.enabled is False
    assert node_data.context.variable_selector is None


def test_workflow_builder_builds_parallel_translation_workflow() -> None:
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.time(),
    )
    builder = WorkflowBuilder()

    start = builder.root(
        "start",
        StartNodeData(
            variables=[paragraph_input("content", required=True)],
        ),
    )

    translation_model = ModelConfig(
        provider="mock",
        name="mock-chat",
        mode=LLMMode.CHAT,
    )

    chinese = start.then(
        "translate_zh",
        LLMNodeData(
            model=translation_model,
            prompt_template=[
                system("Translate the following text to Chinese."),
                user(start.ref("content")),
            ],
        ),
    )
    english = start.then(
        "translate_en",
        LLMNodeData(
            model=translation_model,
            prompt_template=[
                system("Translate the following text to English."),
                user(start.ref("content")),
            ],
        ),
    )
    japanese = start.then(
        "translate_ja",
        LLMNodeData(
            model=translation_model,
            prompt_template=[
                system("Translate the following text to Japanese."),
                user(start.ref("content")),
            ],
        ),
    )

    output = chinese.then(
        "output",
        EndNodeData(
            outputs=[
                chinese.ref("text").output("chinese"),
                english.ref("text").output("english"),
                japanese.ref("text").output("japanese"),
            ],
        ),
    )
    english.connect(output)
    japanese.connect(output)

    workflow = builder.build()
    graph = workflow.materialize(
        WorkflowRuntime(
            workflow_id="parallel-translation",
            graph_runtime_state=graph_runtime_state,
            prepared_llm=cast(PreparedLLMProtocol, object()),
        ),
    )

    assert graph.root_node.id == "start"
    assert isinstance(graph.nodes["output"], EndNode)
    assert sorted((edge.tail, edge.head) for edge in graph.edges.values()) == [
        ("start", "translate_en"),
        ("start", "translate_ja"),
        ("start", "translate_zh"),
        ("translate_en", "output"),
        ("translate_ja", "output"),
        ("translate_zh", "output"),
    ]

    output_node = cast(EndNode, graph.nodes["output"])
    assert [item.variable for item in output_node.node_data.outputs] == [
        "chinese",
        "english",
        "japanese",
    ]
    assert [tuple(item.value_selector) for item in output_node.node_data.outputs] == [
        ("translate_zh", "text"),
        ("translate_en", "text"),
        ("translate_ja", "text"),
    ]


def test_workflow_builder_helpers_produce_typed_authoring_values() -> None:
    ref = NodeOutputRef(node_id="llm", output_name="text")
    prompt = completion_prompt("Answer in one sentence: ", ref)
    binding = ref.output("answer", value_type=OutputVariableType.STRING)
    text = input_variable(
        "question",
        variable_type=VariableEntityType.TEXT_INPUT,
        required=True,
        max_length=512,
    )

    assert template("Result: ", ref) == "Result: {{#llm.text#}}"
    assert prompt.text == "Answer in one sentence: {{#llm.text#}}"
    assert binding.variable == "answer"
    assert tuple(binding.value_selector) == ("llm", "text")
    assert binding.value_type is OutputVariableType.STRING
    assert text.variable == "question"
    assert text.type is VariableEntityType.TEXT_INPUT
    assert text.max_length == 512


def test_workflow_builder_materializes_non_example_builtin_node() -> None:
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.time(),
    )
    builder = WorkflowBuilder()

    start = builder.root(
        "start",
        StartNodeData(
            variables=[paragraph_input("content", required=True)],
        ),
    )
    aggregate = start.then(
        "aggregate",
        VariableAggregatorNodeData(
            output_type="string",
            variables=[["start", "content"]],
        ),
    )
    aggregate.then(
        "output",
        EndNodeData(outputs=[]),
    )

    graph = builder.build().materialize(
        WorkflowRuntime(
            workflow_id="aggregate",
            graph_runtime_state=graph_runtime_state,
        ),
    )

    assert isinstance(graph.nodes["aggregate"], VariableAggregatorNode)


def test_workflow_builder_supports_runtime_kwargs_for_service_nodes() -> None:
    graph_runtime_state = GraphRuntimeState(
        variable_pool=VariablePool(),
        start_at=time.time(),
    )
    builder = WorkflowBuilder()

    start = builder.root(
        "start",
        StartNodeData(
            variables=[paragraph_input("content", required=True)],
        ),
    )
    transform = start.then(
        "transform",
        TemplateTransformNodeData(
            variables=[
                VariableSelector(
                    variable="content",
                    value_selector=("start", "content"),
                ),
            ],
            template="{{ content }}",
        ),
    )
    transform.then(
        "output",
        EndNodeData(outputs=[]),
    )

    def node_kwargs_factory(
        context: NodeMaterializationContext[BaseNodeData],
        node_cls: type[Node],
    ) -> Mapping[str, object]:
        _ = context
        if node_cls is TemplateTransformNode:
            return {"jinja2_template_renderer": _EchoTemplateRenderer()}
        return {}

    graph = builder.build().materialize(
        WorkflowRuntime(
            workflow_id="transform",
            graph_runtime_state=graph_runtime_state,
            node_kwargs_factory=node_kwargs_factory,
        ),
    )

    assert isinstance(graph.nodes["transform"], TemplateTransformNode)
