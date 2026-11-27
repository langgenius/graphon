"""Declarative child graphs for the filter; no executable nodes or factories.

Root sessions use their existing Python node interfaces. Child sessions use
these definitions to select templates and follow frame-qualified traversal.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from graphon.engine_events.frame import FrameStartedEvent
from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import (
    BuiltinNodeTypes,
    ErrorStrategy,
    NodeExecutionType,
    NodeState,
    NodeType,
)
from graphon.graph.scoping import resolve_container_id
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.base.node import Node
from graphon.nodes.base.template import Template
from graphon.nodes.end.entities import EndNodeData
from graphon.runtime.runtime_state import GraphProtocol

from .snapshot import Selector


@dataclass
class _NodeDefinition:
    id: str
    node_type: NodeType
    execution_type: NodeExecutionType
    # Mirrors ``Node.promoted_to_branch``: FAIL_BRANCH promoted this node only,
    # so its normal-path output still streams while it runs.
    promoted_to_branch: bool = False
    template: Template = field(default_factory=lambda: Template(segments=[]))
    write_selectors: set[Selector] = field(default_factory=set)
    state: NodeState = NodeState.UNKNOWN

    def get_streaming_template(self) -> Template:
        return self.template

    def blocks_variable_output(self, variable_selectors: set[Selector]) -> bool:
        return bool(self.write_selectors & variable_selectors)


@dataclass
class _EdgeDefinition:
    id: str
    tail: str
    head: str
    source_handle: str
    state: NodeState = NodeState.UNKNOWN


@dataclass
class _GraphDefinition:
    nodes: dict[str, _NodeDefinition]
    edges: dict[str, _EdgeDefinition]
    root_node: _NodeDefinition
    _outgoing_edges_by_node: dict[str, list[_EdgeDefinition]] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        self._outgoing_edges_by_node = {}
        for edge in self.edges.values():
            self._outgoing_edges_by_node.setdefault(edge.tail, []).append(edge)

    def get_outgoing_edges(self, node_id: str) -> Sequence[_EdgeDefinition]:
        return self._outgoing_edges_by_node.get(node_id, ())


def find_response_container_ids(graph_config: Mapping[str, Any] | None) -> set[str]:
    if not graph_config:
        return set()
    node_configs = {config["id"]: config for config in graph_config.get("nodes", [])}
    container_ids: set[str] = set()
    for config in node_configs.values():
        data = config["data"]
        node_type = data.type if isinstance(data, BaseNodeData) else data["type"]
        if node_type not in {BuiltinNodeTypes.ANSWER, BuiltinNodeTypes.END}:
            continue
        descendant_config = config
        while container_id := resolve_container_id(
            descendant_config, nodes_by_id=node_configs
        ):
            if container_id in container_ids:
                break
            container_ids.add(container_id)
            if container_id not in node_configs:
                break
            descendant_config = node_configs[container_id]
    return container_ids


def _build_node_definition(node_id: str, data: Mapping[str, Any]) -> _NodeDefinition:
    node_type = data["type"]
    node_class = (
        Node
        .get_node_type_classes_mapping()
        .get(node_type, {})
        .get(str(data.get("version", "1")))
    )
    # Unknown host node behavior must wait for traversal rather than promise a reply.
    execution_type = (
        node_class.execution_type if node_class else NodeExecutionType.BRANCH
    )
    promoted_to_branch = False
    if data.get("error_strategy") == ErrorStrategy.FAIL_BRANCH:
        promoted_to_branch = execution_type is not NodeExecutionType.BRANCH
        execution_type = NodeExecutionType.BRANCH
    node = _NodeDefinition(
        node_id,
        node_type,
        execution_type,
        promoted_to_branch=promoted_to_branch,
    )
    if node_type == BuiltinNodeTypes.ANSWER:
        node.template = Template.from_answer_template(
            AnswerNodeData.model_validate(data).answer
        )
    elif node_type == BuiltinNodeTypes.END:
        end_data = EndNodeData.model_validate(data)
        node.template = Template.from_end_outputs([
            output.model_dump() for output in end_data.outputs
        ])
    elif node_type == BuiltinNodeTypes.VARIABLE_ASSIGNER:
        node.write_selectors = {
            tuple(item["variable_selector"]) for item in data.get("items", [])
        }
        if assigned_selector := data.get("assigned_variable_selector"):
            node.write_selectors.add(tuple(assigned_selector))
    elif (
        node_class
        and node_class.blocks_variable_output is not Node.blocks_variable_output
    ):
        # A host's instance-specific write dependencies are not declarative.
        node.execution_type = NodeExecutionType.BRANCH
    return node


def build_child_graph(
    graph_config: Mapping[str, Any] | None, start: FrameStartedEvent
) -> GraphProtocol | None:
    if not graph_config:
        return None
    node_configs = {config["id"]: config for config in graph_config.get("nodes", [])}
    nodes: dict[str, _NodeDefinition] = {}
    for node_id, config in node_configs.items():
        if (
            node_id not in start.node_execution_ids
            or resolve_container_id(config, nodes_by_id=node_configs)
            != start.container_id
        ):
            continue
        data = config["data"]
        if isinstance(data, BaseNodeData):
            data = data.model_dump(mode="python")
        nodes[node_id] = _build_node_definition(node_id, data)
    root_node_id = start.root_node_id
    if root_node_id is None and (container := node_configs.get(start.container_id)):
        data = container["data"]
        root_node_id = (
            data.get("start_node_id")
            if isinstance(data, Mapping)
            else getattr(data, "start_node_id", None)
        )
    if root_node_id not in nodes:
        return None
    edges = {
        config["id"]: _EdgeDefinition(
            config["id"],
            config["source"],
            config["target"],
            config.get("sourceHandle", "source"),
        )
        for config in graph_config.get("edges", [])
        if config["source"] in nodes and config["target"] in nodes
    }
    return cast(GraphProtocol, _GraphDefinition(nodes, edges, nodes[root_node_id]))
