from collections.abc import Mapping, Sequence
from typing import Any, NoReturn, override

from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.nodes.base.node import Node
from graphon.nodes.iteration.entities import ErrorHandleMode, IterationNodeData


class IterationNode(Node[IterationNodeData]):
    """Iteration node definition.

    Iteration execution is interpreted by GraphEngine. The node keeps only its
    configuration and static variable-mapping behavior.
    """

    node_type = BuiltinNodeTypes.ITERATION
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    @override
    def get_default_config(
        cls,
        filters: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        _ = filters
        return {
            "type": "iteration",
            "config": {
                "is_parallel": False,
                "parallel_nums": 10,
                "error_handle_mode": ErrorHandleMode.TERMINATED,
                "flatten_output": True,
            },
        }

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> NoReturn:
        msg = "Iteration nodes are interpreted by GraphEngine."
        raise RuntimeError(msg)

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: IterationNodeData,
    ) -> Mapping[str, Sequence[str]]:
        variable_mapping: dict[str, Sequence[str]] = {
            f"{node_id}.input_selector": node_data.iterator_selector,
        }
        iteration_node_ids = set()

        nodes = graph_config.get("nodes", [])
        for node in nodes:
            node_config_data = node.get("data", {})
            if node_config_data.get("iteration_id") == node_id:
                in_iteration_node_id = node.get("id")
                if in_iteration_node_id:
                    iteration_node_ids.add(in_iteration_node_id)

        node_configs = {
            node["id"]: node for node in graph_config.get("nodes", []) if "id" in node
        }
        for sub_node_id, sub_node_config in node_configs.items():
            if sub_node_config.get("data", {}).get("iteration_id") != node_id:
                continue

            sub_node_variable_mapping = cls._extract_mapping_from_node_config(
                graph_config=graph_config,
                config=sub_node_config,
            )

            variable_mapping.update({
                sub_node_id + "." + key: value
                for key, value in sub_node_variable_mapping.items()
                if value[0] != node_id
            })

        return {
            key: value
            for key, value in variable_mapping.items()
            if value[0] not in iteration_node_ids
        }
