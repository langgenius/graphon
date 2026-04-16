from collections.abc import Mapping, Sequence
from typing import Any, override

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.node_events.base import NodeRunResult
from graphon.nodes.base.node import Node
from graphon.nodes.if_else.entities import IfElseNodeData
from graphon.utils.condition.processor import ConditionProcessor


class IfElseNode(Node[IfElseNodeData]):
    node_type = BuiltinNodeTypes.IF_ELSE
    execution_type = NodeExecutionType.BRANCH

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> NodeRunResult:
        """Evaluate the configured cases and return the matching branch result."""
        node_inputs: dict[str, Sequence[Mapping[str, Any]]] = {"conditions": []}

        process_data: dict[str, list] = {"condition_results": []}

        input_conditions: Sequence[Mapping[str, Any]] = []
        final_result = False
        selected_case_id = "false"
        condition_processor = ConditionProcessor()
        try:
            uses_legacy_shape = self.node_data.cases is None
            for case in self.node_data.iter_cases():
                input_conditions, group_result, final_result = (
                    condition_processor.process_conditions(
                        variable_pool=self.graph_runtime_state.variable_pool,
                        conditions=case.conditions,
                        operator=case.logical_operator,
                    )
                )

                process_data["condition_results"].append({
                    "group": "default" if uses_legacy_shape else case.model_dump(),
                    "results": group_result,
                    "final_result": final_result,
                })

                # Break if a case passes (logical short-circuit)
                if final_result:
                    selected_case_id = "true" if uses_legacy_shape else case.case_id
                    break

            node_inputs["conditions"] = input_conditions

        except (TypeError, ValueError) as e:
            return NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                inputs=node_inputs,
                process_data=process_data,
                error=str(e),
            )

        outputs = {"result": final_result, "selected_case_id": selected_case_id}

        return NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            inputs=node_inputs,
            process_data=process_data,
            edge_source_handle=selected_case_id or "false",
            outputs=outputs,
        )

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: IfElseNodeData,
    ) -> Mapping[str, Sequence[str]]:
        var_mapping: dict[str, list[str]] = {}
        _ = graph_config  # Explicitly mark as unused
        for case in node_data.iter_cases():
            for condition in case.conditions:
                key = f"{node_id}.#{'.'.join(condition.variable_selector)}#"
                var_mapping[key] = condition.variable_selector

        return var_mapping
