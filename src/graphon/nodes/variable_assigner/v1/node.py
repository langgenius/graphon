from collections.abc import Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast, override

from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.node_events.base import (
    NodeEventBase,
    NodeRunResult,
)
from graphon.node_events.node import (
    StreamCompletedEvent,
    VariableUpdatedEvent,
)
from graphon.nodes.base.node import Node
from graphon.nodes.variable_assigner.common import helpers as common_helpers
from graphon.nodes.variable_assigner.common.exc import VariableOperatorNodeError
from graphon.variables.types import SegmentType
from graphon.variables.variables import (
    Variable,
    VariableBase,
)

from .node_data import VariableAssignerData, WriteMode

if TYPE_CHECKING:
    from graphon.runtime.graph_runtime_state import GraphRuntimeState


class VariableAssignerNode(Node[VariableAssignerData]):
    node_type = BuiltinNodeTypes.VARIABLE_ASSIGNER

    @override
    def __init__(
        self,
        node_id: str,
        config: VariableAssignerData,
        *,
        graph_init_params: "GraphInitParams",
        graph_runtime_state: "GraphRuntimeState",
    ) -> None:
        super().__init__(
            node_id=node_id,
            config=config,
            graph_init_params=graph_init_params,
            graph_runtime_state=graph_runtime_state,
        )

    @override
    def blocks_variable_output(self, variable_selectors: set[tuple[str, ...]]) -> bool:
        """Check if this Variable Assigner node blocks the output of specific variables.

        Returns True if this node updates any of the requested conversation variables.

        Returns:
            `True` when the assigned selector is among the requested selectors.

        """
        assigned_selector = tuple(self.node_data.assigned_variable_selector)
        return assigned_selector in variable_selectors

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: VariableAssignerData,
    ) -> Mapping[str, Sequence[str]]:
        _ = graph_config
        mapping = {}
        selector_key = ".".join(node_data.assigned_variable_selector)
        key = f"{node_id}.#{selector_key}#"
        mapping[key] = node_data.assigned_variable_selector

        selector_key = ".".join(node_data.input_variable_selector)
        key = f"{node_id}.#{selector_key}#"
        mapping[key] = node_data.input_variable_selector
        return mapping

    @override
    def _run(self) -> Generator[NodeEventBase, None, None]:
        assigned_variable_selector = self.node_data.assigned_variable_selector
        # Should be String, Number, Object, ArrayString, ArrayNumber, ArrayObject
        original_variable = self.graph_runtime_state.variable_pool.get(
            assigned_variable_selector,
        )
        if not isinstance(original_variable, VariableBase):
            msg = "assigned variable not found"
            raise VariableOperatorNodeError(msg)

        match self.node_data.write_mode:
            case WriteMode.OVER_WRITE:
                income_value = self.graph_runtime_state.variable_pool.get(
                    self.node_data.input_variable_selector,
                )
                if not income_value:
                    msg = "input value not found"
                    raise VariableOperatorNodeError(msg)
                updated_variable = original_variable.model_copy(
                    update={"value": income_value.value},
                )

            case WriteMode.APPEND:
                income_value = self.graph_runtime_state.variable_pool.get(
                    self.node_data.input_variable_selector,
                )
                if not income_value:
                    msg = "input value not found"
                    raise VariableOperatorNodeError(msg)
                updated_value = [*original_variable.value, income_value.value]
                updated_variable = original_variable.model_copy(
                    update={"value": updated_value},
                )

            case WriteMode.CLEAR:
                income_value = SegmentType.get_zero_value(original_variable.value_type)
                updated_variable = original_variable.model_copy(
                    update={"value": income_value.to_object()},
                )

        updated_variables = [
            common_helpers.variable_to_processed_data(
                assigned_variable_selector,
                updated_variable,
            ),
        ]
        yield VariableUpdatedEvent(variable=cast("Variable", updated_variable))
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                inputs={
                    "value": income_value.to_object(),
                },
                # NOTE(QuantumGhost): although only one variable is updated in
                # `v1.VariableAssignerNode`, we still set `output_variables` as a
                # list to keep the output schema compatible with
                # `v2.VariableAssignerNode`.
                process_data=common_helpers.set_updated_variables(
                    {},
                    updated_variables,
                ),
                outputs={},
            ),
        )
