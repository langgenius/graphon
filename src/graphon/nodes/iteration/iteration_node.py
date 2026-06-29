from collections.abc import Generator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, cast, override

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.node_events.base import NodeEventBase, NodeRunResult
from graphon.node_events.iteration import (
    IterationFailedEvent,
    IterationNextEvent,
    IterationStartedEvent,
    IterationSucceededEvent,
)
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerRunResult,
    IterationExecutionFailed,
    IterationExecutionSucceeded,
    IterationFrameRequest,
    IterationFramesRequested,
)
from graphon.nodes.iteration.entities import ErrorHandleMode, IterationNodeData
from graphon.nodes.iteration.exc import (
    InvalidIteratorValueError,
    IteratorVariableNotFoundError,
    StartNodeIdNotFoundError,
)
from graphon.variables.segments import ArrayAnySegment, ArraySegment, NoneSegment


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
    def _run(
        self,
    ) -> Generator[NodeEventBase | IterationFrameRequest, ContainerRunResult, None]:
        variable = self.graph_runtime_state.variable_pool.get(
            self.node_data.iterator_selector,
        )
        if variable is None:
            msg = f"iterator variable {self.node_data.iterator_selector} not found"
            raise IteratorVariableNotFoundError(msg)
        started_at = datetime.now(UTC).replace(tzinfo=None)
        if isinstance(variable, NoneSegment) or (
            isinstance(variable, ArraySegment) and len(variable.value) == 0
        ):
            yield from self._run_empty_iteration(
                variable=variable,
                started_at=started_at,
            )
            return
        iterator_value = self._resolve_iterator_value(variable)
        root_node_id = self._resolve_start_node_id()

        inputs = {"iterator_selector": iterator_value}
        yield IterationStartedEvent(
            start_at=started_at,
            inputs=inputs,
            metadata={"iteration_length": len(iterator_value)},
        )
        indexes = self._initial_iteration_indexes(item_count=len(iterator_value))
        for index in indexes:
            yield IterationNextEvent(index=index)
        result = yield IterationFrameRequest(
            started_at=started_at,
            inputs=inputs,
            items=tuple(iterator_value),
            root_node_id=root_node_id,
            indexes=indexes,
            output_selector=self.node_data.output_selector,
            error_handle_mode=self.node_data.error_handle_mode,
            flatten_output=self.node_data.flatten_output,
            parallel_nums=self._parallel_limit(),
        )
        while isinstance(result, IterationFramesRequested):
            for index in result.indexes:
                yield IterationNextEvent(index=index)
            result = yield IterationFrameRequest(
                started_at=started_at,
                inputs=inputs,
                items=tuple(iterator_value),
                root_node_id=root_node_id,
                indexes=result.indexes,
                output_selector=self.node_data.output_selector,
                error_handle_mode=self.node_data.error_handle_mode,
                flatten_output=self.node_data.flatten_output,
                parallel_nums=self._parallel_limit(),
            )

        if isinstance(result, IterationExecutionSucceeded):
            yield IterationSucceededEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
            )
        elif isinstance(result, IterationExecutionFailed):
            yield IterationFailedEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
                error=result.error,
            )
        else:
            msg = f"Unsupported iteration result {type(result).__name__}"
            raise TypeError(msg)
        yield StreamCompletedEvent(node_run_result=result.node_run_result)

    @override
    def _resume_container_events(
        self,
        *,
        phase_data: Mapping[str, object],
        result: ContainerRunResult,
    ) -> Generator[NodeEventBase | IterationFrameRequest, None, None]:
        if isinstance(result, IterationFramesRequested):
            for index in result.indexes:
                yield IterationNextEvent(index=index)
            yield IterationFrameRequest(
                started_at=self._start_at,
                inputs=cast(Mapping[str, object], phase_data["inputs"]),
                items=cast(tuple[object, ...], phase_data["items"]),
                root_node_id=cast(str, phase_data["root_node_id"]),
                indexes=result.indexes,
                output_selector=cast(Sequence[str], phase_data["output_selector"]),
                error_handle_mode=cast(
                    ErrorHandleMode,
                    phase_data["error_handle_mode"],
                ),
                flatten_output=cast(bool, phase_data["flatten_output"]),
                parallel_nums=cast(int, phase_data["parallel_nums"]),
            )
            return

        if isinstance(result, IterationExecutionSucceeded):
            yield IterationSucceededEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
            )
            yield StreamCompletedEvent(node_run_result=result.node_run_result)
            return

        if isinstance(result, IterationExecutionFailed):
            yield IterationFailedEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
                error=result.error,
            )
            yield StreamCompletedEvent(node_run_result=result.node_run_result)
            return

        msg = f"Unsupported iteration result {type(result).__name__}"
        raise TypeError(msg)

    def _resolve_iterator_value(self, variable: object) -> list[object]:
        if not isinstance(variable, ArraySegment):
            msg = f"invalid iterator value: {variable}, please provide a list."
            raise InvalidIteratorValueError(msg)
        iterator_value = variable.to_object()
        if not isinstance(iterator_value, list):
            msg = f"Invalid iterator value: {iterator_value}, please provide a list."
            raise InvalidIteratorValueError(msg)
        return cast(list[object], iterator_value)

    def _resolve_start_node_id(self) -> str:
        root_node_id = self.node_data.start_node_id
        if not root_node_id:
            msg = f"field start_node_id in iteration {self._node_id} not found"
            raise StartNodeIdNotFoundError(msg)
        return root_node_id

    def _initial_iteration_indexes(self, *, item_count: int) -> tuple[int, ...]:
        initial_count = min(self._parallel_limit(), item_count)
        return tuple(range(initial_count))

    def _parallel_limit(self) -> int:
        if self.node_data.is_parallel:
            return max(self.node_data.parallel_nums, 1)
        return 1

    def _run_empty_iteration(
        self,
        *,
        variable: NoneSegment | ArraySegment,
        started_at: datetime,
    ) -> Generator[NodeEventBase, ContainerRunResult, None]:
        outputs = {"output": ArrayAnySegment(value=[])}
        if isinstance(variable, ArraySegment):
            outputs = {"output": variable.model_copy(update={"value": []})}
        inputs: dict[str, object] = {"iterator_selector": []}
        yield IterationStartedEvent(
            start_at=started_at,
            inputs=inputs,
            metadata={"iteration_length": 0},
        )
        yield IterationSucceededEvent(
            start_at=started_at,
            inputs=inputs,
            outputs=outputs,
            metadata={},
            steps=0,
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs=outputs,
                inputs=inputs,
            ),
        )

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
