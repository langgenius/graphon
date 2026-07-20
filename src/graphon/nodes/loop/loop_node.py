import json
import logging
from collections.abc import Generator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, assert_never, override

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionStatus,
)
from graphon.node_events.base import NodeEventBase, NodeRunResult
from graphon.node_events.loop import (
    LoopFailedEvent,
    LoopNextEvent,
    LoopStartedEvent,
    LoopSucceededEvent,
)
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    ContainerRunResult,
    LoopFrameRequest,
    build_container_value,
)
from graphon.nodes.loop.entities import LoopNodeData
from graphon.variables.factory import (
    TypeMismatchError,
    build_segment_with_type,
    segment_to_variable,
)
from graphon.variables.segments import Segment
from graphon.variables.types import SegmentType

logger = logging.getLogger(__name__)


class LoopNode(Node[LoopNodeData]):
    """Loop node definition.

    Loop execution is interpreted by GraphEngine. The node keeps only its
    configuration, loop-variable initialization, and static variable mapping.
    """

    node_type = BuiltinNodeTypes.LOOP
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(
        self,
    ) -> Generator[NodeEventBase | LoopFrameRequest, None, None]:
        loop_count = self.node_data.loop_count
        inputs: dict[str, object] = {"loop_count": loop_count}
        root_node_id = self.node_data.start_node_id
        loop_variable_selectors = self._initialize_loop_variables(inputs=inputs)
        loop_node_ids = self._extract_loop_node_ids_from_config(
            self.graph_config,
            self._node_id,
        )
        started_at = datetime.now(UTC).replace(tzinfo=None)
        yield LoopStartedEvent(
            start_at=started_at,
            inputs=inputs,
            metadata={"loop_length": loop_count},
        )
        yield LoopFrameRequest(
            inputs={key: build_container_value(value) for key, value in inputs.items()},
            outputs={},
            loop_count=loop_count,
            root_node_id=root_node_id,
            loop_variable_selectors=loop_variable_selectors,
            loop_node_ids=frozenset(loop_node_ids),
            index=0,
        )

    @override
    def _resume_container_events(
        self,
        *,
        result: ContainerRunResult,
    ) -> Generator[NodeEventBase | LoopFrameRequest, None, None]:
        if isinstance(result, LoopFrameRequest):
            yield LoopNextEvent(
                index=result.index,
                pre_loop_output={
                    key: value.to_object() for key, value in result.outputs.items()
                },
            )
            yield result
            return

        if isinstance(result, ContainerExecutionResult):
            container_result = result.node_run_result
            node_run_result = NodeRunResult(
                status=container_result.status,
                inputs={
                    key: value.to_object()
                    for key, value in container_result.inputs.items()
                },
                outputs={
                    key: value.to_object()
                    for key, value in container_result.outputs.items()
                },
                metadata=container_result.metadata,
                llm_usage=container_result.llm_usage,
                error=container_result.error,
                error_type=container_result.error_type,
            )
            if node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED:
                yield LoopSucceededEvent(
                    start_at=self._start_at,
                    inputs=node_run_result.inputs,
                    outputs=node_run_result.outputs,
                    metadata=result.metadata,
                    steps=result.steps,
                )
            elif node_run_result.status == WorkflowNodeExecutionStatus.FAILED:
                yield LoopFailedEvent(
                    start_at=self._start_at,
                    inputs=node_run_result.inputs,
                    outputs=node_run_result.outputs,
                    metadata=result.metadata,
                    steps=result.steps,
                    error=node_run_result.error,
                )
            else:
                msg = f"Unsupported loop status {node_run_result.status}"
                raise ValueError(msg)
            yield StreamCompletedEvent(node_run_result=node_run_result)
            return

        msg = f"Unsupported loop result {type(result).__name__}"
        raise TypeError(msg)

    def _initialize_loop_variables(
        self,
        *,
        inputs: dict[str, Any],
    ) -> dict[str, tuple[str, ...]]:
        loop_variable_selectors: dict[str, tuple[str, ...]] = {}
        if not self.node_data.loop_variables:
            return loop_variable_selectors

        for loop_variable in self.node_data.loop_variables:
            match loop_variable.value_type:
                case "constant":
                    processed_segment = self._get_segment_for_constant(
                        var_type=loop_variable.var_type,
                        original_value=loop_variable.value,
                    )
                case "variable":
                    if not isinstance(loop_variable.value, list):
                        msg = f"Invalid value for loop variable {loop_variable.label}"
                        raise TypeError(msg)
                    processed_segment = self.graph_runtime_state.variable_pool.get(
                        loop_variable.value,
                    )
                case _:
                    assert_never(loop_variable.value_type)

            if processed_segment is None:
                msg = f"Invalid value for loop variable {loop_variable.label}"
                raise ValueError(msg)

            variable_selector = (self._node_id, loop_variable.label)
            variable = segment_to_variable(
                segment=processed_segment,
                selector=variable_selector,
            )
            self.graph_runtime_state.variable_pool.add(
                variable_selector,
                variable.value,
            )
            loop_variable_selectors[loop_variable.label] = variable_selector
            inputs[loop_variable.label] = processed_segment.value
        return loop_variable_selectors

    @classmethod
    @override
    def _extract_variable_selector_to_variable_mapping(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_id: str,
        node_data: LoopNodeData,
    ) -> Mapping[str, Sequence[str]]:
        variable_mapping = {}
        loop_node_ids = cls._extract_loop_node_ids_from_config(graph_config, node_id)

        node_configs = {
            node["id"]: node for node in graph_config.get("nodes", []) if "id" in node
        }
        for sub_node_id, sub_node_config in node_configs.items():
            if sub_node_config.get("data", {}).get("loop_id") != node_id:
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

        for loop_variable in node_data.loop_variables:
            if loop_variable.value_type == "variable":
                if not isinstance(loop_variable.value, list):
                    msg = "Loop variable value must be a selector for variable type"
                    raise TypeError(msg)
                variable_mapping[f"{node_id}.{loop_variable.label}"] = (
                    loop_variable.value
                )

        return {
            key: value
            for key, value in variable_mapping.items()
            if value[0] not in loop_node_ids
        }

    @classmethod
    def _extract_loop_node_ids_from_config(
        cls,
        graph_config: Mapping[str, Any],
        loop_node_id: str,
    ) -> set[str]:
        loop_node_ids = set()
        nodes = graph_config.get("nodes", [])
        for node in nodes:
            node_data = node.get("data", {})
            if node_data.get("loop_id") == loop_node_id:
                node_id = node.get("id")
                if node_id:
                    loop_node_ids.add(node_id)
        return loop_node_ids

    @staticmethod
    def _get_segment_for_constant(
        var_type: SegmentType,
        original_value: Any,
    ) -> Segment:
        value = LoopNode._deserialize_constant_value(
            var_type=var_type,
            original_value=original_value,
        )
        try:
            return build_segment_with_type(var_type, value=value)
        except TypeMismatchError as type_exc:
            if not isinstance(original_value, str):
                raise
            try:
                value = json.loads(original_value)
            except ValueError:
                raise type_exc from None
            return build_segment_with_type(var_type, value)

    @staticmethod
    def _deserialize_constant_value(
        *,
        var_type: SegmentType,
        original_value: Any,
    ) -> Any:
        match var_type:
            case (
                SegmentType.NUMBER
                | SegmentType.INTEGER
                | SegmentType.FLOAT
                | SegmentType.STRING
                | SegmentType.OBJECT
                | SegmentType.SECRET
                | SegmentType.FILE
                | SegmentType.BOOLEAN
                | SegmentType.NONE
                | SegmentType.GROUP
                | SegmentType.ARRAY_BOOLEAN
            ):
                return original_value
            case (
                SegmentType.ARRAY_NUMBER
                | SegmentType.ARRAY_OBJECT
                | SegmentType.ARRAY_STRING
            ):
                if original_value and isinstance(original_value, str):
                    return json.loads(original_value)
                logger.warning(
                    "unexpected value for LoopNode, value_type=%s, value=%s",
                    original_value,
                    var_type,
                )
                return []
            case SegmentType.ARRAY_ANY | SegmentType.ARRAY_FILE:
                msg = "this statement should be unreachable."
                raise AssertionError(msg)
            case _:
                assert_never(var_type)
