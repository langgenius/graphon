import json
import logging
from collections.abc import Generator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, assert_never, cast, override

from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.node_events.base import NodeEventBase
from graphon.node_events.loop import (
    LoopFailedEvent,
    LoopNextEvent,
    LoopStartedEvent,
    LoopSucceededEvent,
)
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import (
    ContainerRunResult,
    LoopExecutionFailed,
    LoopExecutionSucceeded,
    LoopFrameCompleted,
    LoopFrameRequest,
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
    ) -> Generator[NodeEventBase | LoopFrameRequest, ContainerRunResult, None]:
        loop_count = self.node_data.loop_count
        inputs: dict[str, object] = {"loop_count": loop_count}
        root_node_id, loop_variable_selectors, loop_node_ids = self.initialize_loop_run(
            inputs=inputs,
        )
        started_at = datetime.now(UTC).replace(tzinfo=None)
        yield LoopStartedEvent(
            start_at=started_at,
            inputs=inputs,
            metadata={"loop_length": loop_count},
        )
        result = yield LoopFrameRequest(
            kind="loop",
            started_at=started_at,
            inputs=inputs,
            loop_count=loop_count,
            root_node_id=root_node_id,
            loop_variable_selectors=loop_variable_selectors,
            loop_node_ids=frozenset(loop_node_ids),
            index=0,
        )
        while isinstance(result, LoopFrameCompleted):
            yield LoopNextEvent(
                index=result.next_index,
                pre_loop_output=self.node_data.outputs,
            )
            result = yield LoopFrameRequest(
                kind="loop",
                started_at=started_at,
                inputs=inputs,
                loop_count=loop_count,
                root_node_id=root_node_id,
                loop_variable_selectors=loop_variable_selectors,
                loop_node_ids=frozenset(loop_node_ids),
                index=result.next_index,
            )

        if isinstance(result, LoopExecutionSucceeded):
            yield LoopSucceededEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
            )
        elif isinstance(result, LoopExecutionFailed):
            yield LoopFailedEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
                error=result.error,
            )
        else:
            msg = f"Unsupported loop result {type(result).__name__}"
            raise TypeError(msg)
        yield StreamCompletedEvent(node_run_result=result.node_run_result)

    @override
    def _resume_container_events(
        self,
        *,
        phase_data: Mapping[str, object],
        result: ContainerRunResult,
    ) -> Generator[NodeEventBase | LoopFrameRequest, None, None]:
        if isinstance(result, LoopFrameCompleted):
            yield LoopNextEvent(
                index=result.next_index,
                pre_loop_output=self.node_data.outputs,
            )
            yield LoopFrameRequest(
                kind="loop",
                started_at=self._start_at,
                inputs=cast(Mapping[str, object], phase_data["inputs"]),
                loop_count=cast(int, phase_data["loop_count"]),
                root_node_id=cast(str, phase_data["root_node_id"]),
                loop_variable_selectors=cast(
                    Mapping[str, Sequence[str]],
                    phase_data["loop_variable_selectors"],
                ),
                loop_node_ids=frozenset(
                    cast(Sequence[str], phase_data["loop_node_ids"]),
                ),
                index=result.next_index,
            )
            return

        if isinstance(result, LoopExecutionSucceeded):
            yield LoopSucceededEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
            )
            yield StreamCompletedEvent(node_run_result=result.node_run_result)
            return

        if isinstance(result, LoopExecutionFailed):
            yield LoopFailedEvent(
                start_at=result.started_at,
                inputs=result.inputs,
                outputs=result.outputs,
                metadata=result.metadata,
                steps=result.steps,
                error=result.error,
            )
            yield StreamCompletedEvent(node_run_result=result.node_run_result)
            return

        msg = f"Unsupported loop result {type(result).__name__}"
        raise TypeError(msg)

    def initialize_loop_run(
        self,
        *,
        inputs: dict[str, Any],
    ) -> tuple[str, dict[str, list[str]], set[str]]:
        if not self.node_data.start_node_id:
            msg = f"field start_node_id in loop {self._node_id} not found"
            raise ValueError(msg)

        loop_variable_selectors = self._initialize_loop_variables(inputs=inputs)
        loop_node_ids = self._extract_loop_node_ids_from_config(
            self.graph_config,
            self._node_id,
        )
        return self.node_data.start_node_id, loop_variable_selectors, loop_node_ids

    def _initialize_loop_variables(
        self,
        *,
        inputs: dict[str, Any],
    ) -> dict[str, list[str]]:
        loop_variable_selectors: dict[str, list[str]] = {}
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
                    processed_segment = (
                        self.graph_runtime_state.variable_pool.get(loop_variable.value)
                        if isinstance(loop_variable.value, list)
                        else None
                    )
                case _:
                    assert_never(loop_variable.value_type)

            if not processed_segment:
                msg = f"Invalid value for loop variable {loop_variable.label}"
                raise ValueError(msg)

            variable_selector = [self._node_id, loop_variable.label]
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

        for loop_variable in node_data.loop_variables or []:
            if loop_variable.value_type == "variable":
                if loop_variable.value is None:
                    msg = "Loop variable value must be provided for variable type"
                    raise ValueError(msg)
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
