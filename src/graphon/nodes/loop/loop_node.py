import contextlib
import json
import logging
from collections.abc import Generator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, TypedDict, assert_never, override

from graphon.entities.graph_config import NodeConfigDictAdapter
from graphon.entities.graph_init_params import GraphInitParams
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
)
from graphon.graph_events.node import NodeRunSucceededEvent
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import (
    NodeEventBase,
    NodeRunResult,
)
from graphon.node_events.loop import (
    LoopFailedEvent,
    LoopNextEvent,
    LoopStartedEvent,
    LoopSucceededEvent,
)
from graphon.node_events.node import StreamCompletedEvent
from graphon.nodes.base.node import Node
from graphon.nodes.base.usage_tracking_mixin import LLMUsageTrackingMixin
from graphon.nodes.loop.entities import (
    LoopCompletedReason,
    LoopNodeData,
)
from graphon.utils.condition.entities import Condition
from graphon.utils.condition.processor import ConditionProcessor
from graphon.variables.factory import (
    TypeMismatchError,
    build_segment_with_type,
    segment_to_variable,
)
from graphon.variables.segments import Segment
from graphon.variables.types import SegmentType

if TYPE_CHECKING:
    from graphon.graph_engine.graph_engine import GraphEngine

logger = logging.getLogger(__name__)
_DEFAULT_CHILD_ABORT_REASON = "child graph aborted"


class _IterationState(TypedDict, total=False):
    iteration_usage: LLMUsage
    reach_break_node: bool
    loop_duration: float
    single_loop_variable: dict[str, object]


class LoopNode(LLMUsageTrackingMixin, Node[LoopNodeData]):
    """Loop Node."""

    node_type = BuiltinNodeTypes.LOOP
    execution_type = NodeExecutionType.CONTAINER

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator:
        """Run the node."""
        loop_count = self.node_data.loop_count

        inputs = {"loop_count": loop_count}
        root_node_id, loop_variable_selectors, loop_node_ids = (
            self._initialize_loop_run(inputs=inputs)
        )

        start_at = datetime.now(UTC).replace(tzinfo=None)
        condition_processor = ConditionProcessor()
        loop_duration_map: dict[str, float] = {}
        single_loop_variable_map: dict[str, dict[str, object]] = {}
        loop_usage = LLMUsage.empty_usage()

        yield LoopStartedEvent(
            start_at=start_at,
            inputs=inputs,
            metadata={"loop_length": loop_count},
        )

        try:
            reach_break_condition = self._evaluate_break_conditions(
                condition_processor=condition_processor,
                break_conditions=self.node_data.break_conditions,
                logical_operator=self.node_data.logical_operator,
                suppress_errors=True,
            )
            if reach_break_condition:
                loop_count = 0

            for i in range(loop_count):
                iteration_state: _IterationState = {}
                try:
                    yield from self._execute_loop_iteration(
                        current_index=i,
                        root_node_id=root_node_id,
                        loop_node_ids=loop_node_ids,
                        loop_variable_selectors=loop_variable_selectors,
                        iteration_state=iteration_state,
                    )
                finally:
                    iteration_usage = iteration_state.get("iteration_usage")
                    if isinstance(iteration_usage, LLMUsage):
                        loop_usage = self._merge_usage(loop_usage, iteration_usage)

                if self._record_iteration_state(
                    current_index=i,
                    iteration_state=iteration_state,
                    loop_duration_map=loop_duration_map,
                    single_loop_variable_map=single_loop_variable_map,
                ):
                    break

                reach_break_condition = self._evaluate_break_conditions(
                    condition_processor=condition_processor,
                    break_conditions=self.node_data.break_conditions,
                    logical_operator=self.node_data.logical_operator,
                )
                if reach_break_condition:
                    break

                yield LoopNextEvent(
                    index=i + 1,
                    pre_loop_output=self.node_data.outputs,
                )

            self._accumulate_usage(loop_usage)
            yield from self._yield_loop_success_events(
                start_at=start_at,
                inputs=inputs,
                steps=loop_count,
                loop_usage=loop_usage,
                loop_duration_map=loop_duration_map,
                single_loop_variable_map=single_loop_variable_map,
                reach_break_condition=reach_break_condition,
            )

        except Exception as exc:
            logger.exception("Loop node %s failed", self._node_id)
            self._accumulate_usage(loop_usage)
            yield from self._yield_loop_failure_events(
                start_at=start_at,
                inputs=inputs,
                steps=loop_count,
                loop_usage=loop_usage,
                loop_duration_map=loop_duration_map,
                single_loop_variable_map=single_loop_variable_map,
                error=exc,
            )

    @staticmethod
    def _record_iteration_state(
        *,
        current_index: int,
        iteration_state: _IterationState,
        loop_duration_map: dict[str, float],
        single_loop_variable_map: dict[str, dict[str, object]],
    ) -> bool:
        loop_duration_map[str(current_index)] = float(iteration_state["loop_duration"])
        single_loop_variable_map[str(current_index)] = iteration_state[
            "single_loop_variable"
        ]
        return iteration_state["reach_break_node"]

    def _initialize_loop_run(
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

    def _evaluate_break_conditions(
        self,
        *,
        condition_processor: ConditionProcessor,
        break_conditions: Sequence[Condition] | None,
        logical_operator: Literal["and", "or"],
        suppress_errors: bool = False,
    ) -> bool:
        if not break_conditions:
            return False

        if suppress_errors:
            with contextlib.suppress(ValueError):
                _, _, reach_break_condition = condition_processor.process_conditions(
                    variable_pool=self.graph_runtime_state.variable_pool,
                    conditions=break_conditions,
                    operator=logical_operator,
                )
                return reach_break_condition
            return False

        _, _, reach_break_condition = condition_processor.process_conditions(
            variable_pool=self.graph_runtime_state.variable_pool,
            conditions=break_conditions,
            operator=logical_operator,
        )
        return reach_break_condition

    def _execute_loop_iteration(
        self,
        *,
        current_index: int,
        root_node_id: str,
        loop_node_ids: set[str],
        loop_variable_selectors: Mapping[str, Sequence[str]],
        iteration_state: _IterationState,
    ) -> Generator[
        NodeEventBase | GraphNodeEventBase,
        None,
        None,
    ]:
        self._clear_loop_subgraph_variables(loop_node_ids)
        graph_engine = self._create_graph_engine(root_node_id=root_node_id)
        loop_state = {"reach_break_node": False}
        loop_start_time = datetime.now(UTC).replace(tzinfo=None)
        try:
            yield from self._run_single_loop(
                graph_engine=graph_engine,
                current_index=current_index,
                loop_state=loop_state,
            )
        finally:
            iteration_state["iteration_usage"] = (
                graph_engine.graph_runtime_state.llm_usage
            )

        self._merge_iteration_outputs(graph_engine.graph_runtime_state.outputs)
        loop_duration = (
            datetime.now(UTC).replace(tzinfo=None) - loop_start_time
        ).total_seconds()
        iteration_state["reach_break_node"] = loop_state["reach_break_node"]
        iteration_state["loop_duration"] = loop_duration
        iteration_state["single_loop_variable"] = self._collect_loop_variable_values(
            loop_variable_selectors=loop_variable_selectors,
        )

    def _merge_iteration_outputs(self, outputs: Mapping[str, object]) -> None:
        self.graph_runtime_state.merge_response_outputs(outputs)

    def _collect_loop_variable_values(
        self,
        *,
        loop_variable_selectors: Mapping[str, Sequence[str]],
    ) -> dict[str, Any]:
        single_loop_variable = {}
        for key, selector in loop_variable_selectors.items():
            segment = self.graph_runtime_state.variable_pool.get(selector)
            single_loop_variable[key] = segment.value if segment else None
        return single_loop_variable

    def _yield_loop_success_events(
        self,
        *,
        start_at: datetime,
        inputs: Mapping[str, Any],
        steps: int,
        loop_usage: LLMUsage,
        loop_duration_map: Mapping[str, float],
        single_loop_variable_map: Mapping[str, dict[str, object]],
        reach_break_condition: bool,
    ) -> Generator[NodeEventBase, None, None]:
        metadata = {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: loop_usage.total_tokens,
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: loop_usage.total_price,
            WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
            WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: loop_duration_map,
            WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                single_loop_variable_map
            ),
        }
        yield LoopSucceededEvent(
            start_at=start_at,
            inputs=inputs,
            outputs=self.node_data.outputs,
            steps=steps,
            metadata={
                **metadata,
                WorkflowNodeExecutionMetadataKey.COMPLETED_REASON: (
                    LoopCompletedReason.LOOP_BREAK
                    if reach_break_condition
                    else LoopCompletedReason.LOOP_COMPLETED.value
                ),
            },
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=self.node_data.outputs,
                inputs=inputs,
                llm_usage=loop_usage,
            ),
        )

    def _yield_loop_failure_events(
        self,
        *,
        start_at: datetime,
        inputs: Mapping[str, Any],
        steps: int,
        loop_usage: LLMUsage,
        loop_duration_map: Mapping[str, float],
        single_loop_variable_map: Mapping[str, dict[str, object]],
        error: Exception,
    ) -> Generator[NodeEventBase, None, None]:
        metadata = {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: loop_usage.total_tokens,
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: loop_usage.total_price,
            WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
            WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: loop_duration_map,
            WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                single_loop_variable_map
            ),
        }
        yield LoopFailedEvent(
            start_at=start_at,
            inputs=inputs,
            steps=steps,
            metadata={**metadata, "completed_reason": "error"},
            error=str(error),
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=str(error),
                metadata=metadata,
                llm_usage=loop_usage,
            ),
        )

    def _run_single_loop(
        self,
        *,
        graph_engine: "GraphEngine",
        current_index: int,
        loop_state: dict[str, bool],
    ) -> Generator[NodeEventBase | GraphNodeEventBase, None, None]:
        loop_state["reach_break_node"] = False
        for event in graph_engine.run():
            match event:
                case GraphNodeEventBase(node_type=BuiltinNodeTypes.LOOP_START):
                    self._append_loop_info_to_event(
                        event=event,
                        loop_run_index=current_index,
                    )
                    continue
                case NodeRunSucceededEvent(node_type=BuiltinNodeTypes.LOOP_END):
                    self._append_loop_info_to_event(
                        event=event,
                        loop_run_index=current_index,
                    )
                    yield event
                    loop_state["reach_break_node"] = True
                case GraphNodeEventBase():
                    self._append_loop_info_to_event(
                        event=event,
                        loop_run_index=current_index,
                    )
                    yield event
                case GraphRunAbortedEvent(reason=reason):
                    raise RuntimeError(reason or _DEFAULT_CHILD_ABORT_REASON)
                case GraphRunFailedEvent(error=error):
                    raise RuntimeError(error)
                case _:
                    pass

        for loop_var in self.node_data.loop_variables or []:
            key, sel = loop_var.label, [self._node_id, loop_var.label]
            segment = self.graph_runtime_state.variable_pool.get(sel)
            self.node_data.outputs[key] = segment.value if segment else None
        self.node_data.outputs["loop_round"] = current_index + 1

    def run_single_loop(
        self,
        *,
        graph_engine: "GraphEngine",
        current_index: int,
        loop_state: dict[str, bool],
    ) -> Generator[NodeEventBase | GraphNodeEventBase, None, None]:
        """Run one loop iteration with explicit collaborators."""
        yield from self._run_single_loop(
            graph_engine=graph_engine,
            current_index=current_index,
            loop_state=loop_state,
        )

    def _append_loop_info_to_event(
        self,
        event: GraphNodeEventBase,
        loop_run_index: int,
    ) -> None:
        event.in_loop_id = self._node_id
        loop_metadata = {
            WorkflowNodeExecutionMetadataKey.LOOP_ID: self._node_id,
            WorkflowNodeExecutionMetadataKey.LOOP_INDEX: loop_run_index,
        }

        current_metadata = event.node_run_result.metadata
        if WorkflowNodeExecutionMetadataKey.LOOP_ID not in current_metadata:
            event.node_run_result.metadata = {**current_metadata, **loop_metadata}

    def _clear_loop_subgraph_variables(self, loop_node_ids: set[str]) -> None:
        """Remove variables produced by loop sub-graph nodes from previous iterations.

        Keeping stale variables causes a freshly created response coordinator in the
        next iteration to fall back to outdated values when no stream chunks exist.
        """
        variable_pool = self.graph_runtime_state.variable_pool
        for node_id in loop_node_ids:
            variable_pool.remove([node_id])

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

        # Extract loop node IDs statically from graph_config

        loop_node_ids = cls._extract_loop_node_ids_from_config(graph_config, node_id)

        # Get node configs from graph_config
        node_configs = {
            node["id"]: node for node in graph_config.get("nodes", []) if "id" in node
        }
        for sub_node_id, sub_node_config in node_configs.items():
            if sub_node_config.get("data", {}).get("loop_id") != node_id:
                continue

            # variable selector to variable mapping
            try:
                typed_sub_node_config = NodeConfigDictAdapter.validate_python(
                    sub_node_config,
                )
                node_type = typed_sub_node_config["data"].type
                node_mapping = Node.get_node_type_classes_mapping()
                if node_type not in node_mapping:
                    continue
                node_version = str(typed_sub_node_config["data"].version)
                node_cls = node_mapping[node_type][node_version]

                sub_node_variable_mapping = (
                    node_cls.extract_variable_selector_to_variable_mapping(
                        graph_config=graph_config,
                        config=typed_sub_node_config,
                    )
                )
            except NotImplementedError:
                sub_node_variable_mapping = {}

            # remove loop variables
            sub_node_variable_mapping = {
                sub_node_id + "." + key: value
                for key, value in sub_node_variable_mapping.items()
                if value[0] != node_id
            }

            variable_mapping.update(sub_node_variable_mapping)

        for loop_variable in node_data.loop_variables or []:
            if loop_variable.value_type == "variable":
                if loop_variable.value is None:
                    msg = "Loop variable value must be provided for variable type"
                    raise ValueError(msg)
                # add loop variable to variable mapping
                selector = loop_variable.value
                variable_mapping[f"{node_id}.{loop_variable.label}"] = selector

        # remove variable out from loop
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
        """Extract node IDs that belong to a specific loop from graph configuration.

        This method statically analyzes the graph configuration to find all nodes
        that are part of the specified loop, without creating actual node instances.

        :param graph_config: the complete graph configuration
        :param loop_node_id: the ID of the loop node

        Returns:
            The set of node IDs that belong to the target loop.

        """
        loop_node_ids = set()

        # Find all nodes that belong to this loop
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
        """Get the appropriate segment type for a constant value."""
        value = LoopNode._deserialize_constant_value(
            var_type=var_type,
            original_value=original_value,
        )
        try:
            return build_segment_with_type(var_type, value=value)
        except TypeMismatchError as type_exc:
            # Attempt to parse the value as a JSON-encoded string, if applicable.
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

    def _create_graph_engine(self, root_node_id: str) -> Any:
        # Create GraphInitParams for child graph execution.
        graph_init_params = GraphInitParams(
            workflow_id=self.workflow_id,
            graph_config=self.graph_config,
            run_context=self.run_context,
            call_depth=self.workflow_call_depth,
        )

        return self.graph_runtime_state.create_child_engine(
            workflow_id=self.workflow_id,
            graph_init_params=graph_init_params,
            root_node_id=root_node_id,
        )
