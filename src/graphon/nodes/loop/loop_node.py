import contextlib
import json
import logging
from collections.abc import Callable, Generator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, override

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
    LoopVariableData,
)
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
_JSON_ARRAY_LOOP_TYPES = frozenset((
    SegmentType.ARRAY_NUMBER,
    SegmentType.ARRAY_OBJECT,
    SegmentType.ARRAY_STRING,
))


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
        # Get inputs
        loop_count = self.node_data.loop_count
        break_conditions = self.node_data.break_conditions
        logical_operator = self.node_data.logical_operator

        inputs = {"loop_count": loop_count}

        if not self.node_data.start_node_id:
            msg = f"field start_node_id in loop {self._node_id} not found"
            raise ValueError(msg)

        root_node_id = self.node_data.start_node_id

        # Initialize loop variables in the original variable pool
        loop_variable_selectors = {}
        if self.node_data.loop_variables:
            value_processor: dict[
                Literal["constant", "variable"],
                Callable[[LoopVariableData], Segment | None],
            ] = {
                "constant": lambda var: self._get_segment_for_constant(
                    var.var_type,
                    var.value,
                ),
                "variable": lambda var: (
                    self.graph_runtime_state.variable_pool.get(var.value)
                    if isinstance(var.value, list)
                    else None
                ),
            }
            for loop_variable in self.node_data.loop_variables:
                if loop_variable.value_type not in value_processor:
                    msg = (
                        f"Invalid value type '{loop_variable.value_type}' "
                        f"for loop variable {loop_variable.label}"
                    )
                    raise ValueError(msg)

                processed_segment = value_processor[loop_variable.value_type](
                    loop_variable,
                )
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

        start_at = datetime.now(UTC).replace(tzinfo=None)
        condition_processor = ConditionProcessor()

        loop_duration_map: dict[str, float] = {}
        single_loop_variable_map: dict[
            str,
            dict[str, Any],
        ] = {}  # single loop variable output
        loop_usage = LLMUsage.empty_usage()
        loop_node_ids = self._extract_loop_node_ids_from_config(
            self.graph_config,
            self._node_id,
        )

        # Start Loop event
        yield LoopStartedEvent(
            start_at=start_at,
            inputs=inputs,
            metadata={"loop_length": loop_count},
        )

        try:
            reach_break_condition = False
            if break_conditions:
                with contextlib.suppress(ValueError):
                    _, _, reach_break_condition = (
                        condition_processor.process_conditions(
                            variable_pool=self.graph_runtime_state.variable_pool,
                            conditions=break_conditions,
                            operator=logical_operator,
                        )
                    )

            if reach_break_condition:
                loop_count = 0

            for i in range(loop_count):
                # Clear stale variables from previous loop iterations to avoid
                # streaming old values
                self._clear_loop_subgraph_variables(loop_node_ids)
                graph_engine = self._create_graph_engine(root_node_id=root_node_id)
                loop_state = {"reach_break_node": False}

                loop_start_time = datetime.now(UTC).replace(tzinfo=None)
                try:
                    yield from self._run_single_loop(
                        graph_engine=graph_engine,
                        current_index=i,
                        loop_state=loop_state,
                    )
                finally:
                    loop_usage = self._merge_usage(
                        loop_usage,
                        graph_engine.graph_runtime_state.llm_usage,
                    )
                reach_break_node = loop_state["reach_break_node"]
                # Track loop duration
                loop_duration_map[str(i)] = (
                    datetime.now(UTC).replace(tzinfo=None) - loop_start_time
                ).total_seconds()

                # Accumulate outputs from the sub-graph's response nodes
                for key, value in graph_engine.graph_runtime_state.outputs.items():
                    if key == "answer":
                        # Concatenate answer outputs with newline
                        existing_answer = self.graph_runtime_state.get_output(
                            "answer",
                            "",
                        )
                        if existing_answer:
                            self.graph_runtime_state.set_output(
                                "answer",
                                f"{existing_answer}{value}",
                            )
                        else:
                            self.graph_runtime_state.set_output("answer", value)
                    else:
                        # For other outputs, just update
                        self.graph_runtime_state.set_output(key, value)

                # Collect loop variable values after iteration
                single_loop_variable = {}
                for key, selector in loop_variable_selectors.items():
                    segment = self.graph_runtime_state.variable_pool.get(selector)
                    single_loop_variable[key] = segment.value if segment else None

                single_loop_variable_map[str(i)] = single_loop_variable

                if reach_break_node:
                    break

                if break_conditions:
                    _, _, reach_break_condition = (
                        condition_processor.process_conditions(
                            variable_pool=self.graph_runtime_state.variable_pool,
                            conditions=break_conditions,
                            operator=logical_operator,
                        )
                    )
                if reach_break_condition:
                    break

                yield LoopNextEvent(
                    index=i + 1,
                    pre_loop_output=self.node_data.outputs,
                )

            self._accumulate_usage(loop_usage)
            # Loop completed successfully
            yield LoopSucceededEvent(
                start_at=start_at,
                inputs=inputs,
                outputs=self.node_data.outputs,
                steps=loop_count,
                metadata={
                    WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                        loop_usage.total_tokens
                    ),
                    WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                        loop_usage.total_price
                    ),
                    WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
                    WorkflowNodeExecutionMetadataKey.COMPLETED_REASON: (
                        LoopCompletedReason.LOOP_BREAK
                        if reach_break_condition
                        else LoopCompletedReason.LOOP_COMPLETED.value
                    ),
                    WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                        loop_duration_map
                    ),
                    WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                        single_loop_variable_map
                    ),
                },
            )

            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    metadata={
                        WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                            loop_usage.total_tokens
                        ),
                        WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                            loop_usage.total_price
                        ),
                        WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
                        WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                            loop_duration_map
                        ),
                        WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                            single_loop_variable_map
                        ),
                    },
                    outputs=self.node_data.outputs,
                    inputs=inputs,
                    llm_usage=loop_usage,
                ),
            )

        except Exception as e:
            logger.exception("Loop node %s failed", self._node_id)
            self._accumulate_usage(loop_usage)
            yield LoopFailedEvent(
                start_at=start_at,
                inputs=inputs,
                steps=loop_count,
                metadata={
                    WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                        loop_usage.total_tokens
                    ),
                    WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                        loop_usage.total_price
                    ),
                    WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
                    "completed_reason": "error",
                    WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                        loop_duration_map
                    ),
                    WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                        single_loop_variable_map
                    ),
                },
                error=str(e),
            )

            yield StreamCompletedEvent(
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.FAILED,
                    error=str(e),
                    metadata={
                        WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                            loop_usage.total_tokens
                        ),
                        WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                            loop_usage.total_price
                        ),
                        WorkflowNodeExecutionMetadataKey.CURRENCY: loop_usage.currency,
                        WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                            loop_duration_map
                        ),
                        WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                            single_loop_variable_map
                        ),
                    },
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
                assert loop_variable.value is not None, (
                    "Loop variable value must be provided for variable type"
                )
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
        # TODO: Refactor for maintainability:
        # 1. Ensure type handling logic stays synchronized with
        #    _VALID_VAR_TYPE (entities.py)
        # 2. Consider moving this method to LoopVariableData class
        #    for better encapsulation
        if not var_type.is_array_type() or var_type == SegmentType.ARRAY_BOOLEAN:
            value = original_value
        elif var_type in _JSON_ARRAY_LOOP_TYPES:
            if original_value and isinstance(original_value, str):
                value = json.loads(original_value)
            else:
                logger.warning(
                    "unexpected value for LoopNode, value_type=%s, value=%s",
                    original_value,
                    var_type,
                )
                value = []
        else:
            msg = "this statement should be unreachable."
            raise AssertionError(msg)
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
