"""Engine-owned execution for container nodes."""

from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import final

from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.iteration import (
    NodeRunIterationFailedEvent,
    NodeRunIterationNextEvent,
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopFailedEvent,
    NodeRunLoopNextEvent,
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.exc import (
    InvalidIteratorValueError,
    IteratorVariableNotFoundError,
    StartNodeIdNotFoundError,
)
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopCompletedReason
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)
from graphon.utils.condition.processor import ConditionProcessor
from graphon.variables.segments import ArrayAnySegment, ArraySegment, NoneSegment

from .entities.tasks import TaskEvent
from .frames import ExecutionFrame, FrameRegistry


@dataclass(slots=True)
class _LoopRunContext:
    # Frame that owns the loop container node.
    parent_frame_id: str
    # Loop container node id in the parent frame.
    loop_node_id: str
    # Node execution id shared by loop lifecycle events.
    loop_execution_id: str
    # Inputs shown on loop lifecycle events and node results.
    inputs: dict[str, object]
    # Timestamp used for loop lifecycle events and duration calculation.
    started_at: datetime
    # Maximum number of loop rounds configured on the node.
    loop_count: int
    # First node id to schedule inside each loop frame.
    root_node_id: str
    # Loop variable names mapped to their variable-pool selectors.
    loop_variable_selectors: dict[str, list[str]]
    # Node ids inside the loop body whose variables reset before each round.
    loop_node_ids: set[str]
    # Per-round elapsed seconds keyed by loop index.
    duration_map: dict[str, float]
    # Per-round loop variable snapshots keyed by loop index.
    variable_map: dict[str, dict[str, object]]
    # Aggregated LLM usage from completed loop frames.
    usage: LLMUsage
    # Number of loop frames that have completed.
    completed_count: int
    # Whether a break condition or loop-end node stopped the loop.
    reached_break: bool


@dataclass(frozen=True, slots=True)
class _LoopFrameContext:
    loop_execution_id: str
    loop_index: int
    started_at: datetime


@dataclass(slots=True)
class _IterationRunContext:
    # Frame that owns the iteration container node.
    parent_frame_id: str
    # Iteration container node id in the parent frame.
    iteration_node_id: str
    # Node execution id shared by iteration lifecycle events.
    iteration_execution_id: str
    # Immutable iterator values captured when the iteration starts.
    items: tuple[object, ...]
    # Inputs shown on iteration lifecycle events and node results.
    inputs: dict[str, object]
    # Timestamp used for iteration lifecycle events and duration calculation.
    started_at: datetime
    # Per-item output values keyed by iteration index.
    outputs: dict[int, object]
    # Per-item elapsed seconds keyed by iteration index.
    duration_map: dict[str, float]
    # Aggregated LLM usage from completed iteration frames.
    usage: LLMUsage
    # Number of iteration frames scheduled so far.
    scheduled_count: int
    # Number of iteration frames that have completed.
    completed_count: int


@dataclass(frozen=True, slots=True)
class _IterationFrameContext:
    iteration_execution_id: str
    iteration_index: int
    started_at: datetime


@final
class ContainerExecution:
    def __init__(
        self,
        *,
        frame_registry: FrameRegistry,
        graph_execution: GraphExecutionProtocol,
    ) -> None:
        self._frame_registry = frame_registry
        self._graph_execution = graph_execution
        self._loop_runs: dict[str, _LoopRunContext] = {}
        self._loop_frames: dict[str, _LoopFrameContext] = {}
        self._loop_frame_failures: dict[str, str] = {}
        self._loop_break_frames: set[str] = set()
        self._iteration_runs: dict[str, _IterationRunContext] = {}
        self._iteration_frames: dict[str, _IterationFrameContext] = {}
        self._iteration_frame_failures: dict[str, str] = {}

    def prepare_frame_event(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> None:
        loop_context = self._loop_frames.get(frame.frame_id)
        if loop_context is not None:
            run_context = self._loop_runs[loop_context.loop_execution_id]
            event.in_loop_id = run_context.loop_node_id
            loop_metadata = {
                WorkflowNodeExecutionMetadataKey.LOOP_ID: run_context.loop_node_id,
                WorkflowNodeExecutionMetadataKey.LOOP_INDEX: loop_context.loop_index,
            }
            current_metadata = event.node_run_result.metadata
            if WorkflowNodeExecutionMetadataKey.LOOP_ID not in current_metadata:
                event.node_run_result.metadata = {**current_metadata, **loop_metadata}
            if (
                isinstance(event, NodeRunSucceededEvent)
                and event.node_type == BuiltinNodeTypes.LOOP_END
            ):
                self._loop_break_frames.add(frame.frame_id)
            return

        iteration_context = self._iteration_frames.get(frame.frame_id)
        if iteration_context is None:
            return
        run_context = self._iteration_runs[iteration_context.iteration_execution_id]
        event.in_iteration_id = run_context.iteration_node_id
        iter_metadata = {
            WorkflowNodeExecutionMetadataKey.ITERATION_ID: (
                run_context.iteration_node_id
            ),
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX: (
                iteration_context.iteration_index
            ),
        }
        current_metadata = event.node_run_result.metadata
        if WorkflowNodeExecutionMetadataKey.ITERATION_ID not in current_metadata:
            event.node_run_result.metadata = {**current_metadata, **iter_metadata}

    def should_collect(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> bool:
        if frame.frame_id not in self._loop_frames:
            if frame.frame_id not in self._iteration_frames:
                return True
            return event.node_type != BuiltinNodeTypes.ITERATION_START
        return event.node_type != BuiltinNodeTypes.LOOP_START

    def enter_from_started_event(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunStartedEvent,
    ) -> list[GraphNodeEventBase]:
        node = frame.graph.nodes[event.node_id]
        if node.execution_type != NodeExecutionType.CONTAINER:
            return []
        if isinstance(node, LoopNode):
            return self._enter_loop(frame=frame, event=event, node=node)
        if isinstance(node, IterationNode):
            return self._enter_iteration(frame=frame, event=event, node=node)
        return []

    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> bool:
        if frame.frame_id in self._loop_frames:
            self._loop_frame_failures[frame.frame_id] = event.error
            return True
        if frame.frame_id in self._iteration_frames:
            self._iteration_frame_failures[frame.frame_id] = event.error
            return True
        return False

    def complete_frame(self, frame: ExecutionFrame) -> list[TaskEvent]:
        iteration_events = self._complete_iteration_frame(frame)
        if iteration_events:
            return iteration_events

        return self._complete_loop_frame(frame)

    def _enter_loop(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunStartedEvent,
        node: LoopNode,
    ) -> list[GraphNodeEventBase]:
        loop_count = node.node_data.loop_count
        inputs: dict[str, object] = {"loop_count": loop_count}
        root_node_id, loop_variable_selectors, loop_node_ids = node.initialize_loop_run(
            inputs=inputs
        )
        started_at = datetime.now(UTC).replace(tzinfo=None)
        started_event = NodeRunLoopStartedEvent(
            id=event.id,
            node_id=event.node_id,
            node_type=event.node_type,
            node_title=node.node_data.title,
            start_at=started_at,
            inputs=inputs,
            metadata={"loop_length": loop_count},
        )
        run_context = _LoopRunContext(
            parent_frame_id=frame.frame_id,
            loop_node_id=event.node_id,
            loop_execution_id=event.id,
            inputs=inputs,
            started_at=started_at,
            loop_count=loop_count,
            root_node_id=root_node_id,
            loop_variable_selectors=loop_variable_selectors,
            loop_node_ids=loop_node_ids,
            duration_map={},
            variable_map={},
            usage=LLMUsage.empty_usage(),
            completed_count=0,
            reached_break=False,
        )
        self._loop_runs[event.id] = run_context
        if self._loop_break_conditions_reached(
            frame=frame,
            node=node,
            suppress_errors=True,
        ):
            run_context.reached_break = True
            return [
                started_event,
                *[
                    task_event.event
                    for task_event in self._complete_loop(
                        parent_frame=frame,
                        node=node,
                        run_context=run_context,
                        steps=0,
                    )
                ],
            ]

        self._start_loop_frame(
            parent_frame=frame,
            run_context=run_context,
            index=0,
        )

        return [started_event]

    def _start_loop_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_context: _LoopRunContext,
        index: int,
    ) -> None:
        self._clear_loop_subgraph_variables(
            frame=parent_frame,
            loop_node_ids=run_context.loop_node_ids,
        )
        child_runtime_state = GraphRuntimeState(
            variable_pool=parent_frame.graph_runtime_state.variable_pool,
            start_at=time.time(),
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            graph_execution=self._graph_execution,
        )
        child_frame_id = f"{run_context.loop_execution_id}:loop:{index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=run_context.root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._loop_frames[child_frame_id] = _LoopFrameContext(
            loop_execution_id=run_context.loop_execution_id,
            loop_index=index,
            started_at=datetime.now(UTC).replace(tzinfo=None),
        )
        child_frame.state_manager.enqueue_node(
            frame_id=child_frame.frame_id,
            node_id=run_context.root_node_id,
        )
        child_frame.state_manager.start_execution(
            frame_id=child_frame.frame_id,
            node_id=run_context.root_node_id,
        )

    def _complete_loop_frame(self, frame: ExecutionFrame) -> list[TaskEvent]:
        frame_context = self._loop_frames.get(frame.frame_id)
        if frame_context is None:
            return []
        if not frame.state_manager.is_execution_complete():
            return []

        self._loop_frames.pop(frame.frame_id)
        run_context = self._loop_runs[frame_context.loop_execution_id]
        parent_frame = self._frame_registry.get(run_context.parent_frame_id)
        node = parent_frame.graph.nodes[run_context.loop_node_id]
        if not isinstance(node, LoopNode):
            return []

        self._complete_loop_step(
            frame=frame,
            frame_context=frame_context,
            parent_frame=parent_frame,
            node=node,
            run_context=run_context,
        )
        if frame.frame_id in self._loop_frame_failures:
            error = self._loop_frame_failures.pop(frame.frame_id)
            return self._fail_loop(
                parent_frame=parent_frame,
                node=node,
                run_context=run_context,
                error=error,
            )

        if frame.frame_id in self._loop_break_frames:
            self._loop_break_frames.remove(frame.frame_id)
            run_context.reached_break = True
        elif self._loop_break_conditions_reached(
            frame=parent_frame,
            node=node,
            suppress_errors=False,
        ):
            run_context.reached_break = True

        if (
            run_context.reached_break
            or run_context.completed_count >= run_context.loop_count
        ):
            return self._complete_loop(
                parent_frame=parent_frame,
                node=node,
                run_context=run_context,
                steps=run_context.loop_count,
            )

        next_index = run_context.completed_count
        self._start_loop_frame(
            parent_frame=parent_frame,
            run_context=run_context,
            index=next_index,
        )
        return [
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunLoopNextEvent(
                    id=run_context.loop_execution_id,
                    node_id=run_context.loop_node_id,
                    node_type=node.node_type,
                    node_title=node.node_data.title,
                    index=next_index,
                    pre_loop_output=node.node_data.outputs,
                ),
            ),
        ]

    def _complete_loop_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_context: _LoopFrameContext,
        parent_frame: ExecutionFrame,
        node: LoopNode,
        run_context: _LoopRunContext,
    ) -> None:
        run_context.completed_count += 1
        run_context.usage = run_context.usage.plus(frame.graph_runtime_state.llm_usage)
        run_context.duration_map[str(frame_context.loop_index)] = (
            datetime.now(UTC).replace(tzinfo=None) - frame_context.started_at
        ).total_seconds()
        run_context.variable_map[str(frame_context.loop_index)] = (
            self._collect_loop_variable_values(
                frame=parent_frame,
                loop_variable_selectors=run_context.loop_variable_selectors,
            )
        )
        parent_frame.graph_runtime_state.merge_response_outputs(
            frame.graph_runtime_state.outputs,
        )
        for loop_variable in node.node_data.loop_variables or []:
            selector = [run_context.loop_node_id, loop_variable.label]
            segment = parent_frame.graph_runtime_state.variable_pool.get(selector)
            node.node_data.outputs[loop_variable.label] = (
                segment.value if segment else None
            )
        node.node_data.outputs["loop_round"] = frame_context.loop_index + 1

    def _complete_loop(
        self,
        *,
        parent_frame: ExecutionFrame,
        node: LoopNode,
        run_context: _LoopRunContext,
        steps: int,
    ) -> list[TaskEvent]:
        self._loop_runs.pop(run_context.loop_execution_id)
        metadata = self._loop_metadata(run_context)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = (
            LoopCompletedReason.LOOP_BREAK
            if run_context.reached_break
            else LoopCompletedReason.LOOP_COMPLETED.value
        )
        return [
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunLoopSucceededEvent(
                    id=run_context.loop_execution_id,
                    node_id=run_context.loop_node_id,
                    node_type=node.node_type,
                    node_title=node.node_data.title,
                    start_at=run_context.started_at,
                    inputs=run_context.inputs,
                    outputs=node.node_data.outputs,
                    metadata=loop_metadata,
                    steps=steps,
                ),
            ),
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunSucceededEvent(
                    id=run_context.loop_execution_id,
                    node_id=run_context.loop_node_id,
                    node_type=node.node_type,
                    start_at=run_context.started_at,
                    finished_at=datetime.now(UTC).replace(tzinfo=None),
                    node_run_result=NodeRunResult(
                        status=WorkflowNodeExecutionStatus.SUCCEEDED,
                        metadata=metadata,
                        outputs=node.node_data.outputs,
                        inputs=run_context.inputs,
                        llm_usage=run_context.usage,
                    ),
                ),
            ),
        ]

    def _fail_loop(
        self,
        *,
        parent_frame: ExecutionFrame,
        node: LoopNode,
        run_context: _LoopRunContext,
        error: str,
    ) -> list[TaskEvent]:
        self._loop_runs.pop(run_context.loop_execution_id)
        metadata = self._loop_metadata(run_context)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = "error"
        return [
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunLoopFailedEvent(
                    id=run_context.loop_execution_id,
                    node_id=run_context.loop_node_id,
                    node_type=node.node_type,
                    node_title=node.node_data.title,
                    start_at=run_context.started_at,
                    inputs=run_context.inputs,
                    metadata=loop_metadata,
                    steps=run_context.loop_count,
                    error=error,
                ),
            ),
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunFailedEvent(
                    id=run_context.loop_execution_id,
                    node_id=run_context.loop_node_id,
                    node_type=node.node_type,
                    start_at=run_context.started_at,
                    finished_at=datetime.now(UTC).replace(tzinfo=None),
                    error=error,
                    node_run_result=NodeRunResult(
                        status=WorkflowNodeExecutionStatus.FAILED,
                        error=error,
                        metadata=metadata,
                        llm_usage=run_context.usage,
                    ),
                ),
            ),
        ]

    def _loop_metadata(
        self,
        run_context: _LoopRunContext,
    ) -> dict[WorkflowNodeExecutionMetadataKey, object]:
        return {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                run_context.usage.total_tokens
            ),
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                run_context.usage.total_price
            ),
            WorkflowNodeExecutionMetadataKey.CURRENCY: run_context.usage.currency,
            WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                run_context.duration_map
            ),
            WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                run_context.variable_map
            ),
        }

    def _event_metadata(
        self,
        metadata: dict[WorkflowNodeExecutionMetadataKey, object],
    ) -> dict[str, object]:
        return {key.value: value for key, value in metadata.items()}

    def _loop_break_conditions_reached(
        self,
        *,
        frame: ExecutionFrame,
        node: LoopNode,
        suppress_errors: bool,
    ) -> bool:
        if not node.node_data.break_conditions:
            return False
        condition_processor = ConditionProcessor()
        if suppress_errors:
            with contextlib.suppress(ValueError):
                _, _, result = condition_processor.process_conditions(
                    variable_pool=frame.graph_runtime_state.variable_pool,
                    conditions=node.node_data.break_conditions,
                    operator=node.node_data.logical_operator,
                )
                return result
            return False

        _, _, result = condition_processor.process_conditions(
            variable_pool=frame.graph_runtime_state.variable_pool,
            conditions=node.node_data.break_conditions,
            operator=node.node_data.logical_operator,
        )
        return result

    def _collect_loop_variable_values(
        self,
        *,
        frame: ExecutionFrame,
        loop_variable_selectors: dict[str, list[str]],
    ) -> dict[str, object]:
        values: dict[str, object] = {}
        for key, selector in loop_variable_selectors.items():
            segment = frame.graph_runtime_state.variable_pool.get(selector)
            values[key] = segment.value if segment else None
        return values

    def _clear_loop_subgraph_variables(
        self,
        *,
        frame: ExecutionFrame,
        loop_node_ids: set[str],
    ) -> None:
        for node_id in loop_node_ids:
            frame.graph_runtime_state.variable_pool.remove([node_id])

    def _enter_iteration(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunStartedEvent,
        node: IterationNode,
    ) -> list[GraphNodeEventBase]:
        variable = frame.graph_runtime_state.variable_pool.get(
            node.node_data.iterator_selector,
        )
        if variable is None:
            msg = f"iterator variable {node.node_data.iterator_selector} not found"
            raise IteratorVariableNotFoundError(msg)
        if isinstance(variable, NoneSegment) or (
            isinstance(variable, ArraySegment) and len(variable.value) == 0
        ):
            return self._complete_empty_iteration(frame=frame, event=event, node=node)
        if not isinstance(variable, ArraySegment):
            msg = f"invalid iterator value: {variable}, please provide a list."
            raise InvalidIteratorValueError(msg)

        iterator_value = variable.to_object()
        if not isinstance(iterator_value, list):
            msg = f"Invalid iterator value: {iterator_value}, please provide a list."
            raise InvalidIteratorValueError(msg)

        root_node_id = node.node_data.start_node_id
        if not root_node_id:
            msg = f"field start_node_id in iteration {event.node_id} not found"
            raise StartNodeIdNotFoundError(msg)

        started_at = datetime.now(UTC).replace(tzinfo=None)
        inputs: dict[str, object] = {"iterator_selector": iterator_value}
        run_context = _IterationRunContext(
            parent_frame_id=frame.frame_id,
            iteration_node_id=event.node_id,
            iteration_execution_id=event.id,
            items=tuple(iterator_value),
            inputs=inputs,
            started_at=started_at,
            outputs={},
            duration_map={},
            usage=LLMUsage.empty_usage(),
            scheduled_count=0,
            completed_count=0,
        )
        self._iteration_runs[event.id] = run_context

        events: list[GraphNodeEventBase] = [
            NodeRunIterationStartedEvent(
                id=event.id,
                node_id=event.node_id,
                node_type=event.node_type,
                node_title=node.node_data.title,
                start_at=started_at,
                inputs=inputs,
                metadata={"iteration_length": len(iterator_value)},
            ),
        ]
        initial_frame_count = 1
        if node.node_data.is_parallel:
            initial_frame_count = min(
                max(node.node_data.parallel_nums, 1),
                len(run_context.items),
            )
        events.extend(
            self._schedule_iteration_frame(
                parent_frame=frame,
                node=node,
                run_context=run_context,
                root_node_id=root_node_id,
            )
            for _ in range(initial_frame_count)
        )
        return events

    def _complete_empty_iteration(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunStartedEvent,
        node: IterationNode,
    ) -> list[GraphNodeEventBase]:
        variable = frame.graph_runtime_state.variable_pool.get(
            node.node_data.iterator_selector,
        )
        if isinstance(variable, ArraySegment):
            output = variable.model_copy(update={"value": []})
        else:
            output = ArrayAnySegment(value=[])
        outputs = {"output": output}
        return [
            NodeRunSucceededEvent(
                id=event.id,
                node_id=event.node_id,
                node_type=event.node_type,
                start_at=event.start_at,
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs=outputs,
                ),
            ),
        ]

    def _complete_iteration_frame(self, frame: ExecutionFrame) -> list[TaskEvent]:
        frame_context = self._iteration_frames.get(frame.frame_id)
        if frame_context is None:
            return []
        if not frame.state_manager.is_execution_complete():
            return []

        self._iteration_frames.pop(frame.frame_id)
        run_context = self._iteration_runs[frame_context.iteration_execution_id]
        parent_frame = self._frame_registry.get(run_context.parent_frame_id)
        node = parent_frame.graph.nodes[run_context.iteration_node_id]
        if not isinstance(node, IterationNode):
            return []

        if frame.frame_id in self._iteration_frame_failures:
            error = self._iteration_frame_failures.pop(frame.frame_id)
            return self._complete_failed_iteration_frame(
                frame=frame,
                frame_context=frame_context,
                parent_frame=parent_frame,
                node=node,
                run_context=run_context,
                error=error,
            )

        result = frame.graph_runtime_state.variable_pool.get(
            node.node_data.output_selector,
        )
        run_context.outputs[frame_context.iteration_index] = (
            None if result is None else result.to_object()
        )
        self._complete_iteration_step(
            frame=frame,
            frame_context=frame_context,
            run_context=run_context,
        )

        return self._continue_or_complete_iteration(
            parent_frame=parent_frame,
            node=node,
            run_context=run_context,
            last_frame=frame,
        )

    def _complete_failed_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_context: _IterationFrameContext,
        parent_frame: ExecutionFrame,
        node: IterationNode,
        run_context: _IterationRunContext,
        error: str,
    ) -> list[TaskEvent]:
        self._complete_iteration_step(
            frame=frame,
            frame_context=frame_context,
            run_context=run_context,
        )
        match node.node_data.error_handle_mode:
            case ErrorHandleMode.TERMINATED:
                return self._fail_iteration(
                    parent_frame=parent_frame,
                    node=node,
                    run_context=run_context,
                    error=error,
                )
            case ErrorHandleMode.CONTINUE_ON_ERROR:
                run_context.outputs[frame_context.iteration_index] = None
            case ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT:
                pass

        return self._continue_or_complete_iteration(
            parent_frame=parent_frame,
            node=node,
            run_context=run_context,
            last_frame=frame,
        )

    def _complete_iteration_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_context: _IterationFrameContext,
        run_context: _IterationRunContext,
    ) -> None:
        run_context.completed_count += 1
        run_context.duration_map[str(frame_context.iteration_index)] = (
            datetime.now(UTC).replace(tzinfo=None) - frame_context.started_at
        ).total_seconds()
        run_context.usage = run_context.usage.plus(frame.graph_runtime_state.llm_usage)

    def _continue_or_complete_iteration(
        self,
        *,
        parent_frame: ExecutionFrame,
        node: IterationNode,
        run_context: _IterationRunContext,
        last_frame: ExecutionFrame,
    ) -> list[TaskEvent]:
        if run_context.scheduled_count < len(run_context.items):
            root_node_id = node.node_data.start_node_id
            if not root_node_id:
                msg = (
                    "field start_node_id in iteration "
                    f"{run_context.iteration_node_id} not found"
                )
                raise StartNodeIdNotFoundError(msg)
            next_event = self._schedule_iteration_frame(
                parent_frame=parent_frame,
                run_context=run_context,
                node=node,
                root_node_id=root_node_id,
            )
            return [TaskEvent(frame_id=parent_frame.frame_id, event=next_event)]

        if run_context.completed_count < len(run_context.items):
            return []

        self._iteration_runs.pop(run_context.iteration_execution_id)
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_context),
                flatten_output=node.node_data.flatten_output,
            ),
        }
        metadata: dict[WorkflowNodeExecutionMetadataKey, object] = {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                run_context.usage.total_tokens
            ),
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                run_context.usage.total_price
            ),
            WorkflowNodeExecutionMetadataKey.CURRENCY: run_context.usage.currency,
            WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: (
                run_context.duration_map
            ),
        }
        event_metadata = self._event_metadata(metadata)
        parent_frame.graph_runtime_state.merge_response_outputs(
            last_frame.graph_runtime_state.outputs,
        )
        return [
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunIterationSucceededEvent(
                    id=run_context.iteration_execution_id,
                    node_id=run_context.iteration_node_id,
                    node_type=node.node_type,
                    node_title=node.node_data.title,
                    start_at=run_context.started_at,
                    inputs=run_context.inputs,
                    outputs=outputs,
                    metadata=event_metadata,
                    steps=len(run_context.items),
                ),
            ),
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunSucceededEvent(
                    id=run_context.iteration_execution_id,
                    node_id=run_context.iteration_node_id,
                    node_type=node.node_type,
                    start_at=run_context.started_at,
                    finished_at=datetime.now(UTC).replace(tzinfo=None),
                    node_run_result=NodeRunResult(
                        status=WorkflowNodeExecutionStatus.SUCCEEDED,
                        metadata=metadata,
                        outputs=outputs,
                        inputs=run_context.inputs,
                        llm_usage=run_context.usage,
                    ),
                ),
            ),
        ]

    def _fail_iteration(
        self,
        *,
        parent_frame: ExecutionFrame,
        node: IterationNode,
        run_context: _IterationRunContext,
        error: str,
    ) -> list[TaskEvent]:
        self._iteration_runs.pop(run_context.iteration_execution_id)
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_context),
                flatten_output=node.node_data.flatten_output,
            ),
        }
        metadata: dict[WorkflowNodeExecutionMetadataKey, object] = {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                run_context.usage.total_tokens
            ),
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: (
                run_context.usage.total_price
            ),
            WorkflowNodeExecutionMetadataKey.CURRENCY: run_context.usage.currency,
            WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: (
                run_context.duration_map
            ),
        }
        event_metadata = self._event_metadata(metadata)
        return [
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunIterationFailedEvent(
                    id=run_context.iteration_execution_id,
                    node_id=run_context.iteration_node_id,
                    node_type=node.node_type,
                    node_title=node.node_data.title,
                    start_at=run_context.started_at,
                    inputs=run_context.inputs,
                    outputs=outputs,
                    metadata=event_metadata,
                    steps=len(run_context.items),
                    error=error,
                ),
            ),
            TaskEvent(
                frame_id=parent_frame.frame_id,
                event=NodeRunFailedEvent(
                    id=run_context.iteration_execution_id,
                    node_id=run_context.iteration_node_id,
                    node_type=node.node_type,
                    start_at=run_context.started_at,
                    finished_at=datetime.now(UTC).replace(tzinfo=None),
                    error=error,
                    node_run_result=NodeRunResult(
                        status=WorkflowNodeExecutionStatus.FAILED,
                        error=error,
                        metadata=metadata,
                        llm_usage=run_context.usage,
                    ),
                ),
            ),
        ]

    def _schedule_iteration_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        node: IterationNode,
        run_context: _IterationRunContext,
        root_node_id: str,
    ) -> NodeRunIterationNextEvent:
        index = run_context.scheduled_count
        self._start_iteration_frame(
            parent_frame=parent_frame,
            run_context=run_context,
            index=index,
            root_node_id=root_node_id,
        )
        run_context.scheduled_count += 1
        return NodeRunIterationNextEvent(
            id=run_context.iteration_execution_id,
            node_id=run_context.iteration_node_id,
            node_type=node.node_type,
            node_title=node.node_data.title,
            index=index,
        )

    def _start_iteration_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_context: _IterationRunContext,
        index: int,
        root_node_id: str,
    ) -> None:
        variable_pool = parent_frame.graph_runtime_state.variable_pool.model_copy(
            deep=True,
        )
        variable_pool.add([run_context.iteration_node_id, "index"], index)
        variable_pool.add(
            [run_context.iteration_node_id, "item"],
            run_context.items[index],
        )
        child_runtime_state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=time.time(),
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            graph_execution=self._graph_execution,
        )
        child_frame_id = f"{run_context.iteration_execution_id}:iteration:{index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._iteration_frames[child_frame_id] = _IterationFrameContext(
            iteration_execution_id=run_context.iteration_execution_id,
            iteration_index=index,
            started_at=datetime.now(UTC).replace(tzinfo=None),
        )
        child_frame.state_manager.enqueue_node(
            frame_id=child_frame.frame_id,
            node_id=root_node_id,
        )
        child_frame.state_manager.start_execution(
            frame_id=child_frame.frame_id,
            node_id=root_node_id,
        )

    def _flatten_outputs_if_needed(
        self,
        outputs: list[object],
        *,
        flatten_output: bool,
    ) -> list[object]:
        if not flatten_output:
            return outputs
        non_empty_outputs = [output for output in outputs if output is not None]
        if not non_empty_outputs:
            return outputs
        if not all(isinstance(output, list) for output in non_empty_outputs):
            return outputs

        flattened: list[object] = []
        for output in outputs:
            if isinstance(output, list):
                flattened.extend(output)
            elif output is not None:
                flattened.append(output)
        return flattened

    def _ordered_iteration_outputs(
        self,
        run_context: _IterationRunContext,
    ) -> list[object]:
        return [run_context.outputs[index] for index in sorted(run_context.outputs)]
