"""Engine-owned execution for container nodes."""

from __future__ import annotations

import contextlib
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import final

from graphon.enums import (
    BuiltinNodeTypes,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
)
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    IterationExecutionFailed,
    IterationExecutionSucceeded,
    IterationFrameRequest,
    IterationFramesRequested,
    LoopExecutionFailed,
    LoopExecutionSucceeded,
    LoopFrameCompleted,
    LoopFrameRequest,
)
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.entities import LoopCompletedReason
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)
from graphon.utils.condition.processor import ConditionProcessor

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ResumeTask


@dataclass(slots=True)
class _LoopRunContext:
    # Suspended parent node invocation waiting for loop progress or completion.
    invocation_id: str
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
    # Suspended parent node invocation waiting for iteration progress/completion.
    invocation_id: str
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
    # Whether a resume task has already been queued for this invocation.
    resume_pending: bool
    # Maximum active child frames requested by the node.
    parallel_nums: int
    # Variable selector used to read each child frame's output.
    output_selector: Sequence[str]
    # Error behavior requested by the iteration node.
    error_handle_mode: ErrorHandleMode
    # Whether output list values should be flattened in the final result.
    flatten_output: bool


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

    def start_container_await(
        self,
        *,
        frame_id: str,
        node_id: str,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None:
        parent_frame = self._frame_registry.get(frame_id)
        match request:
            case LoopFrameRequest():
                self._start_loop_request(
                    parent_frame=parent_frame,
                    node_id=node_id,
                    invocation_id=invocation_id,
                    request=request,
                )
            case IterationFrameRequest():
                self._start_iteration_request(
                    parent_frame=parent_frame,
                    node_id=node_id,
                    invocation_id=invocation_id,
                    request=request,
                )

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

    def complete_frame(self, frame: ExecutionFrame) -> None:
        if self._complete_iteration_frame(frame):
            return
        self._complete_loop_frame(frame)

    def _start_loop_request(
        self,
        *,
        parent_frame: ExecutionFrame,
        node_id: str,
        invocation_id: str,
        request: LoopFrameRequest,
    ) -> None:
        node = parent_frame.graph.nodes[node_id]
        if not isinstance(node, LoopNode):
            msg = f"node {node_id} cannot handle loop await requests"
            raise TypeError(msg)
        loop_execution_id = node.execution_id
        run_context = self._loop_runs.get(loop_execution_id)
        if run_context is None:
            run_context = _LoopRunContext(
                invocation_id=invocation_id,
                parent_frame_id=parent_frame.frame_id,
                loop_node_id=node_id,
                loop_execution_id=loop_execution_id,
                inputs=dict(request.inputs),
                started_at=request.started_at,
                loop_count=request.loop_count,
                root_node_id=request.root_node_id,
                loop_variable_selectors={
                    key: list(value)
                    for key, value in request.loop_variable_selectors.items()
                },
                loop_node_ids=set(request.loop_node_ids),
                duration_map={},
                variable_map={},
                usage=LLMUsage.empty_usage(),
                completed_count=0,
                reached_break=False,
            )
            self._loop_runs[loop_execution_id] = run_context
            if self._loop_break_conditions_reached(
                frame=parent_frame,
                node=node,
                suppress_errors=True,
            ):
                run_context.reached_break = True
                self._enqueue_container_result(
                    parent_frame=parent_frame,
                    invocation_id=run_context.invocation_id,
                    result=self._complete_loop(
                        node=node,
                        run_context=run_context,
                        steps=0,
                    ),
                )
                return
        else:
            run_context.invocation_id = invocation_id

        self._start_loop_frame(
            parent_frame=parent_frame,
            run_context=run_context,
            index=request.index,
        )

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

    def _complete_loop_frame(self, frame: ExecutionFrame) -> bool:
        frame_context = self._loop_frames.get(frame.frame_id)
        if frame_context is None:
            return False
        if not frame.state_manager.is_execution_complete():
            return True

        self._loop_frames.pop(frame.frame_id)
        run_context = self._loop_runs[frame_context.loop_execution_id]
        parent_frame = self._frame_registry.get(run_context.parent_frame_id)
        node = parent_frame.graph.nodes[run_context.loop_node_id]
        if not isinstance(node, LoopNode):
            return True

        self._complete_loop_step(
            frame=frame,
            frame_context=frame_context,
            parent_frame=parent_frame,
            node=node,
            run_context=run_context,
        )
        if frame.frame_id in self._loop_frame_failures:
            error = self._loop_frame_failures.pop(frame.frame_id)
            self._enqueue_container_result(
                parent_frame=parent_frame,
                invocation_id=run_context.invocation_id,
                result=self._fail_loop(run_context=run_context, error=error),
            )
            return True

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
            self._enqueue_container_result(
                parent_frame=parent_frame,
                invocation_id=run_context.invocation_id,
                result=self._complete_loop(
                    node=node,
                    run_context=run_context,
                    steps=run_context.loop_count,
                ),
            )
            return True

        next_index = run_context.completed_count
        parent_frame.graph_runtime_state.ready_queue.put(
            ResumeTask(
                invocation_id=run_context.invocation_id,
                result=LoopFrameCompleted(next_index=next_index),
            ),
        )
        return True

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
        node: LoopNode,
        run_context: _LoopRunContext,
        steps: int,
    ) -> LoopExecutionSucceeded:
        self._loop_runs.pop(run_context.loop_execution_id)
        metadata = self._loop_metadata(run_context)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = (
            LoopCompletedReason.LOOP_BREAK
            if run_context.reached_break
            else LoopCompletedReason.LOOP_COMPLETED.value
        )
        return LoopExecutionSucceeded(
            started_at=run_context.started_at,
            inputs=run_context.inputs,
            outputs=node.node_data.outputs,
            metadata=loop_metadata,
            steps=steps,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=node.node_data.outputs,
                inputs=run_context.inputs,
                llm_usage=run_context.usage,
            ),
        )

    def _fail_loop(
        self,
        *,
        run_context: _LoopRunContext,
        error: str,
    ) -> LoopExecutionFailed:
        self._loop_runs.pop(run_context.loop_execution_id)
        metadata = self._loop_metadata(run_context)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = "error"
        return LoopExecutionFailed(
            started_at=run_context.started_at,
            inputs=run_context.inputs,
            outputs={},
            metadata=loop_metadata,
            steps=run_context.loop_count,
            error=error,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                llm_usage=run_context.usage,
            ),
        )

    def _enqueue_container_result(
        self,
        *,
        parent_frame: ExecutionFrame,
        invocation_id: str,
        result: (
            LoopExecutionSucceeded
            | LoopExecutionFailed
            | IterationExecutionSucceeded
            | IterationExecutionFailed
            | IterationFramesRequested
        ),
    ) -> None:
        parent_frame.graph_runtime_state.ready_queue.put(
            ResumeTask(invocation_id=invocation_id, result=result),
        )

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

    def _start_iteration_request(
        self,
        *,
        parent_frame: ExecutionFrame,
        node_id: str,
        invocation_id: str,
        request: IterationFrameRequest,
    ) -> None:
        node = parent_frame.graph.nodes[node_id]
        if not isinstance(node, IterationNode):
            msg = f"node {node_id} cannot handle iteration await requests"
            raise TypeError(msg)
        iteration_execution_id = node.execution_id
        run_context = self._iteration_runs.get(iteration_execution_id)
        if run_context is None:
            run_context = _IterationRunContext(
                invocation_id=invocation_id,
                parent_frame_id=parent_frame.frame_id,
                iteration_node_id=node_id,
                iteration_execution_id=iteration_execution_id,
                items=request.items,
                inputs=dict(request.inputs),
                started_at=request.started_at,
                outputs={},
                duration_map={},
                usage=LLMUsage.empty_usage(),
                scheduled_count=0,
                completed_count=0,
                resume_pending=False,
                parallel_nums=request.parallel_nums,
                output_selector=list(request.output_selector),
                error_handle_mode=request.error_handle_mode,
                flatten_output=request.flatten_output,
            )
            self._iteration_runs[iteration_execution_id] = run_context
        else:
            run_context.invocation_id = invocation_id
            run_context.resume_pending = False

        for index in request.indexes:
            self._start_iteration_frame(
                parent_frame=parent_frame,
                run_context=run_context,
                index=index,
                root_node_id=request.root_node_id,
            )
            run_context.scheduled_count = max(run_context.scheduled_count, index + 1)
        self._request_iteration_frames(
            parent_frame=parent_frame,
            run_context=run_context,
        )

    def _complete_iteration_frame(self, frame: ExecutionFrame) -> bool:
        frame_context = self._iteration_frames.get(frame.frame_id)
        if frame_context is None:
            return False
        if not frame.state_manager.is_execution_complete():
            return True

        self._iteration_frames.pop(frame.frame_id)
        run_context = self._iteration_runs[frame_context.iteration_execution_id]
        parent_frame = self._frame_registry.get(run_context.parent_frame_id)

        if frame.frame_id in self._iteration_frame_failures:
            error = self._iteration_frame_failures.pop(frame.frame_id)
            return self._complete_failed_iteration_frame(
                frame=frame,
                frame_context=frame_context,
                parent_frame=parent_frame,
                run_context=run_context,
                error=error,
            )

        result = frame.graph_runtime_state.variable_pool.get(
            run_context.output_selector,
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
            run_context=run_context,
            last_frame=frame,
        )

    def _complete_failed_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_context: _IterationFrameContext,
        parent_frame: ExecutionFrame,
        run_context: _IterationRunContext,
        error: str,
    ) -> bool:
        self._complete_iteration_step(
            frame=frame,
            frame_context=frame_context,
            run_context=run_context,
        )
        match run_context.error_handle_mode:
            case ErrorHandleMode.TERMINATED:
                self._enqueue_container_result(
                    parent_frame=parent_frame,
                    invocation_id=run_context.invocation_id,
                    result=self._fail_iteration(run_context=run_context, error=error),
                )
                return True
            case ErrorHandleMode.CONTINUE_ON_ERROR:
                run_context.outputs[frame_context.iteration_index] = None
            case ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT:
                pass

        return self._continue_or_complete_iteration(
            parent_frame=parent_frame,
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
        run_context: _IterationRunContext,
        last_frame: ExecutionFrame,
    ) -> bool:
        if run_context.completed_count >= len(run_context.items):
            parent_frame.graph_runtime_state.merge_response_outputs(
                last_frame.graph_runtime_state.outputs,
            )
            self._enqueue_container_result(
                parent_frame=parent_frame,
                invocation_id=run_context.invocation_id,
                result=self._complete_iteration(run_context),
            )
            return True

        self._request_iteration_frames(
            parent_frame=parent_frame,
            run_context=run_context,
        )
        return True

    def _request_iteration_frames(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_context: _IterationRunContext,
    ) -> None:
        if run_context.resume_pending:
            return
        if run_context.scheduled_count >= len(run_context.items):
            return
        active_count = run_context.scheduled_count - run_context.completed_count
        capacity = max(run_context.parallel_nums - active_count, 0)
        if capacity == 0:
            return
        end_index = min(len(run_context.items), run_context.scheduled_count + capacity)
        indexes = tuple(range(run_context.scheduled_count, end_index))
        run_context.resume_pending = True
        self._enqueue_container_result(
            parent_frame=parent_frame,
            invocation_id=run_context.invocation_id,
            result=IterationFramesRequested(indexes=indexes),
        )

    def _complete_iteration(
        self,
        run_context: _IterationRunContext,
    ) -> IterationExecutionSucceeded:
        self._iteration_runs.pop(run_context.iteration_execution_id)
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_context),
                flatten_output=run_context.flatten_output,
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
        return IterationExecutionSucceeded(
            started_at=run_context.started_at,
            inputs=run_context.inputs,
            outputs=outputs,
            metadata=self._event_metadata(metadata),
            steps=len(run_context.items),
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=outputs,
                inputs=run_context.inputs,
                llm_usage=run_context.usage,
            ),
        )

    def _fail_iteration(
        self,
        *,
        run_context: _IterationRunContext,
        error: str,
    ) -> IterationExecutionFailed:
        self._iteration_runs.pop(run_context.iteration_execution_id)
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_context),
                flatten_output=run_context.flatten_output,
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
        return IterationExecutionFailed(
            started_at=run_context.started_at,
            inputs=run_context.inputs,
            outputs=outputs,
            metadata=self._event_metadata(metadata),
            steps=len(run_context.items),
            error=error,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                llm_usage=run_context.usage,
            ),
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
