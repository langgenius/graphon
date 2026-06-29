"""Engine-owned execution for iteration container nodes."""

from __future__ import annotations

import threading
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
from graphon.graph_events.node import NodeRunFailedEvent
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    IterationExecutionFailed,
    IterationExecutionSucceeded,
    IterationFrameRequest,
    IterationFramesRequested,
)
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.runtime.container_state import ContainerFrameState, FrameRuntimeData
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


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
    kind = "iteration"

    def __init__(
        self,
        *,
        frame_registry: FrameRegistry,
        graph_execution: GraphExecutionProtocol,
    ) -> None:
        self._frame_registry = frame_registry
        self._graph_execution = graph_execution
        self._iteration_runs: dict[str, _IterationRunContext] = {}
        self._iteration_frames: dict[str, _IterationFrameContext] = {}
        self._iteration_frame_failures: dict[str, str] = {}
        # ponytail: one lock; split by run id if bookkeeping becomes contended.
        self._lock = threading.RLock()

    def prepare_frame_event(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> None:
        with self._lock:
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
        with self._lock:
            if frame.frame_id not in self._iteration_frames:
                return True
            return event.node_type != BuiltinNodeTypes.ITERATION_START

    def start_await(
        self,
        *,
        frame_id: str,
        node_id: str,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None:
        if not isinstance(request, IterationFrameRequest):
            msg = f"iteration handler cannot handle {type(request).__name__}"
            raise TypeError(msg)
        with self._lock:
            parent_frame = self._frame_registry.get(frame_id)
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
        with self._lock:
            if frame.frame_id in self._iteration_frames:
                self._iteration_frame_failures[frame.frame_id] = event.error
                return True
            return False

    def complete_frame(self, frame: ExecutionFrame) -> bool:
        with self._lock:
            return self._complete_iteration_frame(frame)

    def _enqueue_container_result(
        self,
        *,
        parent_frame: ExecutionFrame,
        invocation_id: str,
        result: (
            IterationExecutionSucceeded
            | IterationExecutionFailed
            | IterationFramesRequested
        ),
    ) -> None:
        parent_frame.graph_runtime_state.ready_queue.put(
            ResumeTask(invocation_id=invocation_id, result=result),
        )

    def _event_metadata(
        self,
        metadata: dict[WorkflowNodeExecutionMetadataKey, object],
    ) -> dict[str, object]:
        return {key.value: value for key, value in metadata.items()}

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
        self._root_runtime_state().pop_container_frame(frame.frame_id)
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
        self._root_runtime_state().put_container_frame(
            ContainerFrameState(
                frame_id=child_frame_id,
                kind=self.kind,
                parent_invocation_id=run_context.invocation_id,
                root_node_id=root_node_id,
                phase_data={
                    "index": index,
                    "started_at": datetime.now(UTC).replace(tzinfo=None),
                },
                runtime_data=self._frame_runtime_data(child_frame),
            ),
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

    def _frame_runtime_data(self, frame: ExecutionFrame) -> FrameRuntimeData:
        return FrameRuntimeData(
            variable_pool=frame.graph_runtime_state.variable_pool.model_copy(
                deep=True,
            ),
            outputs=frame.graph_runtime_state.outputs,
            llm_usage=frame.graph_runtime_state.llm_usage,
            node_run_steps=frame.graph_runtime_state.node_run_steps,
            graph_node_states={
                node_id: node.state for node_id, node in frame.graph.nodes.items()
            },
            graph_edge_states={
                edge_id: edge.state for edge_id, edge in frame.graph.edges.items()
            },
        )

    def _root_runtime_state(self) -> GraphRuntimeState:
        return self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
