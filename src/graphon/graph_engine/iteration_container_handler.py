from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import cast, final

from graphon.enums import (
    BuiltinNodeTypes,
    ErrorHandleMode,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import NodeRunFailedEvent
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    ContainerValue,
    IterationFrameRequest,
    build_container_value,
)
from graphon.runtime.container_state import (
    ContainerFrameState,
    IterationFrameState,
    IterationRunState,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.variables.segments import NoneSegment, SerializableSegment

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


@final
class IterationContainerHandler:
    node_type = BuiltinNodeTypes.ITERATION

    def __init__(
        self,
        frame_registry: FrameRegistry,
    ) -> None:
        self._frame_registry = frame_registry
        # ponytail: one lock; split by run id if runtime-state mutation contends.
        self._lock = threading.Lock()

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
        if not isinstance(frame_state, IterationFrameState):
            msg = f"iteration handler cannot restore {frame_state.kind} frame"
            raise TypeError(msg)
        variable_pool = frame_state.runtime_data.variable_pool
        if isinstance(variable_pool, str):
            msg = (
                f"iteration frame {frame_state.frame_id} requires a local variable pool"
            )
            raise TypeError(msg)
        self._frame_registry.materialize_child_frame_from_state(
            frame_state,
            variable_pool=variable_pool.model_copy(deep=True),
        )

    def start_await(
        self,
        *,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None:
        if not isinstance(request, IterationFrameRequest):
            msg = f"iteration handler cannot handle {type(request).__name__}"
            raise TypeError(msg)

        with self._lock:
            run_state = self._iteration_run(invocation_id)
            run_state = self._put_run_state(
                run_state.model_copy(update={"resume_pending": False}),
            )
            parent_frame = self._frame_registry.get(run_state.frame_id)
            if self._finish_failed_iteration_if_ready(
                parent_frame=parent_frame,
                run_state=run_state,
            ):
                return

            for index in request.indexes:
                run_state = self._put_run_state(
                    run_state.model_copy(
                        update={
                            "scheduled_count": max(
                                run_state.scheduled_count,
                                index + 1,
                            ),
                        },
                    ),
                )
                self._start_iteration_frame(
                    parent_frame=parent_frame,
                    run_state=run_state,
                    index=index,
                )

            self._request_iteration_frames(
                parent_frame=parent_frame,
                run_state=run_state,
            )

    def prepare_frame_event(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> None:
        with self._lock:
            frame_state = self._iteration_frame(frame.frame_id)
            run_state = self._iteration_run(frame_state.parent_invocation_id)
            if event.in_iteration_id is None:
                event.in_iteration_id = run_state.node_id
            iteration_metadata = {
                WorkflowNodeExecutionMetadataKey.ITERATION_ID: run_state.node_id,
                WorkflowNodeExecutionMetadataKey.ITERATION_INDEX: frame_state.index,
            }
            current_metadata = event.node_run_result.metadata
            if WorkflowNodeExecutionMetadataKey.ITERATION_ID not in current_metadata:
                event.node_run_result.metadata = {
                    **current_metadata,
                    **iteration_metadata,
                }

    def should_collect(
        self,
        *,
        event: GraphNodeEventBase,
    ) -> bool:
        return event.node_type != BuiltinNodeTypes.ITERATION_START

    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> None:
        with self._lock:
            frame_state = self._iteration_frame(frame.frame_id)
            self._root_runtime_state().put_container_frame(
                frame_state.model_copy(
                    update={"errors": (*frame_state.errors, event.error)},
                ),
            )

    def complete_frame(self, frame: ExecutionFrame) -> None:
        with self._lock:
            if not frame.state_manager.is_execution_complete():
                return

            root_runtime_state = self._root_runtime_state()
            frame_state = self._iteration_frame(frame.frame_id)
            self._complete_ready_iteration_frame(
                frame=frame,
                frame_state=frame_state,
            )
            root_runtime_state.pop_container_frame(frame.frame_id)
            self._frame_registry.remove(frame.frame_id)

    def _complete_ready_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: IterationFrameState,
    ) -> None:
        run_state = self._iteration_run(frame_state.parent_invocation_id)
        parent_frame = self._frame_registry.get(run_state.frame_id)
        if frame_state.errors:
            self._complete_failed_iteration_frame(
                frame=frame,
                frame_state=frame_state,
                parent_frame=parent_frame,
                run_state=run_state,
                error=frame_state.errors[0],
            )
            return

        result = frame.graph_runtime_state.variable_pool.get(
            run_state.output_selector,
        )
        output = NoneSegment() if result is None else cast(SerializableSegment, result)
        run_state = self._complete_iteration_step(
            frame=frame,
            frame_state=frame_state,
            parent_frame=parent_frame,
            run_state=run_state,
            output=output,
            store_output=True,
        )
        self._continue_or_complete_iteration(
            parent_frame=parent_frame,
            run_state=run_state,
        )

    def _complete_failed_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: IterationFrameState,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
        error: str,
    ) -> None:
        run_state = self._complete_iteration_step(
            frame=frame,
            frame_state=frame_state,
            parent_frame=parent_frame,
            run_state=run_state,
            output=NoneSegment(),
            store_output=(
                run_state.error_handle_mode == ErrorHandleMode.CONTINUE_ON_ERROR
            ),
        )
        match run_state.error_handle_mode:
            case ErrorHandleMode.TERMINATED:
                run_state = self._put_run_state(
                    run_state.model_copy(
                        update={"errors": (*run_state.errors, error)},
                    ),
                )
                self._finish_failed_iteration_if_ready(
                    parent_frame=parent_frame,
                    run_state=run_state,
                )
            case (
                ErrorHandleMode.CONTINUE_ON_ERROR
                | ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT
            ):
                self._continue_or_complete_iteration(
                    parent_frame=parent_frame,
                    run_state=run_state,
                )

    def _complete_iteration_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: IterationFrameState,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
        output: SerializableSegment,
        store_output: bool,
    ) -> IterationRunState:
        duration_map = dict(run_state.duration_map)
        duration_map[str(frame_state.index)] = (
            datetime.now(UTC).replace(tzinfo=None) - frame_state.started_at
        ).total_seconds()
        outputs = dict(run_state.outputs)
        if store_output:
            outputs[str(frame_state.index)] = output
        if frame_state.index == len(run_state.items) - 1:
            parent_frame.graph_runtime_state.merge_response_outputs(
                frame.graph_runtime_state.outputs,
            )

        return self._put_run_state(
            run_state.model_copy(
                update={
                    "outputs": outputs,
                    "duration_map": duration_map,
                    "usage": run_state.usage.plus(
                        frame.graph_runtime_state.llm_usage,
                    ),
                    "completed_count": run_state.completed_count + 1,
                },
            ),
        )

    def _continue_or_complete_iteration(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
    ) -> None:
        if self._finish_failed_iteration_if_ready(
            parent_frame=parent_frame,
            run_state=run_state,
        ):
            return
        if run_state.completed_count >= len(run_state.items):
            self._enqueue_container_result(
                runtime_state=parent_frame.graph_runtime_state,
                invocation_id=run_state.invocation_id,
                result=self._complete_iteration(run_state),
            )
            return

        self._request_iteration_frames(
            parent_frame=parent_frame,
            run_state=run_state,
        )

    def _finish_failed_iteration_if_ready(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
    ) -> bool:
        if not run_state.errors:
            return False
        if (
            run_state.completed_count < run_state.scheduled_count
            or run_state.resume_pending
        ):
            return True

        run_state = self._put_run_state(
            run_state.model_copy(update={"resume_pending": True}),
        )
        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=self._fail_iteration(
                run_state=run_state,
                error=run_state.errors[0],
            ),
        )
        return True

    def _request_iteration_frames(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
    ) -> None:
        if run_state.resume_pending or run_state.scheduled_count >= len(
            run_state.items
        ):
            return
        active_count = run_state.scheduled_count - run_state.completed_count
        capacity = max(run_state.parallel_nums - active_count, 0)
        if capacity == 0:
            return
        end_index = min(
            len(run_state.items),
            run_state.scheduled_count + capacity,
        )
        indexes = tuple(range(run_state.scheduled_count, end_index))
        run_state = self._put_run_state(
            run_state.model_copy(update={"resume_pending": True}),
        )
        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=IterationFrameRequest(
                items=run_state.items,
                root_node_id=run_state.root_node_id,
                indexes=indexes,
                output_selector=run_state.output_selector,
                error_handle_mode=run_state.error_handle_mode,
                flatten_output=run_state.flatten_output,
                parallel_nums=run_state.parallel_nums,
            ),
        )

    def _start_iteration_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: IterationRunState,
        index: int,
    ) -> None:
        variable_pool = parent_frame.graph_runtime_state.variable_pool.model_copy(
            deep=True,
        )
        variable_pool.add([run_state.node_id, "index"], index)
        variable_pool.add([run_state.node_id, "item"], run_state.items[index])
        child_runtime_state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=parent_frame.graph_runtime_state.start_at,
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            deferred_ready_queue=(
                parent_frame.graph_runtime_state.deferred_ready_queue
            ),
            graph_execution=parent_frame.graph_runtime_state.graph_execution,
        )
        child_frame_id = f"{run_state.invocation_id}:iteration:{index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=run_state.root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._root_runtime_state().put_container_frame(
            IterationFrameState(
                frame_id=child_frame_id,
                parent_invocation_id=run_state.invocation_id,
                root_node_id=run_state.root_node_id,
                index=index,
                started_at=datetime.now(UTC).replace(tzinfo=None),
                runtime_data=child_frame.graph_runtime_state.snapshot_frame(
                    copy_variable_pool=False,
                ),
            ),
        )
        child_frame.state_manager.enqueue_node(run_state.root_node_id)

    def _complete_iteration(
        self,
        run_state: IterationRunState,
    ) -> ContainerExecutionResult:
        outputs: dict[str, ContainerValue] = {
            "output": build_container_value(
                self._flatten_outputs_if_needed(
                    self._ordered_iteration_outputs(run_state),
                    flatten_output=run_state.flatten_output,
                ),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return ContainerExecutionResult(
            metadata=self._event_metadata(metadata),
            steps=len(run_state.items),
            node_run_result=ContainerNodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=outputs,
                inputs=self._inputs(run_state),
                llm_usage=run_state.usage,
            ),
        )

    def _fail_iteration(
        self,
        *,
        run_state: IterationRunState,
        error: str,
    ) -> ContainerExecutionResult:
        outputs: dict[str, ContainerValue] = {
            "output": build_container_value(
                self._flatten_outputs_if_needed(
                    self._ordered_iteration_outputs(run_state),
                    flatten_output=run_state.flatten_output,
                ),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return ContainerExecutionResult(
            metadata=self._event_metadata(metadata),
            steps=len(run_state.items),
            node_run_result=ContainerNodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                outputs=outputs,
                inputs=self._inputs(run_state),
                llm_usage=run_state.usage,
            ),
        )

    def _iteration_metadata(
        self,
        run_state: IterationRunState,
    ) -> dict[WorkflowNodeExecutionMetadataKey, object]:
        return {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: (
                run_state.usage.total_tokens
            ),
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: run_state.usage.total_price,
            WorkflowNodeExecutionMetadataKey.CURRENCY: run_state.usage.currency,
            WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: (
                run_state.duration_map
            ),
        }

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
        run_state: IterationRunState,
    ) -> list[object]:
        return [
            run_state.outputs[key].to_object()
            for key in sorted(run_state.outputs, key=int)
        ]

    def _enqueue_container_result(
        self,
        *,
        runtime_state: GraphRuntimeState,
        invocation_id: str,
        result: ContainerExecutionResult | IterationFrameRequest,
    ) -> None:
        runtime_state.enqueue_ready_task(
            ResumeTask(invocation_id=invocation_id, result=result),
        )

    def _event_metadata(
        self,
        metadata: dict[WorkflowNodeExecutionMetadataKey, object],
    ) -> dict[str, object]:
        return {key.value: value for key, value in metadata.items()}

    def _inputs(self, run_state: IterationRunState) -> dict[str, ContainerValue]:
        return {
            "iterator_selector": build_container_value(
                [item.to_object() for item in run_state.items],
            ),
        }

    def _iteration_run(self, invocation_id: str) -> IterationRunState:
        run_state = self._root_runtime_state().get_container_run(invocation_id)
        if not isinstance(run_state, IterationRunState):
            msg = f"iteration handler cannot use {run_state.kind} run"
            raise TypeError(msg)
        return run_state

    def _iteration_frame(self, frame_id: str) -> IterationFrameState:
        frame_state = self._root_runtime_state().get_container_frame(frame_id)
        if not isinstance(frame_state, IterationFrameState):
            msg = f"iteration handler cannot use {frame_state.kind} frame"
            raise TypeError(msg)
        return frame_state

    def _put_run_state(self, run_state: IterationRunState) -> IterationRunState:
        self._root_runtime_state().put_container_run(run_state)
        return run_state

    def _root_runtime_state(self) -> GraphRuntimeState:
        return self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
