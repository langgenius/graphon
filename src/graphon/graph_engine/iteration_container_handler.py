from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
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
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    IterationFrameRequest,
)
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


@final
class IterationContainerHandler:
    def __init__(
        self,
        *,
        frame_registry: FrameRegistry,
    ) -> None:
        self._frame_registry = frame_registry
        # ponytail: one lock; split by run id if runtime-state mutation contends.
        self._lock = threading.Lock()

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
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
            run_state = self._initialize_run_state(
                invocation_id=invocation_id,
                request=request,
            )
            parent_frame = self._frame_registry.get(run_state.frame_id)
            if self._finish_failed_iteration_if_ready(
                parent_frame=parent_frame,
                run_state=run_state,
            ):
                return
            for index in request.indexes:
                self._start_iteration_frame(
                    parent_frame=parent_frame,
                    run_state=run_state,
                    index=index,
                    root_node_id=request.root_node_id,
                )
                run_state = self._root_runtime_state().update_container_run_phase_data(
                    invocation_id,
                    {
                        "scheduled_count": max(
                            self._scheduled_count(run_state), index + 1
                        )
                    },
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
            root_runtime_state = self._root_runtime_state()
            frame_state = root_runtime_state.get_container_frame(frame.frame_id)
            run_state = root_runtime_state.get_container_run(
                frame_state.parent_invocation_id,
            )
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
            frame_state = self._root_runtime_state().get_container_frame(frame.frame_id)
            self._root_runtime_state().put_container_frame(
                frame_state.model_copy(
                    update={
                        "phase_data": {
                            **dict(frame_state.phase_data),
                            "failed": True,
                            "error": event.error,
                        },
                    },
                ),
            )

    def complete_frame(self, frame: ExecutionFrame) -> None:
        with self._lock:
            if not frame.state_manager.is_execution_complete():
                return

            root_runtime_state = self._root_runtime_state()
            frame_state = root_runtime_state.pop_container_frame(frame.frame_id)
            self._frame_registry.remove(frame.frame_id)
            self._complete_ready_iteration_frame(
                frame=frame,
                frame_state=frame_state,
            )

    def _complete_ready_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
    ) -> None:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(
            frame_state.parent_invocation_id,
        )
        parent_frame = self._frame_registry.get(run_state.frame_id)
        phase_data = dict(frame_state.phase_data)
        if phase_data.get("failed") is True:
            error = cast(str, phase_data["error"])
            self._complete_failed_iteration_frame(
                frame=frame,
                frame_state=frame_state,
                parent_frame=parent_frame,
                run_state=run_state,
                error=error,
            )
            return

        result = frame.graph_runtime_state.variable_pool.get(
            cast(list[str], run_state.phase_data["output_selector"]),
        )
        run_state = self._complete_iteration_step(
            frame=frame,
            frame_state=frame_state,
            run_state=run_state,
            output=None if result is None else result.to_object(),
            store_output=True,
        )
        self._continue_or_complete_iteration(
            parent_frame=parent_frame,
            run_state=run_state,
            last_frame=frame,
        )

    def _complete_failed_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
        error: str,
    ) -> None:
        run_state = self._complete_iteration_step(
            frame=frame,
            frame_state=frame_state,
            run_state=run_state,
            output=None,
            store_output=self._error_handle_mode(run_state)
            == ErrorHandleMode.CONTINUE_ON_ERROR,
        )
        match self._error_handle_mode(run_state):
            case ErrorHandleMode.TERMINATED:
                phase_data = dict(run_state.phase_data)
                phase_data.setdefault("error", error)
                phase_data["failed"] = True
                run_state = self._put_run_state(
                    run_state.model_copy(update={"phase_data": phase_data}),
                )
                self._finish_failed_iteration_if_ready(
                    parent_frame=parent_frame,
                    run_state=run_state,
                )
                return
            case (
                ErrorHandleMode.CONTINUE_ON_ERROR
                | ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT
            ):
                self._continue_or_complete_iteration(
                    parent_frame=parent_frame,
                    run_state=run_state,
                    last_frame=frame,
                )

    def _complete_iteration_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
        run_state: ContainerRunState,
        output: object,
        store_output: bool,
    ) -> ContainerRunState:
        phase_data = dict(run_state.phase_data)
        completed_count = self._completed_count(run_state) + 1
        duration_map = dict(cast(Mapping[str, float], phase_data["duration_map"]))
        index = frame_state.index
        duration_map[str(index)] = (
            datetime.now(UTC).replace(tzinfo=None) - frame_state.started_at
        ).total_seconds()
        outputs = dict(cast(Mapping[str, object], phase_data["outputs"]))
        if store_output:
            outputs[str(index)] = output
        phase_data.update({
            "outputs": outputs,
            "duration_map": duration_map,
            "usage": self._phase_usage(phase_data).plus(
                frame.graph_runtime_state.llm_usage,
            ),
            "completed_count": completed_count,
        })
        return self._put_run_state(
            run_state.model_copy(update={"phase_data": phase_data}),
        )

    def _continue_or_complete_iteration(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
        last_frame: ExecutionFrame,
    ) -> None:
        if self._finish_failed_iteration_if_ready(
            parent_frame=parent_frame,
            run_state=run_state,
        ):
            return
        if self._completed_count(run_state) >= len(self._items(run_state)):
            parent_frame.graph_runtime_state.merge_response_outputs(
                last_frame.graph_runtime_state.outputs,
            )
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
        run_state: ContainerRunState,
    ) -> bool:
        phase_data = run_state.phase_data
        if phase_data.get("failed") is not True:
            return False
        if self._completed_count(run_state) < self._scheduled_count(run_state) or cast(
            bool, phase_data["resume_pending"]
        ):
            return True

        run_state = self._root_runtime_state().update_container_run_phase_data(
            run_state.invocation_id,
            {"resume_pending": True},
        )
        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=self._fail_iteration(
                run_state=run_state,
                error=cast(str, phase_data["error"]),
            ),
        )
        return True

    def _request_iteration_frames(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
    ) -> None:
        if cast(bool, run_state.phase_data["resume_pending"]):
            return
        items = self._items(run_state)
        scheduled_count = self._scheduled_count(run_state)
        if scheduled_count >= len(items):
            return
        active_count = scheduled_count - self._completed_count(run_state)
        capacity = max(
            cast(int, run_state.phase_data["parallel_nums"]) - active_count,
            0,
        )
        if capacity == 0:
            return
        end_index = min(len(items), scheduled_count + capacity)
        indexes = tuple(range(scheduled_count, end_index))
        run_state = self._root_runtime_state().update_container_run_phase_data(
            run_state.invocation_id, {"resume_pending": True}
        )
        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=IterationFrameRequest(
                items=items,
                root_node_id=cast(str, run_state.phase_data["root_node_id"]),
                indexes=indexes,
                output_selector=cast(
                    Sequence[str],
                    run_state.phase_data["output_selector"],
                ),
                error_handle_mode=self._error_handle_mode(run_state),
                flatten_output=self._flatten_output(run_state),
                parallel_nums=cast(int, run_state.phase_data["parallel_nums"]),
            ),
        )

    def _initialize_run_state(
        self,
        *,
        invocation_id: str,
        request: IterationFrameRequest,
    ) -> ContainerRunState:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(invocation_id)
        phase_data = dict(run_state.phase_data)
        phase_data.update({
            "items": tuple(request.items),
            "root_node_id": request.root_node_id,
            "output_selector": list(request.output_selector),
            "error_handle_mode": request.error_handle_mode,
            "flatten_output": request.flatten_output,
            "parallel_nums": request.parallel_nums,
            "outputs": dict(cast(Mapping[str, object], phase_data.get("outputs", {}))),
            "duration_map": dict(
                cast(Mapping[str, float], phase_data.get("duration_map", {})),
            ),
            "usage": self._phase_usage(phase_data),
            "scheduled_count": cast(int, phase_data.get("scheduled_count", 0)),
            "completed_count": cast(int, phase_data.get("completed_count", 0)),
            "resume_pending": False,
        })
        return self._put_run_state(
            run_state.model_copy(update={"phase_data": phase_data}),
        )

    def _start_iteration_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
        index: int,
        root_node_id: str,
    ) -> None:
        variable_pool = parent_frame.graph_runtime_state.variable_pool.model_copy(
            deep=True,
        )
        variable_pool.add([run_state.node_id, "index"], index)
        variable_pool.add([run_state.node_id, "item"], self._items(run_state)[index])
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
            root_node_id=root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._root_runtime_state().put_container_frame(
            ContainerFrameState(
                frame_id=child_frame_id,
                kind="iteration",
                parent_invocation_id=run_state.invocation_id,
                root_node_id=root_node_id,
                index=index,
                started_at=datetime.now(UTC).replace(tzinfo=None),
                runtime_data=child_frame.graph_runtime_state.snapshot_frame(
                    copy_variable_pool=False,
                ),
            ),
        )
        child_frame.state_manager.enqueue_node(root_node_id)

    def _complete_iteration(
        self,
        run_state: ContainerRunState,
    ) -> ContainerExecutionResult:
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_state),
                flatten_output=self._flatten_output(run_state),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return ContainerExecutionResult(
            metadata=self._event_metadata(metadata),
            steps=len(self._items(run_state)),
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=outputs,
                inputs=self._inputs(run_state),
                llm_usage=self._phase_usage(run_state.phase_data),
            ),
        )

    def _fail_iteration(
        self,
        *,
        run_state: ContainerRunState,
        error: str,
    ) -> ContainerExecutionResult:
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_state),
                flatten_output=self._flatten_output(run_state),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return ContainerExecutionResult(
            metadata=self._event_metadata(metadata),
            steps=len(self._items(run_state)),
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                outputs=outputs,
                inputs=self._inputs(run_state),
                llm_usage=self._phase_usage(run_state.phase_data),
            ),
        )

    def _iteration_metadata(
        self,
        run_state: ContainerRunState,
    ) -> dict[WorkflowNodeExecutionMetadataKey, object]:
        phase_data = dict(run_state.phase_data)
        usage = self._phase_usage(phase_data)
        return {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
            WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
            WorkflowNodeExecutionMetadataKey.ITERATION_DURATION_MAP: phase_data[
                "duration_map"
            ],
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

    def _ordered_iteration_outputs(self, run_state: ContainerRunState) -> list[object]:
        outputs = cast(Mapping[str, object], run_state.phase_data["outputs"])
        return [outputs[key] for key in sorted(outputs, key=int)]

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

    def _phase_usage(self, phase_data: Mapping[str, object]) -> LLMUsage:
        value = phase_data.get("usage", LLMUsage.empty_usage())
        if isinstance(value, LLMUsage):
            return value
        return LLMUsage.model_validate(value)

    def _inputs(self, run_state: ContainerRunState) -> Mapping[str, object]:
        return {"iterator_selector": list(self._items(run_state))}

    def _items(self, run_state: ContainerRunState) -> tuple[object, ...]:
        return tuple(cast(Sequence[object], run_state.phase_data["items"]))

    def _error_handle_mode(self, run_state: ContainerRunState) -> ErrorHandleMode:
        return ErrorHandleMode(run_state.phase_data["error_handle_mode"])

    def _flatten_output(self, run_state: ContainerRunState) -> bool:
        return cast(bool, run_state.phase_data["flatten_output"])

    def _scheduled_count(self, run_state: ContainerRunState) -> int:
        return cast(int, run_state.phase_data["scheduled_count"])

    def _completed_count(self, run_state: ContainerRunState) -> int:
        return cast(int, run_state.phase_data["completed_count"])

    def _put_run_state(self, run_state: ContainerRunState) -> ContainerRunState:
        self._root_runtime_state().put_container_run(run_state)
        return run_state

    def _root_runtime_state(self) -> GraphRuntimeState:
        return self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
