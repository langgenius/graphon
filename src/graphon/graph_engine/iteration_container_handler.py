from __future__ import annotations

import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast, final

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
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


@final
class IterationContainerHandler:
    kind = "iteration"

    def __init__(
        self,
        *,
        frame_registry: FrameRegistry,
        graph_execution: GraphExecutionProtocol,
    ) -> None:
        self._frame_registry = frame_registry
        self._graph_execution = graph_execution
        # ponytail: one lock; split by run id if runtime-state mutation contends.
        self._lock = threading.RLock()

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
            node = parent_frame.graph.nodes[node_id]
            if not isinstance(node, IterationNode):
                msg = f"node {node_id} cannot handle iteration await requests"
                raise TypeError(msg)

            run_state = self._initialize_run_state(
                invocation_id=invocation_id,
                request=request,
            )
            for index in request.indexes:
                self._start_iteration_frame(
                    parent_frame=parent_frame,
                    run_state=run_state,
                    index=index,
                    root_node_id=request.root_node_id,
                )
                run_state = self._update_run_state(
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
                WorkflowNodeExecutionMetadataKey.ITERATION_INDEX: self._frame_index(
                    frame_state,
                ),
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
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> bool:
        _ = frame
        return event.node_type != BuiltinNodeTypes.ITERATION_START

    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> bool:
        with self._lock:
            root_runtime_state = self._root_runtime_state()
            if not root_runtime_state.has_container_frame(frame.frame_id):
                return False
            frame_state = root_runtime_state.get_container_frame(frame.frame_id)
            if frame_state.kind != self.kind:
                return False
            self._put_frame_state(
                frame_state.model_copy(
                    update={
                        "phase_data": {
                            **dict(frame_state.phase_data),
                            "failed": True,
                            "error": event.error,
                        },
                        "runtime_data": self._frame_runtime_data(frame),
                    },
                ),
            )
            return True

    def complete_frame(self, frame: ExecutionFrame) -> bool:
        with self._lock:
            root_runtime_state = self._root_runtime_state()
            if not root_runtime_state.has_container_frame(frame.frame_id):
                return False
            frame_state = root_runtime_state.get_container_frame(frame.frame_id)
            if frame_state.kind != self.kind:
                return False
            if not frame.state_manager.is_execution_complete():
                self._put_frame_state(
                    frame_state.model_copy(
                        update={"runtime_data": self._frame_runtime_data(frame)},
                    ),
                )
                return True

            frame_state = root_runtime_state.pop_container_frame(frame.frame_id)
            return self._complete_ready_iteration_frame(
                frame=frame,
                frame_state=frame_state,
            )

    def _complete_ready_iteration_frame(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
    ) -> bool:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(
            frame_state.parent_invocation_id,
        )
        parent_frame = self._frame_registry.get(run_state.frame_id)
        phase_data = dict(frame_state.phase_data)
        if phase_data.get("failed") is True:
            error = cast(str, phase_data["error"])
            return self._complete_failed_iteration_frame(
                frame=frame,
                frame_state=frame_state,
                parent_frame=parent_frame,
                run_state=run_state,
                error=error,
            )

        result = frame.graph_runtime_state.variable_pool.get(
            self._output_selector(run_state),
        )
        run_state = self._complete_iteration_step(
            frame=frame,
            frame_state=frame_state,
            run_state=run_state,
            output=None if result is None else result.to_object(),
            store_output=True,
        )
        return self._continue_or_complete_iteration(
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
    ) -> bool:
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
                self._enqueue_container_result(
                    runtime_state=parent_frame.graph_runtime_state,
                    invocation_id=run_state.invocation_id,
                    result=self._fail_iteration(run_state=run_state, error=error),
                )
                return True
            case (
                ErrorHandleMode.CONTINUE_ON_ERROR
                | ErrorHandleMode.REMOVE_ABNORMAL_OUTPUT
            ):
                return self._continue_or_complete_iteration(
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
        index = self._frame_index(frame_state)
        duration_map[str(index)] = (
            datetime.now(UTC).replace(tzinfo=None) - self._frame_started_at(frame_state)
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
    ) -> bool:
        if self._completed_count(run_state) >= len(self._items(run_state)):
            parent_frame.graph_runtime_state.merge_response_outputs(
                last_frame.graph_runtime_state.outputs,
            )
            self._enqueue_container_result(
                runtime_state=parent_frame.graph_runtime_state,
                invocation_id=run_state.invocation_id,
                result=self._complete_iteration(run_state),
            )
            return True

        self._request_iteration_frames(
            parent_frame=parent_frame,
            run_state=run_state,
        )
        return True

    def _request_iteration_frames(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
    ) -> None:
        if self._resume_pending(run_state):
            return
        items = self._items(run_state)
        scheduled_count = self._scheduled_count(run_state)
        if scheduled_count >= len(items):
            return
        active_count = scheduled_count - self._completed_count(run_state)
        capacity = max(self._parallel_nums(run_state) - active_count, 0)
        if capacity == 0:
            return
        end_index = min(len(items), scheduled_count + capacity)
        indexes = tuple(range(scheduled_count, end_index))
        run_state = self._update_run_state(
            run_state.invocation_id, {"resume_pending": True}
        )
        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=IterationFramesRequested(indexes=indexes),
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
            "inputs": dict(request.inputs),
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
            start_at=time.time(),
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            graph_execution=self._graph_execution,
        )
        child_frame_id = f"{run_state.execution_id}:iteration:{index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._put_frame_state(
            ContainerFrameState(
                frame_id=child_frame_id,
                kind=self.kind,
                parent_invocation_id=run_state.invocation_id,
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

    def _complete_iteration(
        self,
        run_state: ContainerRunState,
    ) -> IterationExecutionSucceeded:
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_state),
                flatten_output=self._flatten_output(run_state),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return IterationExecutionSucceeded(
            started_at=run_state.started_at,
            inputs=self._inputs(run_state),
            outputs=outputs,
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
    ) -> IterationExecutionFailed:
        outputs = {
            "output": self._flatten_outputs_if_needed(
                self._ordered_iteration_outputs(run_state),
                flatten_output=self._flatten_output(run_state),
            ),
        }
        metadata = self._iteration_metadata(run_state)
        return IterationExecutionFailed(
            started_at=run_state.started_at,
            inputs=self._inputs(run_state),
            outputs=outputs,
            metadata=self._event_metadata(metadata),
            steps=len(self._items(run_state)),
            error=error,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
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
        result: (
            IterationExecutionSucceeded
            | IterationExecutionFailed
            | IterationFramesRequested
        ),
    ) -> None:
        runtime_state.enqueue_ready_task(
            ResumeTask(invocation_id=invocation_id, result=result),
        )

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

    def _event_metadata(
        self,
        metadata: dict[WorkflowNodeExecutionMetadataKey, object],
    ) -> dict[str, object]:
        return {key.value: value for key, value in metadata.items()}

    def _frame_index(self, frame_state: ContainerFrameState) -> int:
        return cast(int, frame_state.phase_data["index"])

    def _frame_started_at(self, frame_state: ContainerFrameState) -> datetime:
        value = frame_state.phase_data["started_at"]
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(cast(str, value))

    def _phase_usage(self, phase_data: Mapping[str, object]) -> LLMUsage:
        value = phase_data.get("usage", LLMUsage.empty_usage())
        if isinstance(value, LLMUsage):
            return value
        return LLMUsage.model_validate(value)

    def _inputs(self, run_state: ContainerRunState) -> Mapping[str, object]:
        return cast(Mapping[str, object], run_state.phase_data["inputs"])

    def _items(self, run_state: ContainerRunState) -> tuple[object, ...]:
        return cast(tuple[object, ...], run_state.phase_data["items"])

    def _output_selector(self, run_state: ContainerRunState) -> Sequence[str]:
        return cast(Sequence[str], run_state.phase_data["output_selector"])

    def _error_handle_mode(self, run_state: ContainerRunState) -> ErrorHandleMode:
        return cast(ErrorHandleMode, run_state.phase_data["error_handle_mode"])

    def _flatten_output(self, run_state: ContainerRunState) -> bool:
        return cast(bool, run_state.phase_data["flatten_output"])

    def _parallel_nums(self, run_state: ContainerRunState) -> int:
        return cast(int, run_state.phase_data["parallel_nums"])

    def _scheduled_count(self, run_state: ContainerRunState) -> int:
        return cast(int, run_state.phase_data["scheduled_count"])

    def _completed_count(self, run_state: ContainerRunState) -> int:
        return cast(int, run_state.phase_data["completed_count"])

    def _resume_pending(self, run_state: ContainerRunState) -> bool:
        return cast(bool, run_state.phase_data["resume_pending"])

    def _update_run_state(
        self,
        invocation_id: str,
        updates: Mapping[str, object],
    ) -> ContainerRunState:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(invocation_id)
        return self._put_run_state(
            run_state.model_copy(
                update={"phase_data": {**dict(run_state.phase_data), **updates}},
            ),
        )

    def _put_run_state(self, run_state: ContainerRunState) -> ContainerRunState:
        self._root_runtime_state().put_container_run(run_state)
        return run_state

    def _put_frame_state(self, frame_state: ContainerFrameState) -> None:
        self._root_runtime_state().put_container_frame(frame_state)

    def _root_runtime_state(self) -> GraphRuntimeState:
        return self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
