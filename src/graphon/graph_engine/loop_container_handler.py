from __future__ import annotations

import contextlib
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
from graphon.graph_events.node import NodeRunFailedEvent, NodeRunSucceededEvent
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    LoopExecutionFailed,
    LoopExecutionSucceeded,
    LoopFrameCompleted,
    LoopFrameRequest,
)
from graphon.nodes.loop.entities import LoopCompletedReason
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
    FrameRuntimeData,
)
from graphon.runtime.graph_runtime_state import (
    GraphExecutionProtocol,
    GraphRuntimeState,
)
from graphon.utils.condition.processor import ConditionProcessor

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


@final
class LoopContainerHandler:
    kind = "loop"

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
        if not isinstance(request, LoopFrameRequest):
            msg = f"loop handler cannot handle {type(request).__name__}"
            raise TypeError(msg)

        with self._lock:
            parent_frame = self._frame_registry.get(frame_id)
            node = parent_frame.graph.nodes[node_id]
            if not isinstance(node, LoopNode):
                msg = f"node {node_id} cannot handle loop await requests"
                raise TypeError(msg)

            run_state = self._initialize_run_state(
                invocation_id=invocation_id,
                request=request,
            )
            if self._loop_break_conditions_reached(
                frame=parent_frame,
                node=node,
                suppress_errors=True,
            ):
                run_state = self._update_run_state(
                    invocation_id,
                    {"reached_break": True},
                )
                self._enqueue_container_result(
                    runtime_state=parent_frame.graph_runtime_state,
                    invocation_id=run_state.invocation_id,
                    result=self._complete_loop(
                        node=node,
                        run_state=run_state,
                        steps=0,
                    ),
                )
                return

            self._start_loop_frame(
                parent_frame=parent_frame,
                run_state=run_state,
                request=request,
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
            event.in_loop_id = run_state.node_id
            loop_metadata = {
                WorkflowNodeExecutionMetadataKey.LOOP_ID: run_state.node_id,
                WorkflowNodeExecutionMetadataKey.LOOP_INDEX: self._frame_index(
                    frame_state,
                ),
            }
            current_metadata = event.node_run_result.metadata
            if WorkflowNodeExecutionMetadataKey.LOOP_ID not in current_metadata:
                event.node_run_result.metadata = {
                    **current_metadata,
                    **loop_metadata,
                }
            if (
                isinstance(event, NodeRunSucceededEvent)
                and event.node_type == BuiltinNodeTypes.LOOP_END
            ):
                self._put_frame_state(
                    frame_state.model_copy(
                        update={
                            "phase_data": {
                                **dict(frame_state.phase_data),
                                "reached_break": True,
                            },
                            "runtime_data": self._frame_runtime_data(frame),
                        },
                    ),
                )

    def should_collect(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> bool:
        _ = frame
        return event.node_type != BuiltinNodeTypes.LOOP_START

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
            return self._complete_ready_loop_frame(
                frame=frame,
                frame_state=frame_state,
            )

    def _complete_ready_loop_frame(
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
        node = parent_frame.graph.nodes[run_state.node_id]
        if not isinstance(node, LoopNode):
            return True

        run_state = self._complete_loop_step(
            frame=frame,
            frame_state=frame_state,
            parent_frame=parent_frame,
            node=node,
            run_state=run_state,
        )
        phase_data = dict(frame_state.phase_data)
        if phase_data.get("failed") is True:
            error = cast(str, phase_data["error"])
            self._enqueue_container_result(
                runtime_state=parent_frame.graph_runtime_state,
                invocation_id=run_state.invocation_id,
                result=self._fail_loop(run_state=run_state, error=error),
            )
            return True

        if phase_data.get("reached_break") is True or (
            self._loop_break_conditions_reached(
                frame=parent_frame,
                node=node,
                suppress_errors=False,
            )
        ):
            run_state = self._update_run_state(
                run_state.invocation_id,
                {"reached_break": True},
            )

        run_phase_data = dict(run_state.phase_data)
        completed_count = cast(int, run_phase_data["completed_count"])
        loop_count = cast(int, run_phase_data["loop_count"])
        if cast(bool, run_phase_data["reached_break"]) or (
            completed_count >= loop_count
        ):
            self._enqueue_container_result(
                runtime_state=parent_frame.graph_runtime_state,
                invocation_id=run_state.invocation_id,
                result=self._complete_loop(
                    node=node,
                    run_state=run_state,
                    steps=loop_count,
                ),
            )
            return True

        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=LoopFrameCompleted(next_index=completed_count),
        )
        return True

    def _initialize_run_state(
        self,
        *,
        invocation_id: str,
        request: LoopFrameRequest,
    ) -> ContainerRunState:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(invocation_id)
        phase_data = dict(run_state.phase_data)
        phase_data.update({
            "inputs": dict(request.inputs),
            "loop_count": request.loop_count,
            "root_node_id": request.root_node_id,
            "loop_variable_selectors": {
                key: list(value)
                for key, value in request.loop_variable_selectors.items()
            },
            "loop_node_ids": tuple(sorted(request.loop_node_ids)),
            "duration_map": dict(
                cast(Mapping[str, float], phase_data.get("duration_map", {})),
            ),
            "variable_map": dict(
                cast(
                    Mapping[str, dict[str, object]],
                    phase_data.get("variable_map", {}),
                ),
            ),
            "usage": self._phase_usage(phase_data),
            "completed_count": cast(int, phase_data.get("completed_count", 0)),
            "reached_break": cast(bool, phase_data.get("reached_break", False)),
        })
        updated_run_state = run_state.model_copy(update={"phase_data": phase_data})
        root_runtime_state.put_container_run(updated_run_state)
        return updated_run_state

    def _start_loop_frame(
        self,
        *,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
        request: LoopFrameRequest,
    ) -> None:
        self._clear_loop_subgraph_variables(
            frame=parent_frame,
            loop_node_ids=set(request.loop_node_ids),
        )
        child_runtime_state = GraphRuntimeState(
            variable_pool=parent_frame.graph_runtime_state.variable_pool,
            start_at=time.time(),
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            graph_execution=self._graph_execution,
        )
        child_frame_id = f"{run_state.execution_id}:loop:{request.index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=request.root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._put_frame_state(
            ContainerFrameState(
                frame_id=child_frame_id,
                kind=self.kind,
                parent_invocation_id=run_state.invocation_id,
                root_node_id=request.root_node_id,
                phase_data={
                    "index": request.index,
                    "started_at": datetime.now(UTC).replace(tzinfo=None),
                },
                runtime_data=self._frame_runtime_data(child_frame),
            ),
        )
        child_frame.state_manager.enqueue_node(
            frame_id=child_frame.frame_id,
            node_id=request.root_node_id,
        )
        child_frame.state_manager.start_execution(
            frame_id=child_frame.frame_id,
            node_id=request.root_node_id,
        )

    def _complete_loop_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
        parent_frame: ExecutionFrame,
        node: LoopNode,
        run_state: ContainerRunState,
    ) -> ContainerRunState:
        phase_data = dict(run_state.phase_data)
        completed_count = cast(int, phase_data["completed_count"]) + 1
        usage = self._phase_usage(phase_data).plus(frame.graph_runtime_state.llm_usage)
        duration_map = dict(cast(Mapping[str, float], phase_data["duration_map"]))
        loop_index = self._frame_index(frame_state)
        duration_map[str(loop_index)] = (
            datetime.now(UTC).replace(tzinfo=None) - self._frame_started_at(frame_state)
        ).total_seconds()
        loop_variable_selectors = cast(
            Mapping[str, Sequence[str]],
            phase_data["loop_variable_selectors"],
        )
        variable_map = dict(
            cast(Mapping[str, dict[str, object]], phase_data["variable_map"]),
        )
        variable_map[str(loop_index)] = self._collect_loop_variable_values(
            frame=parent_frame,
            loop_variable_selectors=loop_variable_selectors,
        )
        parent_frame.graph_runtime_state.merge_response_outputs(
            frame.graph_runtime_state.outputs,
        )
        for loop_variable in node.node_data.loop_variables or []:
            selector = [run_state.node_id, loop_variable.label]
            segment = parent_frame.graph_runtime_state.variable_pool.get(selector)
            node.node_data.outputs[loop_variable.label] = (
                segment.value if segment else None
            )
        node.node_data.outputs["loop_round"] = loop_index + 1
        phase_data.update({
            "completed_count": completed_count,
            "duration_map": duration_map,
            "variable_map": variable_map,
            "usage": usage,
        })
        return self._put_run_state(
            run_state.model_copy(update={"phase_data": phase_data}),
        )

    def _complete_loop(
        self,
        *,
        node: LoopNode,
        run_state: ContainerRunState,
        steps: int,
    ) -> LoopExecutionSucceeded:
        self._root_runtime_state().pop_container_run(run_state.invocation_id)
        metadata = self._loop_metadata(run_state)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = (
            LoopCompletedReason.LOOP_BREAK
            if cast(bool, run_state.phase_data["reached_break"])
            else LoopCompletedReason.LOOP_COMPLETED.value
        )
        return LoopExecutionSucceeded(
            started_at=run_state.started_at,
            inputs=cast(Mapping[str, object], run_state.phase_data["inputs"]),
            outputs=node.node_data.outputs,
            metadata=loop_metadata,
            steps=steps,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=node.node_data.outputs,
                inputs=cast(Mapping[str, object], run_state.phase_data["inputs"]),
                llm_usage=self._phase_usage(dict(run_state.phase_data)),
            ),
        )

    def _fail_loop(
        self,
        *,
        run_state: ContainerRunState,
        error: str,
    ) -> LoopExecutionFailed:
        self._root_runtime_state().pop_container_run(run_state.invocation_id)
        metadata = self._loop_metadata(run_state)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = "error"
        return LoopExecutionFailed(
            started_at=run_state.started_at,
            inputs=cast(Mapping[str, object], run_state.phase_data["inputs"]),
            outputs={},
            metadata=loop_metadata,
            steps=cast(int, run_state.phase_data["loop_count"]),
            error=error,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                llm_usage=self._phase_usage(dict(run_state.phase_data)),
            ),
        )

    def _enqueue_container_result(
        self,
        *,
        runtime_state: GraphRuntimeState,
        invocation_id: str,
        result: LoopExecutionSucceeded | LoopExecutionFailed | LoopFrameCompleted,
    ) -> None:
        runtime_state.enqueue_ready_task(
            ResumeTask(invocation_id=invocation_id, result=result),
        )

    def _loop_metadata(
        self,
        run_state: ContainerRunState,
    ) -> dict[WorkflowNodeExecutionMetadataKey, object]:
        phase_data = dict(run_state.phase_data)
        usage = self._phase_usage(phase_data)
        return {
            WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS: usage.total_tokens,
            WorkflowNodeExecutionMetadataKey.TOTAL_PRICE: usage.total_price,
            WorkflowNodeExecutionMetadataKey.CURRENCY: usage.currency,
            WorkflowNodeExecutionMetadataKey.LOOP_DURATION_MAP: (
                phase_data["duration_map"]
            ),
            WorkflowNodeExecutionMetadataKey.LOOP_VARIABLE_MAP: (
                phase_data["variable_map"]
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
        loop_variable_selectors: Mapping[str, Sequence[str]],
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
