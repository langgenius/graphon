from __future__ import annotations

import contextlib
import threading
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
    ContainerExecutionResult,
    LoopFrameRequest,
)
from graphon.nodes.loop.loop_node import LoopNode
from graphon.runtime.container_state import (
    ContainerFrameState,
    ContainerRunState,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.utils.condition.processor import ConditionProcessor

from .frames import ExecutionFrame, FrameRegistry
from .ready_queue import ROOT_FRAME_ID, ResumeTask


@final
class LoopContainerHandler:
    def __init__(
        self,
        *,
        frame_registry: FrameRegistry,
    ) -> None:
        self._frame_registry = frame_registry
        # ponytail: one lock; split by run id if runtime-state mutation contends.
        self._lock = threading.Lock()

    def restore_frame(self, frame_state: ContainerFrameState) -> None:
        root_runtime_state = self._root_runtime_state()
        run_state = root_runtime_state.get_container_run(
            frame_state.parent_invocation_id,
        )
        parent_frame = self._frame_registry.get(run_state.frame_id)
        self._frame_registry.materialize_child_frame_from_state(
            frame_state,
            variable_pool=parent_frame.graph_runtime_state.variable_pool,
        )

    def start_await(
        self,
        *,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None:
        if not isinstance(request, LoopFrameRequest):
            msg = f"loop handler cannot handle {type(request).__name__}"
            raise TypeError(msg)

        with self._lock:
            run_state = self._initialize_run_state(
                invocation_id=invocation_id,
                request=request,
            )
            parent_frame = self._frame_registry.get(run_state.frame_id)
            node = parent_frame.graph.nodes[run_state.node_id]
            if not isinstance(node, LoopNode):
                msg = f"node {run_state.node_id} cannot handle loop await requests"
                raise TypeError(msg)
            if self._loop_break_conditions_reached(
                frame=parent_frame,
                node=node,
                suppress_errors=True,
            ):
                run_state = self._root_runtime_state().update_container_run_phase_data(
                    invocation_id,
                    {"reached_break": True},
                )
                self._enqueue_container_result(
                    runtime_state=parent_frame.graph_runtime_state,
                    invocation_id=run_state.invocation_id,
                    result=self._complete_loop(
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
                WorkflowNodeExecutionMetadataKey.LOOP_INDEX: frame_state.index,
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
                root_runtime_state.put_container_frame(
                    frame_state.model_copy(
                        update={
                            "phase_data": {
                                **dict(frame_state.phase_data),
                                "reached_break": True,
                            },
                        },
                    ),
                )

    def should_collect(
        self,
        *,
        event: GraphNodeEventBase,
    ) -> bool:
        return event.node_type != BuiltinNodeTypes.LOOP_START

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
            self._complete_ready_loop_frame(
                frame=frame,
                frame_state=frame_state,
            )

    def _complete_ready_loop_frame(
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
        node = cast(LoopNode, parent_frame.graph.nodes[run_state.node_id])

        run_state = self._complete_loop_step(
            frame=frame,
            frame_state=frame_state,
            parent_frame=parent_frame,
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
            return

        if phase_data.get("reached_break") is True or (
            self._loop_break_conditions_reached(
                frame=parent_frame,
                node=node,
                suppress_errors=False,
            )
        ):
            run_state = self._root_runtime_state().update_container_run_phase_data(
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
                    run_state=run_state,
                    steps=loop_count,
                ),
            )
            return

        self._enqueue_container_result(
            runtime_state=parent_frame.graph_runtime_state,
            invocation_id=run_state.invocation_id,
            result=LoopFrameRequest(
                inputs=cast(Mapping[str, object], run_phase_data["inputs"]),
                outputs=cast(Mapping[str, object], run_phase_data["outputs"]),
                loop_count=loop_count,
                root_node_id=cast(str, run_phase_data["root_node_id"]),
                loop_variable_selectors=cast(
                    Mapping[str, Sequence[str]],
                    run_phase_data["loop_variable_selectors"],
                ),
                loop_node_ids=frozenset(
                    cast(Sequence[str], run_phase_data["loop_node_ids"]),
                ),
                index=completed_count,
            ),
        )

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
            "outputs": dict(request.outputs),
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
        for node_id in request.loop_node_ids:
            parent_frame.graph_runtime_state.variable_pool.remove([node_id])
        child_runtime_state = GraphRuntimeState(
            variable_pool=parent_frame.graph_runtime_state.variable_pool,
            start_at=parent_frame.graph_runtime_state.start_at,
            ready_queue=parent_frame.graph_runtime_state.ready_queue,
            deferred_ready_queue=(
                parent_frame.graph_runtime_state.deferred_ready_queue
            ),
            graph_execution=parent_frame.graph_runtime_state.graph_execution,
        )
        child_frame_id = f"{run_state.invocation_id}:loop:{request.index}"
        child_frame = self._frame_registry.materialize_child_frame(
            frame_id=child_frame_id,
            root_node_id=request.root_node_id,
            graph_runtime_state=child_runtime_state,
        )
        self._root_runtime_state().put_container_frame(
            ContainerFrameState(
                frame_id=child_frame_id,
                kind="loop",
                parent_invocation_id=run_state.invocation_id,
                root_node_id=request.root_node_id,
                index=request.index,
                started_at=datetime.now(UTC).replace(tzinfo=None),
                runtime_data=child_frame.graph_runtime_state.snapshot_frame(
                    variable_pool_scope="parent",
                ),
            ),
        )
        child_frame.state_manager.enqueue_node(request.root_node_id)

    def _complete_loop_step(
        self,
        *,
        frame: ExecutionFrame,
        frame_state: ContainerFrameState,
        parent_frame: ExecutionFrame,
        run_state: ContainerRunState,
    ) -> ContainerRunState:
        phase_data = dict(run_state.phase_data)
        completed_count = cast(int, phase_data["completed_count"]) + 1
        usage = self._phase_usage(phase_data).plus(frame.graph_runtime_state.llm_usage)
        duration_map = dict(cast(Mapping[str, float], phase_data["duration_map"]))
        loop_index = frame_state.index
        duration_map[str(loop_index)] = (
            datetime.now(UTC).replace(tzinfo=None) - frame_state.started_at
        ).total_seconds()
        loop_variable_selectors = cast(
            Mapping[str, Sequence[str]],
            phase_data["loop_variable_selectors"],
        )
        outputs = dict(cast(Mapping[str, object], phase_data["outputs"]))
        variable_map = dict(
            cast(Mapping[str, dict[str, object]], phase_data["variable_map"]),
        )
        loop_variable_values: dict[str, object] = {}
        for key, selector in loop_variable_selectors.items():
            segment = parent_frame.graph_runtime_state.variable_pool.get(selector)
            if segment is None:
                msg = f"loop variable {key} is missing"
                raise ValueError(msg)
            loop_variable_values[key] = segment.value
        variable_map[str(loop_index)] = loop_variable_values
        parent_frame.graph_runtime_state.merge_response_outputs(
            frame.graph_runtime_state.outputs,
        )
        outputs.update(loop_variable_values)
        outputs["loop_round"] = loop_index + 1
        phase_data.update({
            "completed_count": completed_count,
            "duration_map": duration_map,
            "outputs": outputs,
            "variable_map": variable_map,
            "usage": usage,
        })
        updated_run_state = run_state.model_copy(
            update={"phase_data": phase_data},
        )
        self._root_runtime_state().put_container_run(updated_run_state)
        return updated_run_state

    def _complete_loop(
        self,
        *,
        run_state: ContainerRunState,
        steps: int,
    ) -> ContainerExecutionResult:
        metadata = self._loop_metadata(run_state)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = (
            "loop_break"
            if cast(bool, run_state.phase_data["reached_break"])
            else "loop_completed"
        )
        return ContainerExecutionResult(
            metadata=loop_metadata,
            steps=steps,
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                metadata=metadata,
                outputs=cast(Mapping[str, object], run_state.phase_data["outputs"]),
                inputs=cast(Mapping[str, object], run_state.phase_data["inputs"]),
                llm_usage=self._phase_usage(dict(run_state.phase_data)),
            ),
        )

    def _fail_loop(
        self,
        *,
        run_state: ContainerRunState,
        error: str,
    ) -> ContainerExecutionResult:
        metadata = self._loop_metadata(run_state)
        loop_metadata = self._event_metadata(metadata)
        loop_metadata[WorkflowNodeExecutionMetadataKey.COMPLETED_REASON.value] = "error"
        inputs = cast(Mapping[str, object], run_state.phase_data["inputs"])
        return ContainerExecutionResult(
            metadata=loop_metadata,
            steps=cast(int, run_state.phase_data["loop_count"]),
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error=error,
                metadata=metadata,
                inputs=inputs,
                llm_usage=self._phase_usage(dict(run_state.phase_data)),
            ),
        )

    def _enqueue_container_result(
        self,
        *,
        runtime_state: GraphRuntimeState,
        invocation_id: str,
        result: ContainerExecutionResult | LoopFrameRequest,
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

    def _phase_usage(self, phase_data: Mapping[str, object]) -> LLMUsage:
        value = phase_data.get("usage", LLMUsage.empty_usage())
        if isinstance(value, LLMUsage):
            return value
        return LLMUsage.model_validate(value)

    def _root_runtime_state(self) -> GraphRuntimeState:
        return self._frame_registry.get(ROOT_FRAME_ID).graph_runtime_state
