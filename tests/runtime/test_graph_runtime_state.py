import json
from datetime import UTC, datetime
from time import time
from unittest.mock import MagicMock

import pytest

from graphon.enums import ErrorHandleMode
from graphon.file import File, FileTransferMethod, FileType
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.graph_engine.ready_queue.protocol import ReadyTask, ResumeTask, StartTask
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.nodes.container_effects import (
    IterationFrameRequest,
    build_container_value,
)
from graphon.runtime.container_state import (
    FrameRuntimeData,
    IterationFrameState,
    IterationRunState,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper
from graphon.runtime.ready_queue import ReadyQueue
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import ArrayFileSegment, FileSegment
from graphon.variables.variables import StringVariable

CONVERSATION_VARIABLE_NODE_ID = "conversation"


class _PrefixedReadyQueue:
    def __init__(self) -> None:
        self._queue = InMemoryReadyQueue()

    def put(self, item: ReadyTask) -> None:
        self._queue.put(item)

    def get(self, timeout: float | None = None) -> ReadyTask:
        return self._queue.get(timeout)

    def task_done(self) -> None:
        self._queue.task_done()

    def qsize(self) -> int:
        return self._queue.qsize()

    def drain(self) -> list[ReadyTask]:
        return self._queue.drain()

    def dumps(self) -> str:
        return f"prefixed:{self._queue.dumps()}"

    def loads(self, data: str) -> None:
        if not data.startswith("prefixed:"):
            msg = "invalid prefixed queue snapshot"
            raise ValueError(msg)
        self._queue.loads(data.removeprefix("prefixed:"))


class TestGraphRuntimeState:
    def test_execution_context_defaults_to_empty_context(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        with state.execution_context:
            assert state.execution_context is not None

    def test_property_getters(self) -> None:
        variable_pool = VariablePool()
        start_time = time()

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=start_time)

        assert state.variable_pool == variable_pool
        assert state.start_at == start_time
        assert state.total_tokens == 0
        assert state.node_run_steps == 0

    def test_outputs_immutability(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        outputs1 = state.outputs
        outputs2 = state.outputs
        assert outputs1 == outputs2
        assert outputs1 is not outputs2

        outputs = state.outputs
        outputs["test"] = "value"
        assert "test" not in state.outputs

        state.set_output("key1", "value1")
        assert state.get_output("key1") == "value1"

    def test_merge_response_outputs_appends_answer_and_overwrites_others(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        state.merge_response_outputs({"answer": "Hello", "status": "draft"})
        state.merge_response_outputs({"answer": " world", "status": "final"})

        assert state.get_output("answer") == "Hello world"
        assert state.get_output("status") == "final"

    def test_llm_usage_immutability(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        usage1 = state.llm_usage
        usage2 = state.llm_usage
        assert usage1 is not usage2

    def test_type_validation(self) -> None:
        with pytest.raises(ValueError, match="node_run_steps must be non-negative"):
            GraphRuntimeState(
                variable_pool=VariablePool(),
                start_at=time(),
                node_run_steps=-1,
            )

    def test_helper_methods(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        initial_steps = state.node_run_steps
        state.increment_node_run_steps()
        assert state.node_run_steps == initial_steps + 1

        initial_tokens = state.total_tokens
        state.add_llm_usage(LLMUsage.from_metadata({"total_tokens": 50}))
        assert state.total_tokens == initial_tokens + 50
        assert state.llm_usage.total_tokens == 50

    def test_ready_queue_default_instantiation(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        queue = state.ready_queue

        assert isinstance(queue, InMemoryReadyQueue)

    def test_deferred_ready_tasks_round_trip_in_runtime_snapshot(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        first = StartTask(frame_id="root", node_id="a")
        second = StartTask(frame_id="child", node_id="b")
        state.defer_ready_task(first)
        state.defer_ready_task(second)

        restored = GraphRuntimeState.from_snapshot(state.dumps())

        assert restored.drain_deferred_ready_tasks() == [first, second]
        assert restored.drain_deferred_ready_tasks() == []

    def test_custom_ready_queues_round_trip_with_supplied_factory(self) -> None:
        ready_queue: ReadyQueue = _PrefixedReadyQueue()
        deferred_ready_queue: ReadyQueue = _PrefixedReadyQueue()
        state = GraphRuntimeState(
            variable_pool=VariablePool(),
            start_at=time(),
            ready_queue=ready_queue,
            deferred_ready_queue=deferred_ready_queue,
        )
        live_task = StartTask(frame_id="root", node_id="live")
        deferred_task = StartTask(frame_id="root", node_id="deferred")
        state.ready_queue.put(live_task)
        state.defer_ready_task(deferred_task)

        restored = GraphRuntimeState.from_snapshot(
            state.dumps(),
            ready_queue_factory=_PrefixedReadyQueue,
        )

        assert isinstance(restored.ready_queue, _PrefixedReadyQueue)
        assert restored.ready_queue.drain() == [live_task]
        assert restored.drain_deferred_ready_tasks() == [deferred_task]

    def test_container_runtime_state_preserves_file_values(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        file_value = File(
            file_id="file-1",
            file_type=FileType.DOCUMENT,
            transfer_method=FileTransferMethod.REMOTE_URL,
            remote_url="https://example.com/resume.pdf",
            filename="resume.pdf",
            extension=".pdf",
            mime_type="application/pdf",
            size=128,
        )
        request = IterationFrameRequest(
            items=(build_container_value(file_value),),
            root_node_id="iteration-start",
            indexes=(0,),
            output_selector=("iteration", "item"),
            error_handle_mode=ErrorHandleMode.TERMINATED,
            flatten_output=False,
            parallel_nums=1,
        )
        run = IterationRunState(
            invocation_id="invocation-1",
            frame_id="root",
            node_id="iteration",
            started_at=datetime.fromtimestamp(1, UTC).replace(tzinfo=None),
            items=(build_container_value(file_value),),
            root_node_id="iteration-start",
            output_selector=("iteration", "item"),
            error_handle_mode=ErrorHandleMode.TERMINATED,
            flatten_output=False,
            parallel_nums=1,
        )
        frame = IterationFrameState(
            frame_id="exec-iteration:iteration:0",
            parent_invocation_id="invocation-1",
            root_node_id="iteration-start",
            index=0,
            started_at=datetime.fromtimestamp(1, UTC).replace(tzinfo=None),
            runtime_data=FrameRuntimeData(
                variable_pool=VariablePool(),
                outputs={},
                llm_usage=LLMUsage.empty_usage(),
                node_run_steps=0,
                graph_node_states={},
                graph_edge_states={},
            ),
        )
        state.put_container_run(run)
        state.put_container_frame(frame)
        state.ready_queue.put(
            ResumeTask(invocation_id=run.invocation_id, result=request),
        )

        restored = GraphRuntimeState.from_snapshot(state.dumps())

        restored_run = restored.get_container_run("invocation-1")
        assert isinstance(restored_run, IterationRunState)
        assert isinstance(restored_run.items[0], FileSegment)
        assert restored_run.items[0].value == file_value
        restored_task = restored.ready_queue.get(timeout=0.01)
        assert isinstance(restored_task, ResumeTask)
        assert isinstance(restored_task.result, IterationFrameRequest)
        assert isinstance(restored_task.result.items[0], FileSegment)
        assert restored_task.result.items[0].value == file_value
        assert restored.get_container_frame("exec-iteration:iteration:0") == frame

    def test_graph_execution_lazy_instantiation(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        execution = state.graph_execution

        assert isinstance(execution, GraphExecution)
        assert not execution.workflow_id
        assert state.graph_execution is execution

    def test_graph_configuration_rejects_different_graph(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        mock_graph = MagicMock()

        state.attach_graph(mock_graph)
        state.attach_graph(mock_graph)

        other_graph = MagicMock()
        with pytest.raises(
            ValueError,
            match="GraphRuntimeState already attached to a different graph instance",
        ):
            state.attach_graph(other_graph)

    def test_read_only_wrapper_exposes_additional_state(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        wrapper = ReadOnlyGraphRuntimeStateWrapper(state)

        assert wrapper.ready_queue_size == 0
        assert wrapper.exceptions_count == 0

    def test_read_only_wrapper_serializes_runtime_state(self) -> None:
        state = GraphRuntimeState(
            variable_pool=VariablePool(),
            start_at=time(),
            llm_usage=LLMUsage.from_metadata({"total_tokens": 5}),
        )
        state.set_output("result", {"success": True})
        state.ready_queue.put(StartTask(frame_id="root", node_id="node-1"))

        wrapper = ReadOnlyGraphRuntimeStateWrapper(state)

        wrapper_snapshot = json.loads(wrapper.dumps())
        state_snapshot = json.loads(state.dumps())

        assert wrapper_snapshot == state_snapshot

    def test_dumps_and_loads_roundtrip(self) -> None:
        variable_pool = VariablePool()
        variable_pool.add(("node1", "value"), "payload")

        usage = LLMUsage.from_metadata({
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "total_tokens": 5,
            "total_price": "1.23",
            "currency": "USD",
            "latency": 0.5,
        })
        state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=time(),
            node_run_steps=3,
            llm_usage=usage,
        )
        state.set_output("final", {"result": True})
        state.ready_queue.put(StartTask(frame_id="root", node_id="node-A"))

        graph_execution = state.graph_execution
        graph_execution.workflow_id = "wf-123"
        graph_execution.exceptions_count = 4
        graph_execution.started = True
        graph_execution.error = ValueError("saved failure")

        snapshot = state.dumps()

        restored = GraphRuntimeState.from_snapshot(snapshot)

        assert restored.total_tokens == 5
        assert restored.node_run_steps == 3
        assert restored.get_output("final") == {"result": True}
        assert restored.llm_usage.total_tokens == usage.total_tokens
        assert restored.ready_queue.qsize() == 1
        assert restored.ready_queue.get(timeout=0.01) == StartTask(
            frame_id="root",
            node_id="node-A",
        )

        restored_segment = restored.variable_pool.get(("node1", "value"))
        assert restored_segment is not None
        assert restored_segment.value == "payload"

        restored_execution = restored.graph_execution
        assert restored_execution.workflow_id == "wf-123"
        assert restored_execution.exceptions_count == 4
        assert restored_execution.started is True
        assert isinstance(restored_execution.error, RuntimeError)
        assert str(restored_execution.error) == "saved failure"

    def test_snapshot_restore_preserves_updated_conversation_variable(self) -> None:
        variable_pool = VariablePool.from_bootstrap(
            conversation_variables=[
                StringVariable(name="session_name", value="before"),
            ],
        )
        variable_pool.add((CONVERSATION_VARIABLE_NODE_ID, "session_name"), "after")

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot = state.dumps()
        restored = GraphRuntimeState.from_snapshot(snapshot)

        restored_value = restored.variable_pool.get((
            CONVERSATION_VARIABLE_NODE_ID,
            "session_name",
        ))
        assert restored_value is not None
        assert restored_value.value == "after"

    def test_snapshot_restore_preserves_file_segments(self) -> None:
        variable_pool = VariablePool()
        file_value = File(
            file_id="file-1",
            file_type=FileType.DOCUMENT,
            transfer_method=FileTransferMethod.REMOTE_URL,
            remote_url="https://example.com/resume.pdf",
            filename="resume.pdf",
            extension=".pdf",
            mime_type="application/pdf",
            size=128,
        )
        variable_pool.add(("node", "attachment"), FileSegment(value=file_value))
        variable_pool.add(("node", "attachments"), ArrayFileSegment(value=[file_value]))

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())

        restored = GraphRuntimeState.from_snapshot(state.dumps())

        restored_file = restored.variable_pool.get(("node", "attachment"))
        restored_files = restored.variable_pool.get(("node", "attachments"))
        assert isinstance(restored_file, FileSegment)
        assert restored_file.value.filename == "resume.pdf"
        assert isinstance(restored_files, ArrayFileSegment)
        assert restored_files.value[0].filename == "resume.pdf"
