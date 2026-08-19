import json
from datetime import UTC, datetime
from time import time
from unittest.mock import MagicMock

import pytest

from graphon.engine.ready_queue.entities import (
    ReadyTask,
    ResumeTask,
    StartTask,
)
from graphon.engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.enums import (
    BuiltinNodeTypes,
    ErrorHandleMode,
    NodeExecutionType,
    NodeState,
)
from graphon.file import File, FileTransferMethod, FileType
from graphon.graph.graph import Graph
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
from graphon.runtime.execution import ROOT_FRAME_ID, GraphExecution
from graphon.runtime.graph_runtime_state import RuntimeState
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


def test_graph_execution_supplies_existing_workflow_identity() -> None:
    """A child runtime preserves the aggregate shared by its parent frame."""
    execution = GraphExecution(workflow_id="workflow")

    state = RuntimeState(
        variable_pool=VariablePool(),
        start_at=time(),
        graph_execution=execution,
    )

    assert state.graph_execution is execution


def test_runtime_state_requires_workflow_identity() -> None:
    """Construction cannot silently create an anonymous execution aggregate."""
    with pytest.raises(
        ValueError,
        match="workflow_id or graph_execution is required",
    ):
        RuntimeState(variable_pool=VariablePool(), start_at=time())


def test_runtime_state_rejects_conflicting_workflow_identity() -> None:
    """Two explicit identity sources must name the same workflow."""
    execution = GraphExecution(workflow_id="existing-workflow")

    with pytest.raises(
        ValueError,
        match=r"workflow_id must match graph_execution\.workflow_id",
    ):
        RuntimeState(
            workflow_id="different-workflow",
            graph_execution=execution,
            variable_pool=VariablePool(),
            start_at=time(),
        )


@pytest.mark.parametrize(
    ("node_states", "edge_states"),
    [
        ({}, {"root-edge": NodeState.TAKEN}),
        ({"root": NodeState.TAKEN}, {}),
    ],
    ids=["missing-node", "missing-edge"],
)
def test_attach_graph_rejects_saved_state_missing_current_graph_entries(
    node_states: dict[str, NodeState],
    edge_states: dict[str, NodeState],
) -> None:
    """Verify snapshots must contain every entry in the current graph scope.

    The two parameter cases omit either the current node or the current edge so
    the subset validation cannot accidentally weaken the existing completeness
    requirement while permitting unrelated saved entries.
    """
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
    )
    state.restore_graph_state(
        node_states=node_states,
        edge_states=edge_states,
    )
    graph = MagicMock(
        nodes={"root": MagicMock()},
        edges={"root-edge": MagicMock()},
    )

    with pytest.raises(
        RuntimeError,
        match="Saved graph state does not match rebuilt graph",
    ):
        state.attach_graph(graph)


class TestGraphRuntimeState:  # ruff:ignore[too-many-public-methods]
    def test_execution_context_defaults_to_empty_context(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

        with state.execution_context:
            assert state.execution_context is not None

    def test_property_getters(self) -> None:
        variable_pool = VariablePool()
        start_time = time()

        state = RuntimeState(
            workflow_id="workflow", variable_pool=variable_pool, start_at=start_time
        )

        assert state.variable_pool == variable_pool
        assert state.start_at == start_time
        assert state.total_tokens == 0
        assert state.node_run_steps == 0

    def test_outputs_immutability(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

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
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

        state.merge_response_outputs({"answer": "Hello", "status": "draft"})
        state.merge_response_outputs({"answer": " world", "status": "final"})

        assert state.get_output("answer") == "Hello world"
        assert state.get_output("status") == "final"

    def test_llm_usage_immutability(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

        usage1 = state.llm_usage
        usage2 = state.llm_usage
        assert usage1 is not usage2

    def test_type_validation(self) -> None:
        with pytest.raises(ValueError, match="node_run_steps must be non-negative"):
            RuntimeState(
                workflow_id="workflow",
                variable_pool=VariablePool(),
                start_at=time(),
                node_run_steps=-1,
            )

    def test_helper_methods(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

        initial_steps = state.node_run_steps
        state.increment_node_run_steps()
        assert state.node_run_steps == initial_steps + 1

        initial_tokens = state.total_tokens
        state.add_llm_usage(LLMUsage.from_metadata({"total_tokens": 50}))
        assert state.total_tokens == initial_tokens + 50
        assert state.llm_usage.total_tokens == 50

    def test_ready_queue_default_instantiation(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )

        queue = state.ready_queue

        assert isinstance(queue, InMemoryReadyQueue)

    def test_deferred_ready_tasks_round_trip_in_runtime_snapshot(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        first = StartTask(frame_id="root", node_id="a")
        second = StartTask(frame_id="child", node_id="b")
        state.defer_ready_task(first)
        state.defer_ready_task(second)

        restored = RuntimeState.from_snapshot(state.dumps())

        assert restored.drain_deferred_ready_tasks() == [first, second]
        assert restored.drain_deferred_ready_tasks() == []

    def test_custom_ready_queues_round_trip_with_supplied_factory(self) -> None:
        ready_queue: ReadyQueue = _PrefixedReadyQueue()
        deferred_ready_queue: ReadyQueue = _PrefixedReadyQueue()
        state = RuntimeState(
            workflow_id="workflow",
            variable_pool=VariablePool(),
            start_at=time(),
            ready_queue=ready_queue,
            deferred_ready_queue=deferred_ready_queue,
        )
        live_task = StartTask(frame_id="root", node_id="live")
        deferred_task = StartTask(frame_id="root", node_id="deferred")
        state.ready_queue.put(live_task)
        state.defer_ready_task(deferred_task)

        restored = RuntimeState.from_snapshot(
            state.dumps(),
            ready_queue_factory=_PrefixedReadyQueue,
        )

        assert isinstance(restored.ready_queue, _PrefixedReadyQueue)
        assert restored.ready_queue.drain() == [live_task]
        assert restored.drain_deferred_ready_tasks() == [deferred_task]

    def test_container_runtime_state_preserves_file_values(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
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

        restored = RuntimeState.from_snapshot(state.dumps())

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

    def test_workflow_id_creates_graph_execution(self) -> None:
        """A root runtime creates its execution aggregate from its workflow ID."""
        state = RuntimeState(
            workflow_id="workflow",
            variable_pool=VariablePool(),
            start_at=time(),
        )

        execution = state.graph_execution

        assert isinstance(execution, GraphExecution)
        assert execution.workflow_id == "workflow"
        assert execution.execution_id
        assert state.graph_execution is execution

    def test_graph_configuration_rejects_different_graph(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        mock_graph = MagicMock()

        state.attach_graph(mock_graph)
        state.attach_graph(mock_graph)

        other_graph = MagicMock()
        with pytest.raises(
            ValueError,
            match="RuntimeState already attached to a different graph instance",
        ):
            state.attach_graph(other_graph)

    def test_attach_graph_rejects_current_state_from_a_larger_scope(self) -> None:
        """Version 3 state must describe exactly the graph being attached."""
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        state.restore_graph_state(
            node_states={
                "root": NodeState.TAKEN,
                "child": NodeState.SKIPPED,
            },
            edge_states={
                "root-edge": NodeState.TAKEN,
                "child-edge": NodeState.SKIPPED,
            },
        )
        root_node = MagicMock(state=NodeState.UNKNOWN)
        root_edge = MagicMock(state=NodeState.UNKNOWN)
        graph = MagicMock(
            nodes={"root": root_node},
            edges={"root-edge": root_edge},
        )

        with pytest.raises(
            RuntimeError,
            match="Saved graph state does not match rebuilt graph",
        ):
            state.attach_graph(graph)

    def test_version_2_state_migrates_only_after_graph_attachment(self) -> None:
        """Legacy positional IDs are converted without relaxing version 3 reads.

        Root and child scopes intentionally reuse the same public edge ID. The
        version 2 state is a full-graph map keyed by global positional IDs; graph
        attachment must select only the root entry before writing version 3.
        """
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        payload = json.loads(state.dumps())
        payload.update({
            "version": "2.0",
            "graph_node_states": {
                "start": NodeState.TAKEN,
                "end": NodeState.UNKNOWN,
                "child-start": NodeState.TAKEN,
                "child-end": NodeState.UNKNOWN,
            },
            "graph_edge_states": {
                "edge_1": NodeState.TAKEN,
                "edge_2": NodeState.SKIPPED,
            },
        })
        restored = RuntimeState.from_snapshot(json.dumps(payload))

        with pytest.raises(
            RuntimeError,
            match="must attach its graph before serialization",
        ):
            restored.dumps()

        start = MagicMock(state=NodeState.UNKNOWN)
        end = MagicMock(state=NodeState.UNKNOWN)
        root_edge = MagicMock(state=NodeState.UNKNOWN)
        graph = MagicMock(
            nodes={"start": start, "end": end},
            edges={"shared-edge": root_edge},
            graph_config={
                "nodes": [
                    {"id": "start", "data": {}},
                    {"id": "end", "data": {}},
                    {"id": "child-start", "data": {"container_id": "container"}},
                    {"id": "child-end", "data": {"container_id": "container"}},
                ],
                "edges": [
                    {
                        "id": "ignored-edge",
                        "source": "start",
                        "target": "end",
                        "sourceHandle": 1,
                    },
                    {"id": "shared-edge", "source": "start", "target": "end"},
                    {
                        "id": "shared-edge",
                        "source": "child-start",
                        "target": "child-end",
                    },
                ],
            },
        )

        restored.attach_graph(graph)
        migrated = json.loads(restored.dumps())

        assert start.state is NodeState.TAKEN
        assert end.state is NodeState.UNKNOWN
        assert root_edge.state is NodeState.TAKEN
        assert migrated["version"] == "3.0"
        assert migrated["graph_edge_states"] == {"shared-edge": NodeState.TAKEN}
        assert "compatibility_marker" not in migrated

    def test_version_2_graph_builder_state_migrates_without_graph_config(self) -> None:
        """Keep legacy generated IDs when a programmatic graph matches exactly."""
        root = MagicMock(
            id="start",
            node_type=BuiltinNodeTypes.START,
            execution_type=NodeExecutionType.ROOT,
            state=NodeState.TAKEN,
        )
        end = MagicMock(
            id="end",
            node_type=BuiltinNodeTypes.END,
            execution_type=NodeExecutionType.EXECUTABLE,
            state=NodeState.UNKNOWN,
        )
        graph = Graph.new().add_root(root).add_node(end).build()
        assert graph.graph_config is None

        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        state.attach_graph(graph)
        payload = json.loads(state.dumps())
        payload["version"] = "2.0"

        restored = RuntimeState.from_snapshot(json.dumps(payload))
        restored.attach_graph(graph)

        assert json.loads(restored.dumps())["version"] == "3.0"
        assert root.state is NodeState.TAKEN
        assert end.state is NodeState.UNKNOWN

    def test_failed_version_2_migration_does_not_commit_compatibility_state(
        self,
    ) -> None:
        """Leave legacy state retryable when the rebuilt root graph does not match."""
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        payload = json.loads(state.dumps())
        payload.update({
            "version": "2.0",
            "graph_node_states": {"start": NodeState.TAKEN},
            "graph_edge_states": {"edge_0": NodeState.SKIPPED},
        })
        restored = RuntimeState.from_snapshot(json.dumps(payload))
        graph_config = {
            "nodes": [{"id": "start", "data": {}}],
            "edges": [{"id": "public-edge", "source": "start", "target": "start"}],
        }
        mismatched_graph = MagicMock(
            nodes={"other": MagicMock(state=NodeState.UNKNOWN)},
            edges={"public-edge": MagicMock(state=NodeState.UNKNOWN)},
            graph_config=graph_config,
        )

        with pytest.raises(
            RuntimeError,
            match="Saved graph state does not match rebuilt graph",
        ):
            restored.attach_graph(mismatched_graph)
        with pytest.raises(
            RuntimeError,
            match="must attach its graph before serialization",
        ):
            restored.dumps()

        matching_node = MagicMock(state=NodeState.UNKNOWN)
        matching_edge = MagicMock(state=NodeState.UNKNOWN)
        restored.attach_graph(
            MagicMock(
                nodes={"start": matching_node},
                edges={"public-edge": matching_edge},
                graph_config=graph_config,
            )
        )
        assert matching_node.state is NodeState.TAKEN
        assert matching_edge.state is NodeState.SKIPPED

    def test_read_only_wrapper_exposes_additional_state(self) -> None:
        state = RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=time()
        )
        wrapper = ReadOnlyGraphRuntimeStateWrapper(state)

        assert wrapper.ready_queue_size == 0
        assert wrapper.exceptions_count == 0

    def test_read_only_wrapper_serializes_runtime_state(self) -> None:
        state = RuntimeState(
            workflow_id="workflow",
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
        state = RuntimeState(
            workflow_id="wf-123",
            variable_pool=variable_pool,
            start_at=time(),
            node_run_steps=3,
            llm_usage=usage,
        )
        state.set_output("final", {"result": True})
        state.ready_queue.put(StartTask(frame_id="root", node_id="node-A"))

        graph_execution = state.graph_execution
        graph_execution.execution_id = "execution-123"
        assert graph_execution.next_event_sequence() == 1
        assert graph_execution.next_event_sequence() == 2
        graph_execution.exceptions_count = 4
        graph_execution.started = True
        graph_execution.error = ValueError("saved failure")

        snapshot = state.dumps()

        restored = RuntimeState.from_snapshot(snapshot)

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
        assert restored_execution.execution_id == "execution-123"
        assert restored_execution.last_event_sequence == 2
        assert restored_execution.next_event_sequence() == 3
        assert restored_execution.exceptions_count == 4
        assert restored_execution.started is True
        assert isinstance(restored_execution.error, RuntimeError)
        assert str(restored_execution.error) == "saved failure"

    def test_version_1_snapshot_migrates_to_current_frame_state(self) -> None:
        variable_pool = VariablePool()
        variable_pool.add(("legacy", "value"), "preserved")
        usage = LLMUsage.from_metadata({"total_tokens": 5})
        snapshot = json.dumps({
            "version": "1.0",
            "start_at": 1.0,
            "total_tokens": 5,
            "node_run_steps": 3,
            "llm_usage": usage.model_dump(mode="json"),
            "outputs": {"answer": "legacy"},
            "variable_pool": variable_pool.model_dump(mode="json"),
            "ready_queue": json.dumps({
                "type": "InMemoryReadyQueue",
                "version": "1.0",
                "items": ["ready"],
            }),
            "graph_execution": json.dumps({
                "type": "GraphExecution",
                "version": "1.0",
                "workflow_id": "workflow",
                "started": True,
                "completed": False,
                "aborted": False,
                "paused": True,
                "pause_reasons": [],
                "error": None,
                "exceptions_count": 1,
                "node_executions": [
                    {
                        "node_id": "ready",
                        "state": NodeState.TAKEN,
                        "retry_count": 2,
                        "execution_id": None,
                        "error": None,
                    },
                ],
            }),
            "paused_nodes": ["paused"],
            "deferred_nodes": ["deferred", "paused"],
            "graph_state": {
                "nodes": {"ready": NodeState.TAKEN},
                "edges": {"edge_0": NodeState.SKIPPED},
            },
        })

        restored = RuntimeState.from_snapshot(snapshot)
        ready_node = MagicMock(state=NodeState.UNKNOWN)
        approved_edge = MagicMock(state=NodeState.UNKNOWN)
        restored.attach_graph(
            MagicMock(
                nodes={"ready": ready_node},
                edges={"approved-edge": approved_edge},
                graph_config={
                    "nodes": [{"id": "ready", "data": {}}],
                    "edges": [
                        {
                            "id": "approved-edge",
                            "source": "ready",
                            "target": "ready",
                        },
                    ],
                },
            ),
        )
        migrated = json.loads(restored.dumps())

        assert migrated["version"] == "3.0"
        assert json.loads(migrated["ready_queue"])["version"] == "2.0"
        assert json.loads(migrated["deferred_ready_tasks"])["version"] == "2.0"
        assert json.loads(migrated["graph_execution"])["version"] == "2.0"
        assert restored.graph_execution.execution_id
        assert restored.graph_execution.last_event_sequence == 0
        assert migrated["graph_node_states"] == {"ready": NodeState.TAKEN}
        assert migrated["graph_edge_states"] == {
            "approved-edge": NodeState.SKIPPED,
        }
        assert ready_node.state is NodeState.TAKEN
        assert approved_edge.state is NodeState.SKIPPED
        assert restored.ready_queue.drain() == [
            StartTask(frame_id=ROOT_FRAME_ID, node_id="ready"),
        ]
        assert restored.drain_deferred_ready_tasks() == [
            StartTask(frame_id=ROOT_FRAME_ID, node_id="paused"),
            StartTask(frame_id=ROOT_FRAME_ID, node_id="deferred"),
        ]
        node_execution = restored.graph_execution.get_or_create_node_execution(
            frame_id=ROOT_FRAME_ID,
            node_id="ready",
        )
        assert node_execution.retry_count == 2
        assert node_execution.execution_id
        assert restored.total_tokens == 5
        assert restored.node_run_steps == 3
        assert restored.get_output("answer") == "legacy"
        value = restored.variable_pool.get(("legacy", "value"))
        assert value is not None
        assert value.value == "preserved"

    def test_snapshot_restore_preserves_updated_conversation_variable(self) -> None:
        variable_pool = VariablePool.from_bootstrap(
            conversation_variables=[
                StringVariable(name="session_name", value="before"),
            ],
        )
        variable_pool.add((CONVERSATION_VARIABLE_NODE_ID, "session_name"), "after")

        state = RuntimeState(
            workflow_id="workflow", variable_pool=variable_pool, start_at=time()
        )
        snapshot = state.dumps()
        restored = RuntimeState.from_snapshot(snapshot)

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

        state = RuntimeState(
            workflow_id="workflow", variable_pool=variable_pool, start_at=time()
        )

        restored = RuntimeState.from_snapshot(state.dumps())

        restored_file = restored.variable_pool.get(("node", "attachment"))
        restored_files = restored.variable_pool.get(("node", "attachments"))
        assert isinstance(restored_file, FileSegment)
        assert restored_file.value.filename == "resume.pdf"
        assert isinstance(restored_files, ArrayFileSegment)
        assert restored_files.value[0].filename == "resume.pdf"


def test_version_2_graph_execution_without_sequence_defaults_to_zero() -> None:
    execution = GraphExecution.from_snapshot(
        json.dumps({
            "version": "2.0",
            "workflow_id": "workflow",
            "execution_id": "execution",
            "started": True,
            "completed": False,
            "aborted": False,
            "paused": True,
            "pause_reasons": [],
            "error": None,
            "exceptions_count": 0,
            "node_executions": [],
        })
    )

    assert execution.execution_id == "execution"
    assert execution.last_event_sequence == 0
    assert execution.next_event_sequence() == 1
