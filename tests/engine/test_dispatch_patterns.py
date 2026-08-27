import json
import queue
import threading
from collections.abc import Generator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock, call

import pytest

from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine.command import (
    AbortCommand,
    CommandProcessor,
    PauseCommand,
    UpdateVariablesCommand,
)
from graphon.engine.command.builtin.in_memory import InMemoryChannel
from graphon.engine.command.builtin.redis import RedisChannel
from graphon.engine.container_handler import (
    ContainerHandler,
    IterationContainerHandler,
    LoopContainerHandler,
)
from graphon.engine.dispatcher import Dispatcher
from graphon.engine.event.processor import NodeEventProcessor
from graphon.engine.event.stream import EventStream
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine.layer import ExecutionLimitsLayer, Layer
from graphon.engine.ready_queue.entities import (
    ReadyTask,
    ResumeTask,
    StartTask,
)
from graphon.engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.engine.scheduler import Scheduler
from graphon.engine.worker import (
    DispatchTask,
    NodeEventTask,
    Worker,
    WorkerPool,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPausedEvent,
)
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunPauseRequestedEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.engine_events.traversal import (
    GraphEdgeSkippedEvent,
    GraphEdgeTakenEvent,
)
from graphon.entities.pause_reason import HitlRequired, SchedulingPause
from graphon.enums import (
    BuiltinNodeTypes,
    ErrorHandleMode,
    ErrorStrategy,
    NodeExecutionType,
    NodeState,
    NodeType,
)
from graphon.graph.graph import Graph
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.base import NodeRunResult
from graphon.nodes.container_effects import (
    ContainerExecutionResult,
    ContainerNodeRunResult,
    IterationFrameRequest,
    build_container_value,
)
from graphon.nodes.human_input.entities import PauseRequested
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.runtime.container_state import (
    FrameRuntimeData,
    IterationFrameState,
    IterationRunState,
    create_container_run_state,
)
from graphon.runtime.execution import GraphExecution
from graphon.runtime.init_params import InitParams
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import StringVariable


def _variable_value(
    runtime_state: RuntimeState,
    selector: list[str],
) -> object:
    variable = runtime_state.variable_pool.get(selector)
    assert variable is not None
    return variable.to_object()


def _execution_frame(
    *,
    frame_id: str,
    graph: Graph,
    runtime_state: object | None = None,
    scheduler: object | None = None,
    failure_handler: object | None = None,
    container_id: str = "",
) -> ExecutionFrame:
    if isinstance(runtime_state, MagicMock):
        runtime_state.has_container_frame.return_value = False
    if scheduler is None:
        resolved_scheduler = MagicMock()
        resolved_scheduler.process_node_success.return_value = ([], [])
        resolved_scheduler.handle_branch_completion.return_value = ([], [])
    else:
        resolved_scheduler = scheduler
    return ExecutionFrame(
        frame_id=frame_id,
        graph=graph,
        state=cast(Any, runtime_state or MagicMock()),
        scheduler=cast(Any, resolved_scheduler),
        failure_handler=cast(Any, failure_handler or MagicMock()),
        container_id=container_id,
    )


def _event_processor(
    *,
    graph_execution: object,
    event_stream: object,
    frame_registry: FrameRegistry,
) -> NodeEventProcessor:
    container_handlers = _container_handlers(
        frame_registry=frame_registry,
    )
    return NodeEventProcessor(
        graph_execution=cast(Any, graph_execution),
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
        container_handlers=container_handlers,
    )


def _event_processor_with_container(
    *,
    graph_execution: object,
    event_stream: object,
    frame_registry: FrameRegistry,
) -> tuple[NodeEventProcessor, dict[str, ContainerHandler]]:
    container_handlers = _container_handlers(
        frame_registry=frame_registry,
    )
    return (
        NodeEventProcessor(
            graph_execution=cast(Any, graph_execution),
            event_stream=cast(EventStream, event_stream),
            frame_registry=frame_registry,
            container_handlers=container_handlers,
        ),
        container_handlers,
    )


def _container_handlers(
    *,
    frame_registry: FrameRegistry,
) -> dict[str, ContainerHandler]:
    return {
        "loop": LoopContainerHandler(
            frame_registry=frame_registry,
        ),
        "iteration": IterationContainerHandler(
            frame_registry=frame_registry,
        ),
    }


def _load_minimal_engine(
    *,
    command_channel: InMemoryChannel | None = None,
) -> Engine:
    """Create a fully initialized one-node engine for lifecycle regressions.

    The engine is loaded through the same public DSL entry point used by callers,
    so its graph, runtime state, scheduler, dispatcher, and workers have their
    normal relationships. Tests may optionally supply an in-memory command channel
    to exercise pause and abort behavior without reaching into engine internals.

    Args:
        command_channel: Command source to bind to the engine, or ``None`` to use
            the engine's default in-memory channel.

    Returns:
        A runnable engine whose only node is a valid workflow start node.

    """
    return loads(
        json.dumps({
            "kind": "graph",
            "graph": {
                "nodes": [
                    {
                        "id": "start",
                        "data": {"type": "start", "variables": []},
                    }
                ],
                "edges": [],
            },
        }),
        command_channel=command_channel,
        workers=1,
    )


def _get_resume_task(ready_queue: InMemoryReadyQueue) -> ResumeTask:
    task = ready_queue.get(timeout=0.01)
    assert isinstance(task, ResumeTask)
    return task


def _start_iteration_await(
    container_handler: ContainerHandler,
    runtime_state: RuntimeState,
    *,
    invocation_id: str,
    indexes: tuple[int, ...],
    items: tuple[object, ...],
    error_handle_mode: ErrorHandleMode,
    flatten_output: bool,
    parallel_nums: int,
) -> None:
    request = IterationFrameRequest(
        items=tuple(build_container_value(item) for item in items),
        root_node_id="iteration-start",
        indexes=indexes,
        output_selector=("answer", "text"),
        error_handle_mode=error_handle_mode,
        flatten_output=flatten_output,
        parallel_nums=parallel_nums,
    )
    runtime_state.put_container_run(
        create_container_run_state(
            invocation_id=invocation_id,
            frame_id="root",
            node_id="iteration",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            request=request,
        ),
    )
    container_handler.handle_request(
        invocation_id=invocation_id,
        request=request,
    )


def _worker(
    *,
    ready_queue: InMemoryReadyQueue,
    dispatch_queue: queue.Queue[DispatchTask],
    frame_registry: FrameRegistry,
) -> Worker:
    task_claiming = threading.Event()
    task_claiming.set()
    return Worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[],
        task_claim_lock=threading.Lock(),
        task_claiming=task_claiming,
    )


@dataclass
class _FrameNode:
    id: str
    node_type: BuiltinNodeTypes

    execution_type: ClassVar[NodeExecutionType] = NodeExecutionType.EXECUTABLE
    error_strategy: ClassVar[None] = None
    retry: ClassVar[bool] = False
    state: ClassVar[NodeState] = NodeState.UNKNOWN


class _FrameFactory:
    def with_graph_config(
        self,
        graph_config: Mapping[str, Any],
    ) -> "_FrameFactory":
        """Accept graph scoping for this stateless frame test factory."""
        _ = graph_config
        return self

    def validate_node(
        self,
        node_config: dict[str, object],
    ) -> NodeExecutionType:
        """Accept minimal frame configs and report their execution semantics."""
        _ = node_config
        return _FrameNode.execution_type

    def with_runtime_state(
        self,
        runtime_state: RuntimeState,
    ) -> "_FrameFactory":
        _ = runtime_state
        return self

    def create_node(self, node_config: dict[str, object]) -> _FrameNode:
        node_data = cast(dict[str, object], node_config["data"])
        return _FrameNode(
            str(node_config["id"]),
            cast(BuiltinNodeTypes, node_data["type"]),
        )


@pytest.mark.parametrize(
    ("payload", "expected_command_type"),
    [
        (
            {"command_type": "abort", "reason": "stop"},
            AbortCommand,
        ),
        (
            {"command_type": "pause", "reason": "wait"},
            PauseCommand,
        ),
        (
            {"command_type": "update_variables", "updates": []},
            UpdateVariablesCommand,
        ),
    ],
)
def test_redis_channel_deserializes_discriminated_command(
    payload: dict[str, object],
    expected_command_type: type,
) -> None:
    channel = RedisChannel(redis_client=MagicMock(), channel_key="test-channel")

    command = channel.deserialize_command(payload)

    assert isinstance(command, expected_command_type)


def test_redis_channel_fetches_command_list_without_pending_marker() -> None:
    """Verify one Redis transaction both reads and clears the command list.

    Fetching must not consult a second marker key: the list transaction is the
    complete source of truth, and its three pipeline calls make that contract
    observable without depending on a concrete Redis client implementation.
    """
    redis_client = MagicMock()
    pipeline = redis_client.pipeline.return_value.__enter__.return_value
    pipeline.execute.return_value = [
        [AbortCommand(reason="stop").model_dump_json()],
        1,
    ]
    channel = RedisChannel(redis_client=redis_client, channel_key="test-channel")

    commands = channel.fetch_commands()

    assert commands == [AbortCommand(reason="stop")]
    assert pipeline.method_calls == [
        call.lrange("test-channel", 0, -1),
        call.delete("test-channel"),
        call.execute(),
    ]


def test_redis_channel_skips_non_object_json_and_continues_batch() -> None:
    """Reject one malformed command without discarding later valid commands.

    Redis returns the entire drained list as one batch. A syntactically valid JSON
    value can still be an invalid command shape, so deserialization must isolate the
    bad item and continue processing the remaining entries.
    """
    redis_client = MagicMock()
    pipeline = redis_client.pipeline.return_value.__enter__.return_value
    valid_command = AbortCommand(reason="stop")
    pipeline.execute.return_value = [
        ["[]", valid_command.model_dump_json()],
        1,
    ]
    channel = RedisChannel(redis_client=redis_client, channel_key="test-channel")

    assert channel.fetch_commands() == [valid_command]


def test_redis_channel_migrates_wrapped_variable_updates() -> None:
    """Accept the previous Redis payload during its bounded one-hour lifetime.

    Compatibility belongs only at the transport boundary: decoded commands
    expose variables directly without restoring the removed wrapper type.
    """
    variable = StringVariable(
        name="answer",
        selector=["node", "answer"],
        value="updated",
    )
    channel = RedisChannel(redis_client=MagicMock(), channel_key="test-channel")

    command = channel.deserialize_command({
        "command_type": "update_variables",
        "updates": [{"value": variable.model_dump(mode="json")}],
    })

    assert isinstance(command, UpdateVariablesCommand)
    assert list(command.updates) == [variable]


def test_redis_channel_writes_legacy_marker_and_variable_update_payload() -> None:
    """Keep commands readable by old consumers throughout a rolling deployment.

    The legacy consumer ignores the list unless its pending marker exists and
    validates variable updates only when each variable is nested below ``value``.
    The exact pipeline calls and round-trip through the new consumer protect both
    sides of that compatibility contract.
    """
    redis_client = MagicMock()
    pipeline = redis_client.pipeline.return_value.__enter__.return_value
    variable = StringVariable(
        name="answer",
        selector=["node", "answer"],
        value="updated",
    )
    command = UpdateVariablesCommand(updates=[variable])
    channel = RedisChannel(
        redis_client=redis_client,
        channel_key="test-channel",
        command_ttl=60,
    )

    channel.send_command(command)

    command_json = pipeline.rpush.call_args.args[1]
    payload = json.loads(command_json)
    assert payload == {
        "command_type": "update_variables",
        "updates": [{"value": variable.model_dump(mode="json")}],
    }
    assert channel.deserialize_command(payload) == command
    assert pipeline.method_calls == [
        call.rpush("test-channel", command_json),
        call.expire("test-channel", 60),
        call.set("test-channel:pending", "1", ex=60),
        call.execute(),
    ]


def test_command_processor_directly_handles_builtin_commands() -> None:
    """Verify direct command matching preserves every built-in command behavior.

    The processor must skip an invalid variable update without dropping the valid
    update that follows it, propagate inherited variables without injecting them
    into unrelated frames, then apply pause and abort commands from the same batch.
    This is the behavior previously split across handler classes.
    """
    channel = InMemoryChannel()
    execution = GraphExecution(workflow_id="workflow")
    variable_pool = VariablePool()
    nested_variable_pool = VariablePool()
    nested_variable_pool.add(("node", "answer"), "stale")
    isolated_variable_pool = VariablePool()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, MagicMock()),
            runtime_state=SimpleNamespace(variable_pool=variable_pool),
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="nested",
            graph=cast(Graph, MagicMock()),
            runtime_state=SimpleNamespace(variable_pool=nested_variable_pool),
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="isolated",
            graph=cast(Graph, MagicMock()),
            runtime_state=SimpleNamespace(variable_pool=isolated_variable_pool),
        ),
    )
    channel.send_command(
        UpdateVariablesCommand(
            updates=[
                StringVariable(
                    name="invalid",
                    selector=["invalid"],
                    value="ignored",
                ),
                StringVariable(
                    name="answer",
                    selector=["node", "answer"],
                    value="updated",
                ),
            ],
        ),
    )
    channel.send_command(PauseCommand(reason="wait"))
    channel.send_command(AbortCommand(reason="stop"))

    CommandProcessor(
        command_channel=channel,
        graph_execution=execution,
        frame_registry=frame_registry,
    ).process_commands()

    updated = variable_pool.get(["node", "answer"])
    assert updated is not None
    assert updated.to_object() == "updated"
    nested_updated = nested_variable_pool.get(["node", "answer"])
    assert nested_updated is not None
    assert nested_updated.to_object() == "updated"
    assert nested_updated is not updated
    assert isolated_variable_pool.get(["node", "answer"]) is None
    assert execution.paused
    pause_reason = execution.pause_reasons[0]
    assert isinstance(pause_reason, SchedulingPause)
    assert pause_reason.message == "wait"
    assert execution.aborted
    assert str(execution.error) == "Aborted: stop"


@pytest.mark.parametrize("workers", [0, True, 1.5])
def test_engine_rejects_invalid_workers_before_mutating_runtime_state(
    workers: Any,
) -> None:
    """Reject invalid worker counts before attaching the caller's runtime state.

    Booleans and floats can compare greater than zero in Python but are not valid
    thread counts. The public constructor must enforce the complete WorkerPool
    invariant before mutating the supplied state.
    """
    runtime_state = MagicMock()

    with pytest.raises(ValueError, match="workers must be a positive integer"):
        Engine(
            graph=MagicMock(),
            runtime_state=runtime_state,
            workers=workers,
        )

    runtime_state.attach_graph.assert_not_called()


def test_worker_pool_rejects_zero_workers() -> None:
    """Reject a directly constructed pool that could never claim queued work.

    Direct callers bypass ``Engine`` validation, so the pool must enforce
    the same positive-worker invariant before retaining any collaborators.
    """
    with pytest.raises(ValueError, match="workers must be a positive integer"):
        WorkerPool(
            ready_queue=MagicMock(),
            dispatch_queue=MagicMock(),
            frame_registry=MagicMock(),
            layers=[],
            workers=0,
        )


def test_scheduler_enqueues_ready_task_for_frame() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    graph = SimpleNamespace(
        nodes={"start": SimpleNamespace(state=NodeState.UNKNOWN)},
    )
    scheduler = Scheduler(
        graph=cast(Graph, graph),
        state=runtime_state,
        frame_id="root",
    )

    scheduler.enqueue_node("start")

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="start",
    )
    assert graph.nodes["start"].state == NodeState.TAKEN
    assert not scheduler.is_execution_complete()


def test_scheduler_defers_ready_task_when_paused() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow", paused=True)
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(
        nodes={"start": SimpleNamespace(state=NodeState.UNKNOWN)},
    )
    scheduler = Scheduler(
        graph=cast(Graph, graph),
        state=runtime_state,
        frame_id="root",
    )

    scheduler.enqueue_node("start")

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="start"),
    ]
    assert graph.nodes["start"].state == NodeState.TAKEN
    assert not scheduler.is_execution_complete()


def test_scheduler_completion_ignores_other_frame_queue_items() -> None:
    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="other-frame", node_id="answer"))
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
    )
    graph = SimpleNamespace(nodes={})
    scheduler = Scheduler(
        graph=cast(Graph, graph),
        state=runtime_state,
        frame_id="root",
    )

    assert scheduler.is_execution_complete() is True


def test_pause_defers_queued_tasks_without_losing_frame_progress() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(
        nodes={
            "active": SimpleNamespace(state=NodeState.UNKNOWN),
            "queued": SimpleNamespace(state=NodeState.UNKNOWN),
        }
    )
    scheduler = Scheduler(
        graph=cast(Graph, graph),
        state=runtime_state,
        frame_id="root",
    )
    scheduler.enqueue_node("active")
    scheduler.enqueue_node("queued")
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="root",
        node_id="active",
    )
    graph_execution.paused = True
    worker_pool = MagicMock()
    worker_pool.drain.side_effect = ready_queue.drain
    dispatcher = Dispatcher(
        dispatch_queue=queue.Queue(),
        event_processor=MagicMock(),
        graph_execution=graph_execution,
        scheduler=scheduler,
        command_processor=MagicMock(),
        worker_pool=worker_pool,
        event_stream=MagicMock(),
    )

    assert dispatcher._run_until_exit()

    assert ready_queue.qsize() == 0
    assert runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="root", node_id="queued")
    ]
    assert not scheduler.is_execution_complete()
    scheduler.finish_execution("active")
    assert not scheduler.is_execution_complete()
    scheduler.finish_execution("queued")
    assert scheduler.is_execution_complete()
    worker_pool.drain.assert_called_once_with()
    worker_pool.stop.assert_not_called()


def test_worker_pool_drain_does_not_stop_worker_with_current_task() -> None:
    class WorkerStub:
        def __init__(self, *, has_current_task: bool) -> None:
            self.has_current_task = has_current_task
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    active_worker = WorkerStub(has_current_task=True)
    idle_worker = WorkerStub(has_current_task=False)
    pool = object.__new__(WorkerPool)
    pool._lock = cast(Any, threading.RLock())
    pool._task_claim_lock = threading.Lock()
    pool._task_claiming = threading.Event()
    pool._task_claiming.set()
    pool._ready_queue = InMemoryReadyQueue()
    pool._workers = cast(Any, [active_worker, idle_worker])

    pool.drain()

    assert active_worker.stopped is False
    assert idle_worker.stopped is True


def test_worker_pool_drain_observes_task_claimed_during_pause() -> None:  # ruff: ignore[complex-structure]
    class BlockingReadyQueue:
        def __init__(self) -> None:
            self._queue = InMemoryReadyQueue()
            self.task_removed = threading.Event()
            self.release_claim = threading.Event()

        def put(self, item: ReadyTask) -> None:
            self._queue.put(item)

        def get(self, timeout: float | None = None) -> ReadyTask:
            task = self._queue.get(timeout)
            self.task_removed.set()
            if not self.release_claim.wait(timeout=1):
                msg = "task claim was not released"
                raise TimeoutError(msg)
            return task

        def task_done(self) -> None:
            self._queue.task_done()

        def qsize(self) -> int:
            return self._queue.qsize()

        def drain(self) -> list[ReadyTask]:
            return self._queue.drain()

        def dumps(self) -> str:
            return self._queue.dumps()

        def loads(self, data: str) -> None:
            self._queue.loads(data)

    class BlockingNode:
        id = "node"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "pending"

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunSucceededEvent, None, None]:
            node_started.set()
            if not finish_node.wait(timeout=1):
                msg = "node was not released"
                raise TimeoutError(msg)
            now = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=now,
                finished_at=now,
                node_run_result=NodeRunResult(),
            )

    ready_queue = BlockingReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="node"))
    node_started = threading.Event()
    finish_node = threading.Event()
    dispatch_queue: queue.Queue[DispatchTask] = queue.Queue()
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=GraphExecution(workflow_id="workflow"),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, SimpleNamespace(nodes={"node": BlockingNode()})),
            runtime_state=runtime_state,
        ),
    )
    pool = WorkerPool(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[],
        workers=1,
    )
    drained_tasks: list[ReadyTask] = []
    drain_done = threading.Event()

    def drain_pool() -> None:
        drained_tasks.extend(pool.drain())
        drain_done.set()

    pool.start()
    drain_thread = threading.Thread(target=drain_pool)
    try:
        assert ready_queue.task_removed.wait(timeout=1)
        drain_thread.start()
        assert not drain_done.wait(timeout=0.05)
        ready_queue.release_claim.set()
        assert node_started.wait(timeout=1)
        assert drain_done.wait(timeout=1)
        assert drained_tasks == []
        assert pool.has_current_tasks()
    finally:
        ready_queue.release_claim.set()
        finish_node.set()
        drain_thread.join(timeout=1)
        pool.stop()


def test_worker_pool_runs_queued_siblings_with_fixed_workers() -> None:
    class ParallelNode:
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "pending"

        def __init__(self, node_id: str) -> None:
            self.id = node_id

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunSucceededEvent, None, None]:
            first_node_started.set()
            barrier.wait(timeout=1)
            now = datetime.now(UTC).replace(tzinfo=None)
            yield NodeRunSucceededEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                start_at=now,
                finished_at=now,
                node_run_result=NodeRunResult(),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="first"))
    ready_queue.put(StartTask(frame_id="root", node_id="second"))
    dispatch_queue: queue.Queue[DispatchTask] = queue.Queue()
    first_node_started = threading.Event()
    barrier = threading.Barrier(2)
    graph = SimpleNamespace(
        nodes={
            "first": ParallelNode("first"),
            "second": ParallelNode("second"),
        },
    )
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=GraphExecution(workflow_id="workflow"),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    pool = WorkerPool(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
        layers=[],
        workers=2,
    )

    pool.start()
    try:
        assert first_node_started.wait(timeout=1)
        first_event = dispatch_queue.get(timeout=1)
        second_event = dispatch_queue.get(timeout=1)
    finally:
        pool.stop()

    assert isinstance(first_event, NodeEventTask)
    assert isinstance(second_event, NodeEventTask)
    events = (first_event, second_event)
    assert {event.event.node_id for event in events} == {"first", "second"}
    assert all(isinstance(event.event, NodeRunSucceededEvent) for event in events)


def test_pause_requested_event_defers_current_task_for_resume() -> None:
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    child_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=0,
        ready_queue=ready_queue,
        deferred_ready_queue=root_runtime_state.deferred_ready_queue,
        graph_execution=graph_execution,
    )
    init_params = InitParams(
        workflow_id="workflow",
        graph_config={},
        run_context={},
        call_depth=0,
    )
    iteration_node = IterationNode(
        node_id="iteration",
        data=IterationNode.validate_node_data({
            "type": "iteration",
            "start_node_id": "human",
            "iterator_selector": ["start", "items"],
            "output_selector": ["human", "output"],
            "is_parallel": False,
            "parallel_nums": 1,
            "error_handle_mode": ErrorHandleMode.TERMINATED,
            "flatten_output": False,
        }),
        init_params=init_params,
        runtime_state=root_runtime_state,
    )
    root_graph = Graph(
        root_node=iteration_node,
        nodes={"iteration": iteration_node},
    )
    human_node = HumanInputNode(
        node_id="human",
        data=HumanInputNode.validate_node_data({
            "type": "human-input",
            "title": "Human",
        }),
        init_params=init_params,
        runtime_state=child_runtime_state,
        hitl_callback=lambda _context: PauseRequested(session_id="unused"),
    )
    child_graph = Graph(
        root_node=human_node,
        nodes={"human": human_node},
    )
    child_runtime_state.attach_graph(child_graph)
    request = IterationFrameRequest(
        items=(build_container_value("input"),),
        root_node_id="human",
        indexes=(0,),
        output_selector=("iteration", "item"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=1,
    )
    root_runtime_state.put_container_run(
        create_container_run_state(
            invocation_id="iteration-invocation",
            frame_id="root",
            node_id="iteration",
            started_at=datetime.now(UTC).replace(tzinfo=None),
            request=request,
        ),
    )
    root_runtime_state.put_container_frame(
        IterationFrameState(
            frame_id="child-frame",
            parent_invocation_id="iteration-invocation",
            root_node_id="human",
            index=0,
            started_at=datetime.now(UTC).replace(tzinfo=None),
            runtime_data=child_runtime_state.snapshot_frame(),
        )
    )
    scheduler = Scheduler(
        graph=child_graph,
        state=child_runtime_state,
        frame_id="child-frame",
    )
    frame_registry = FrameRegistry()
    event_stream = MagicMock()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            runtime_state=root_runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="child-frame",
            graph=child_graph,
            runtime_state=child_runtime_state,
            scheduler=scheduler,
        ),
    )
    scheduler.track_unfinished("human")
    handler = _event_processor(
        graph_execution=graph_execution,
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
    )

    handler.dispatch(
        NodeEventTask(
            frame_id="child-frame",
            event=NodeRunPauseRequestedEvent(
                id="human-run",
                node_id="human",
                node_type=BuiltinNodeTypes.HUMAN_INPUT,
                reason=HitlRequired(
                    session_id="session-1",
                    node_id="human",
                    node_title="Human",
                ),
            ),
        )
    )

    assert graph_execution.paused
    assert not scheduler.is_execution_complete()
    assert root_runtime_state.drain_deferred_ready_tasks() == [
        StartTask(frame_id="child-frame", node_id="human")
    ]
    assert (
        root_runtime_state.get_container_frame("child-frame").frame_id == "child-frame"
    )


def test_graph_execution_tracks_node_executions_by_frame() -> None:
    execution = GraphExecution(workflow_id="workflow")

    first = execution.get_or_create_node_execution(
        frame_id="iteration-0",
        node_id="answer",
    )
    second = execution.get_or_create_node_execution(
        frame_id="iteration-1",
        node_id="answer",
    )

    assert first is not second
    assert (
        execution.node_executions["iteration-0", "answer"].execution_id
        == first.execution_id
    )
    assert (
        execution.node_executions["iteration-1", "answer"].execution_id
        == second.execution_id
    )
    assert first.execution_id != second.execution_id


def test_frame_registry_creates_child_frame_with_rebound_runtime() -> None:
    @dataclass
    class RuntimeBoundNode:
        id: str
        runtime_state: RuntimeState

        node_type: ClassVar[NodeType] = BuiltinNodeTypes.START
        execution_type: ClassVar[NodeExecutionType] = NodeExecutionType.ROOT
        error_strategy: ClassVar[None] = None
        state: ClassVar[NodeState] = NodeState.UNKNOWN

    class RuntimeBoundFactory:
        def __init__(self, runtime_state: RuntimeState) -> None:
            self.runtime_state = runtime_state

        def with_runtime_state(
            self,
            runtime_state: RuntimeState,
        ) -> "RuntimeBoundFactory":
            return RuntimeBoundFactory(runtime_state)

        def with_graph_config(
            self,
            graph_config: Mapping[str, Any],
        ) -> "RuntimeBoundFactory":
            """Accept graph scoping without changing runtime-state binding."""
            _ = graph_config
            return self

        def validate_node(
            self,
            node_config: dict[str, object],
        ) -> NodeExecutionType:
            """Accept the minimal config and report its root execution type."""
            _ = node_config
            return RuntimeBoundNode.execution_type

        def create_node(self, node_config: dict[str, object]) -> RuntimeBoundNode:
            return RuntimeBoundNode(str(node_config["id"]), self.runtime_state)

    graph_config = {
        "nodes": [{"id": "start", "data": {"type": BuiltinNodeTypes.START}}],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, RuntimeBoundFactory(root_runtime_state)),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            runtime_state=root_runtime_state,
        ),
    )
    child_frame = frame_registry.create_child(
        frame_id="child",
        parent_frame_id="root",
        container_id="",
        root_node_id="start",
        variable_pool=VariablePool(),
    )

    assert child_frame.graph is not root_graph
    assert child_frame.graph.nodes["start"] is not root_graph.nodes["start"]
    assert child_frame.graph.nodes["start"].runtime_state is child_frame.state
    assert child_frame.state.ready_queue is root_runtime_state.ready_queue
    assert child_frame.state.graph_execution is graph_execution


def test_frame_registry_restores_child_frame() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            runtime_state=root_runtime_state,
        ),
    )
    variable_pool = VariablePool()
    variable_pool.add(["child", "value"], "saved")
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"answer": "saved"},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=2,
            graph_node_states={"start": NodeState.TAKEN},
            graph_edge_states={},
        ),
    )

    child_frame = frame_registry.restore_child(
        frame_id=frame_state.frame_id,
        parent_frame_id="root",
        container_id="",
        root_node_id=frame_state.root_node_id,
        runtime_data=frame_state.runtime_data,
        variable_pool=cast(
            VariablePool,
            frame_state.runtime_data.variable_pool,
        ).model_copy(deep=True),
    )

    assert child_frame.frame_id == "child-frame"
    assert _variable_value(child_frame.state, ["child", "value"]) == ("saved")
    assert child_frame.state.outputs == {"answer": "saved"}
    assert child_frame.state.node_run_steps == 2
    assert child_frame.graph.nodes["start"].state == NodeState.TAKEN

    graph_execution.pause(
        HitlRequired(
            session_id="session-1",
            node_id="start",
            node_title="Start",
        )
    )
    deferred_task = StartTask(frame_id="child-frame", node_id="start")
    child_frame.state.enqueue_ready_task(deferred_task)
    assert root_runtime_state.drain_deferred_ready_tasks() == [deferred_task]


def test_frame_registry_rejects_frame_state_with_missing_graph_state_ids() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            runtime_state=root_runtime_state,
        ),
    )
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=VariablePool(),
            outputs={},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=0,
            graph_node_states={"missing-node": NodeState.TAKEN},
            graph_edge_states={"missing-edge": NodeState.TAKEN},
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="Saved graph state does not match rebuilt graph",
    ):
        frame_registry.restore_child(
            frame_id=frame_state.frame_id,
            parent_frame_id="root",
            container_id="",
            root_node_id=frame_state.root_node_id,
            runtime_data=frame_state.runtime_data,
            variable_pool=cast(
                VariablePool,
                frame_state.runtime_data.variable_pool,
            ).model_copy(deep=True),
        )
    with pytest.raises(KeyError):
        frame_registry["child-frame"]


def test_frame_registry_copies_frame_runtime_data_from_state() -> None:
    graph_config = {
        "nodes": [
            {"id": "start", "data": {"type": BuiltinNodeTypes.ITERATION_START}},
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    root_graph = Graph.init(
        graph_config=graph_config,
        node_factory=cast(Any, _FrameFactory()),
        root_node_id="start",
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=root_graph,
            runtime_state=root_runtime_state,
        ),
    )
    variable_pool = VariablePool()
    variable_pool.add(["child", "value"], "saved")
    frame_state = IterationFrameState(
        frame_id="child-frame",
        parent_invocation_id="iteration-invocation",
        root_node_id="start",
        index=0,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        runtime_data=FrameRuntimeData(
            variable_pool=variable_pool,
            outputs={"nested": {"value": "saved"}},
            llm_usage=LLMUsage.empty_usage(),
            node_run_steps=0,
            graph_node_states={"start": NodeState.UNKNOWN},
            graph_edge_states={},
        ),
    )

    child_frame = frame_registry.restore_child(
        frame_id=frame_state.frame_id,
        parent_frame_id="root",
        container_id="",
        root_node_id=frame_state.root_node_id,
        runtime_data=frame_state.runtime_data,
        variable_pool=cast(
            VariablePool,
            frame_state.runtime_data.variable_pool,
        ).model_copy(deep=True),
    )
    child_frame.state.variable_pool.add(["child", "value"], "changed")
    child_frame.state.set_output("nested", {"value": "changed"})

    saved_variable = cast(
        VariablePool,
        frame_state.runtime_data.variable_pool,
    ).get(["child", "value"])
    assert saved_variable is not None
    assert saved_variable.to_object() == "saved"
    assert frame_state.runtime_data.outputs == {"nested": {"value": "saved"}}


def test_worker_executes_node_from_ready_task() -> None:
    class RunnableNode:
        id = "start"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "run-1"

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Start",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="root", node_id="start"))
    dispatch_queue = queue.Queue()
    graph = SimpleNamespace(nodes={"start": RunnableNode()})
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=GraphExecution(workflow_id="workflow"),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = dispatch_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, NodeEventTask)
    assert event.frame_id == "root"
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.node_id == "start"


def test_worker_resolves_node_from_task_frame() -> None:
    class RunnableNode:
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE
        execution_id = "run-child"

        def __init__(self, node_id: str, title: str) -> None:
            self.id = node_id
            self.title = title

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title=self.title,
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="child", node_id="answer"))
    dispatch_queue = queue.Queue()
    root_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Root")})
    child_graph = SimpleNamespace(nodes={"answer": RunnableNode("answer", "Child")})
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, root_graph),
            runtime_state=runtime_state,
        ),
    )
    frame_registry.register(
        _execution_frame(
            frame_id="child",
            graph=cast(Graph, child_graph),
            runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = dispatch_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, NodeEventTask)
    assert event.frame_id == "child"
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.node_title == "Child"


def test_worker_binds_node_execution_id_from_task_frame() -> None:
    class RunnableNode:
        id = "answer"
        node_type = BuiltinNodeTypes.CODE
        execution_type = NodeExecutionType.EXECUTABLE

        def __init__(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def bind_execution_id(self, execution_id: str) -> None:
            self.execution_id = execution_id

        def run(self) -> Generator[NodeRunStartedEvent, None, None]:
            yield NodeRunStartedEvent(
                id=self.execution_id,
                node_id=self.id,
                node_type=self.node_type,
                node_title="Answer",
                start_at=datetime.now(UTC).replace(tzinfo=None),
            )

    ready_queue = InMemoryReadyQueue()
    ready_queue.put(StartTask(frame_id="child", node_id="answer"))
    dispatch_queue = queue.Queue()
    graph_execution = GraphExecution(workflow_id="workflow")
    root_execution = graph_execution.get_or_create_node_execution(
        frame_id="root",
        node_id="answer",
    )
    child_execution = graph_execution.get_or_create_node_execution(
        frame_id="child",
        node_id="answer",
    )
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    graph = SimpleNamespace(nodes={"answer": RunnableNode(root_execution.execution_id)})
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="child",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    worker = _worker(
        ready_queue=ready_queue,
        dispatch_queue=dispatch_queue,
        frame_registry=frame_registry,
    )

    worker.start()
    try:
        event = dispatch_queue.get(timeout=1)
    finally:
        worker.stop()
        worker.join(timeout=1)

    assert isinstance(event, NodeEventTask)
    assert isinstance(event.event, NodeRunStartedEvent)
    assert event.event.id != root_execution.execution_id
    assert event.event.id == child_execution.execution_id


def test_dispatcher_preserves_task_event_for_dispatch() -> None:
    event = NodeRunStartedEvent(
        id="run-1",
        node_id="start",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    task_event = NodeEventTask(frame_id="root", event=event)
    dispatch_queue = queue.Queue()
    dispatch_queue.put(task_event)

    class RecordingNodeEventProcessor:
        dispatched_events: list[object]

        def __init__(self) -> None:
            self.dispatched_events = []

        def dispatch(self, event: object) -> None:
            self.dispatched_events.append(event)

    event_processor = RecordingNodeEventProcessor()
    graph_execution = MagicMock(
        aborted=False,
        paused=False,
        error=None,
        completed=False,
    )
    scheduler = MagicMock()
    scheduler.is_execution_complete.side_effect = lambda: bool(
        event_processor.dispatched_events
    )
    dispatcher = Dispatcher(
        dispatch_queue=dispatch_queue,
        event_processor=cast(NodeEventProcessor, event_processor),
        graph_execution=graph_execution,
        scheduler=scheduler,
        command_processor=MagicMock(),
        worker_pool=MagicMock(),
        event_stream=MagicMock(),
    )

    dispatcher._dispatcher_loop()

    assert event_processor.dispatched_events == [task_event]


def test_dispatcher_keeps_polling_when_no_dispatch_event_is_available() -> None:
    graph_execution = GraphExecution(workflow_id="workflow")
    graph_execution.start()
    scheduler = MagicMock()
    scheduler.is_execution_complete.return_value = False
    command_channel = MagicMock()
    command_channel.fetch_commands.side_effect = [
        [],
        [AbortCommand(reason="stop")],
        [],
    ]
    command_processor = CommandProcessor(
        command_channel=command_channel,
        graph_execution=graph_execution,
        frame_registry=FrameRegistry(),
    )
    completed = threading.Event()
    event_stream = MagicMock()
    event_stream.mark_complete.side_effect = completed.set
    dispatcher = Dispatcher(
        dispatch_queue=queue.Queue(),
        event_processor=MagicMock(),
        graph_execution=graph_execution,
        scheduler=scheduler,
        command_processor=command_processor,
        worker_pool=MagicMock(),
        event_stream=event_stream,
    )

    dispatcher.start()
    try:
        assert completed.wait(timeout=1)
    finally:
        dispatcher.stop()

    assert graph_execution.aborted
    assert command_channel.fetch_commands.call_count == 3


def test_dispatcher_polls_commands_while_draining_active_workers() -> None:
    """Abort pause draining without snapshotting a still-active worker.

    The first command poll starts pause draining and the second enters the drain.
    The empty dispatch queue must remain a polling point, so the third poll observes
    the abort even though the worker is deliberately reported active forever.
    Because that worker may still mutate frame state, the aborted execution must
    not create a pause snapshot.
    """
    graph_execution = GraphExecution(workflow_id="workflow")
    graph_execution.start()
    command_channel = MagicMock()
    command_channel.fetch_commands.side_effect = [
        [PauseCommand(reason="wait")],
        [],
        [AbortCommand(reason="stop")],
    ]
    command_processor = CommandProcessor(
        command_channel=command_channel,
        graph_execution=graph_execution,
        frame_registry=FrameRegistry(),
    )
    worker_pool = MagicMock()
    worker_pool.drain.return_value = []
    worker_pool.has_current_tasks.return_value = True
    event_processor = MagicMock()
    completed = threading.Event()
    event_stream = MagicMock()
    event_stream.mark_complete.side_effect = completed.set
    scheduler = MagicMock()
    scheduler.is_execution_complete.return_value = False
    dispatcher = Dispatcher(
        dispatch_queue=queue.Queue(),
        event_processor=event_processor,
        graph_execution=graph_execution,
        scheduler=scheduler,
        command_processor=command_processor,
        worker_pool=worker_pool,
        event_stream=event_stream,
    )

    dispatcher.start()
    try:
        assert completed.wait(timeout=1)
    finally:
        dispatcher.stop()

    assert graph_execution.aborted
    assert graph_execution.paused
    assert command_channel.fetch_commands.call_count == 3
    event_processor.snapshot_frames.assert_not_called()


def test_engine_terminal_failure_takes_precedence_over_pause() -> None:
    """Report a real sibling failure instead of an overlapping pause.

    Two workers run sibling nodes. The failing sibling is held until the answer
    succeeds and queues a pause, reproducing the production race through
    ``Engine.run()`` without constructing an impossible partial engine.
    """
    failure_may_run = threading.Event()

    class PauseBeforeFailure(Layer):
        def on_node_run_start(self, node: Any) -> None:
            """Hold only the failing sibling until pause processing can begin.

            The answer sibling remains free to finish. Once its success event has
            queued the pause command, ``failure_may_run`` releases this worker so
            the dispatcher observes a fatal result during pause draining.

            Args:
                node: Node about to run on one of the engine's workers.

            """
            if node.id == "fail":
                assert failure_may_run.wait(timeout=1)

        def on_event(self, event: EngineEvent) -> None:
            """Queue pause from answer success, then release the failing worker.

            A node-success event is a dispatcher command boundary. Sending the
            pause here guarantees it is applied before the already-enqueued sibling
            failure is dispatched, reproducing the terminal-state race.

            Args:
                event: Engine event currently being published to this layer.

            """
            if not (
                isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer"
            ):
                return
            command_channel = self.command_channel
            assert command_channel is not None
            command_channel.send_command(PauseCommand(reason="wait"))
            failure_may_run.set()

    engine = loads(
        json.dumps({
            "kind": "graph",
            "graph": {
                "nodes": [
                    {
                        "id": "start",
                        "data": {"type": "start", "variables": []},
                    },
                    {
                        "id": "answer",
                        "data": {"type": "answer", "answer": "done"},
                    },
                    {
                        "id": "fail",
                        "data": {
                            "type": "assigner",
                            "version": "2",
                            "items": [
                                {
                                    "variable_selector": ["missing", "value"],
                                    "input_type": "constant",
                                    "operation": "over-write",
                                    "value": "unused",
                                }
                            ],
                        },
                    },
                ],
                "edges": [
                    {"source": "start", "target": "answer"},
                    {"source": "start", "target": "fail"},
                ],
            },
        }),
        workers=2,
    )
    engine.add_layer(PauseBeforeFailure())
    events: list[EngineEvent] = []

    try:
        with pytest.raises(RuntimeError, match="Variable"):
            events.extend(engine.run())
    finally:
        failure_may_run.set()

    assert engine.runtime_state.graph_execution.paused
    assert isinstance(events[-1], GraphRunFailedEvent)
    assert not any(isinstance(event, GraphRunPausedEvent) for event in events)


def test_engine_terminal_abort_takes_precedence_over_pause() -> None:
    """Emit an abort rather than a pause when both lifecycle flags are set.

    Abort can arrive while the dispatcher drains workers for an earlier pause.
    The completed run must expose the irreversible abort as its terminal result so
    callers do not persist it as resumable state.
    """
    command_channel = InMemoryChannel()
    command_channel.send_command(PauseCommand(reason="wait"))
    command_channel.send_command(AbortCommand(reason="stop"))
    engine = _load_minimal_engine(command_channel=command_channel)

    events = list(engine.run())

    assert engine.runtime_state.graph_execution.paused
    assert engine.runtime_state.graph_execution.aborted
    assert isinstance(events[-1], GraphRunAbortedEvent)
    assert not any(isinstance(event, GraphRunPausedEvent) for event in events)


def test_resume_rejects_task_for_missing_container_invocation() -> None:
    """Reject an orphaned ResumeTask before any worker can claim it.

    Resume tasks depend on persisted container-run state. Preflight must resolve
    that invocation synchronously, restore the drained queue on failure, and leave
    workers stopped; otherwise a worker dies on ``KeyError`` and the dispatcher can
    incorrectly observe an empty scheduler as successful completion.
    """
    engine = _load_minimal_engine()
    runtime_state = engine.runtime_state
    runtime_state.graph_execution.start()
    resume_task = ResumeTask(
        invocation_id="missing-invocation",
        result=ContainerExecutionResult(
            metadata={},
            steps=0,
            node_run_result=ContainerNodeRunResult(),
        ),
    )
    runtime_state.enqueue_ready_task(resume_task)
    events: list[EngineEvent] = []

    with pytest.raises(KeyError, match="missing-invocation"):
        events.extend(engine.run())

    assert runtime_state.ready_queue.get(timeout=0.01) == resume_task
    assert isinstance(events[-1], GraphRunFailedEvent)


def test_event_processor_dispatches_task_event_payload() -> None:
    event = NodeRunStartedEvent(
        id="run-1",
        node_id="start",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Start",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    node_execution = MagicMock(retry_count=0)
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = node_execution
    event_stream = MagicMock()
    runtime_state = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, MagicMock()),
            runtime_state=runtime_state,
        ),
    )
    handler = _event_processor(
        graph_execution=graph_execution,
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
    )

    handler.dispatch(NodeEventTask(frame_id="root", event=event))

    runtime_state.increment_node_run_steps.assert_called_once_with()
    event_stream.collect.assert_called_once_with(event)


def test_event_processor_stamps_frame_owner_on_node_and_edge_events() -> None:
    graph = MagicMock()
    graph.nodes = {"child": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    event_stream = MagicMock()
    scheduler = MagicMock()
    taken = GraphEdgeTakenEvent(
        frame_id="root",
        edge_id="taken",
        source_node_id="child",
        target_node_id="next",
    )
    skipped = GraphEdgeSkippedEvent(
        frame_id="root",
        edge_id="skipped",
        source_node_id="child",
        target_node_id="other",
    )
    scheduler.process_node_success.return_value = (["next"], [taken, skipped])
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
            scheduler=scheduler,
            container_id="owner",
        ),
    )
    handler = _event_processor(
        graph_execution=graph_execution,
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
    )
    event = NodeRunSucceededEvent(
        id="run-child",
        node_id="child",
        node_type=BuiltinNodeTypes.CODE,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(outputs={"answer": "ok"}),
        container_id="stale",
    )

    handler.dispatch(NodeEventTask(frame_id="root", event=event))

    scheduler.process_node_success.assert_called_once_with("child")
    scheduler.enqueue_node.assert_called_once_with("next")
    assert event.container_id == "owner"
    assert taken.container_id == "owner"
    assert skipped.container_id == "owner"
    assert event_stream.collect.call_args_list == [
        call(taken),
        call(skipped),
        call(event),
    ]


def test_loop_exception_preserves_default_outputs() -> None:
    """Keep configured Loop fallbacks authoritative after a partial run.

    A failed Loop may already have written partial values into its parent variable
    pool. The exception event represents the configured default-value continuation,
    so dispatch must store and emit that fallback instead of reconciling it with the
    stale partial value as successful Loop completion does.
    """
    graph = MagicMock()
    graph.nodes = {
        "loop": MagicMock(
            error_strategy=ErrorStrategy.DEFAULT_VALUE,
            execution_type=NodeExecutionType.CONTAINER,
        ),
    }
    runtime_state = MagicMock()
    runtime_state.variable_pool = VariablePool()
    runtime_state.variable_pool.add(("loop", "counter"), 1)
    event_stream = MagicMock()
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    processor = _event_processor(
        graph_execution=MagicMock(),
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
    )
    event = NodeRunExceptionEvent(
        id="loop-run",
        node_id="loop",
        node_type=BuiltinNodeTypes.LOOP,
        start_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
        node_run_result=NodeRunResult(outputs={"counter": 99}),
        error="loop failed",
    )

    processor.dispatch(NodeEventTask(frame_id="root", event=event))

    assert event.node_run_result.outputs == {"counter": 99}
    stored_counter = runtime_state.variable_pool.get(("loop", "counter"))
    assert stored_counter is not None
    assert stored_counter.to_object() == 99
    event_stream.collect.assert_called_once_with(event)


def test_parallel_iteration_preserves_aggregate_and_response_order() -> None:  # ruff: ignore[too-many-locals]
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {
                    "type": BuiltinNodeTypes.ITERATION_START,
                    "container_id": "iteration",
                },
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b", "c"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": True,
        "parallel_nums": 2,
    })
    iteration_node.runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    event_stream = MagicMock()
    handler, container_handlers = _event_processor_with_container(
        graph_execution=graph_execution,
        event_stream=cast(EventStream, event_stream),
        frame_registry=frame_registry,
    )

    _start_iteration_await(
        container_handlers["iteration"],
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0, 1),
        items=("a", "b", "c"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=2,
    )

    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:1",
        node_id="iteration-start",
    )
    assert ready_queue.qsize() == 0

    second_frame = frame_registry["iteration-invocation:iteration:1"]
    second_frame.state.variable_pool.add(["answer", "text"], "second")
    second_frame.state.set_output("answer", "second")
    handler.dispatch(
        NodeEventTask(
            frame_id="iteration-invocation:iteration:1",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-1",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFrameRequest)
    assert resume_task.result.indexes == (2,)
    container_handlers["iteration"].handle_request(
        invocation_id=resume_task.invocation_id,
        request=resume_task.result,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:2",
        node_id="iteration-start",
    )

    third_frame = frame_registry["iteration-invocation:iteration:2"]
    third_frame.state.variable_pool.add(["answer", "text"], "third")
    third_frame.state.set_output("answer", "third")
    handler.dispatch(
        NodeEventTask(
            frame_id="iteration-invocation:iteration:2",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-2",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    first_frame = frame_registry["iteration-invocation:iteration:0"]
    first_frame.state.variable_pool.add(["answer", "text"], "first")
    first_frame.state.set_output("answer", "first")
    handler.dispatch(
        NodeEventTask(
            frame_id="iteration-invocation:iteration:0",
            event=NodeRunSucceededEvent(
                id="iteration-start-run-0",
                node_id="iteration-start",
                node_type=BuiltinNodeTypes.ITERATION_START,
                start_at=datetime.now(UTC).replace(tzinfo=None),
                finished_at=datetime.now(UTC).replace(tzinfo=None),
                node_run_result=NodeRunResult(),
            ),
        ),
    )

    final_resume_task = _get_resume_task(ready_queue)
    assert isinstance(final_resume_task.result, ContainerExecutionResult)
    assert final_resume_task.result.node_run_result.outputs["output"].to_object() == [
        "first",
        "second",
        "third",
    ]
    assert runtime_state.outputs["answer"] == "third"


def test_terminated_iteration_waits_for_all_scheduled_frames() -> None:
    ready_queue = InMemoryReadyQueue()
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=VariablePool(),
        start_at=1,
        ready_queue=ready_queue,
    )
    run_state = IterationRunState(
        invocation_id="iteration-invocation",
        frame_id="root",
        node_id="iteration",
        started_at=datetime.now(UTC).replace(tzinfo=None),
        items=(build_container_value("a"), build_container_value("b")),
        root_node_id="iteration-start",
        output_selector=("answer", "text"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=False,
        parallel_nums=2,
        scheduled_count=2,
        completed_count=1,
        errors=("bad item",),
    )
    runtime_state.put_container_run(run_state)
    frame_registry = MagicMock()
    frame_registry.__getitem__.return_value.state = runtime_state
    handler = IterationContainerHandler(frame_registry=frame_registry)
    parent_frame = cast(
        ExecutionFrame,
        SimpleNamespace(state=runtime_state),
    )

    assert handler._finish_failed_iteration_if_ready(
        parent_frame=parent_frame,
        run_state=run_state,
    )
    assert ready_queue.qsize() == 0

    run_state = run_state.model_copy(
        update={"completed_count": 2},
    )
    runtime_state.put_container_run(run_state)
    assert handler._finish_failed_iteration_if_ready(
        parent_frame=parent_frame,
        run_state=run_state,
    )
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, ContainerExecutionResult)
    assert resume_task.result.node_run_result.error == "bad item"


def test_iteration_frame_completion_requests_next_index() -> None:
    graph_config = {
        "nodes": [
            {
                "id": "iteration-start",
                "data": {
                    "type": BuiltinNodeTypes.ITERATION_START,
                    "container_id": "iteration",
                },
            },
        ],
        "edges": [],
    }
    ready_queue = InMemoryReadyQueue()
    variable_pool = VariablePool()
    variable_pool.add(["source", "items"], ["a", "b", "c"])
    graph_execution = GraphExecution(workflow_id="workflow")
    runtime_state = RuntimeState(
        workflow_id="workflow",
        variable_pool=variable_pool,
        start_at=1,
        ready_queue=ready_queue,
        graph_execution=graph_execution,
    )
    iteration_node = IterationNode.__new__(IterationNode)
    iteration_node.init_node_identity("iteration")
    iteration_node.init_node_data({
        "type": "iteration",
        "start_node_id": "iteration-start",
        "iterator_selector": ["source", "items"],
        "output_selector": ["answer", "text"],
        "error_handle_mode": ErrorHandleMode.TERMINATED,
        "is_parallel": True,
        "parallel_nums": 2,
    })
    iteration_node.runtime_state = runtime_state
    iteration_node.graph_config = graph_config
    graph = SimpleNamespace(
        nodes={"iteration": iteration_node},
        graph_config=graph_config,
        node_factory=_FrameFactory(),
    )
    frame_registry = FrameRegistry()
    frame_registry.register(
        _execution_frame(
            frame_id="root",
            graph=cast(Graph, graph),
            runtime_state=runtime_state,
        ),
    )
    container_handler = IterationContainerHandler(
        frame_registry=frame_registry,
    )
    _start_iteration_await(
        container_handler,
        runtime_state,
        invocation_id="iteration-invocation",
        indexes=(0, 1),
        items=("a", "b", "c"),
        error_handle_mode=ErrorHandleMode.TERMINATED,
        flatten_output=True,
        parallel_nums=2,
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:0",
        node_id="iteration-start",
    )
    assert ready_queue.get(timeout=0.01) == StartTask(
        frame_id="iteration-invocation:iteration:1",
        node_id="iteration-start",
    )

    sibling_frame = frame_registry["iteration-invocation:iteration:1"]
    sibling_frame.state.variable_pool.add(["answer", "text"], "second")
    sibling_frame.scheduler.finish_execution("iteration-start")

    container_handler.complete_frame_if_ready(sibling_frame)

    run_state = runtime_state.get_container_run("iteration-invocation")
    assert isinstance(run_state, IterationRunState)
    assert run_state.completed_count == 1
    assert run_state.outputs["1"].to_object() == "second"
    resume_task = _get_resume_task(ready_queue)
    assert isinstance(resume_task.result, IterationFrameRequest)
    assert resume_task.result.indexes == (2,)


@pytest.mark.parametrize(
    ("max_steps", "max_time", "step_count", "elapsed_time", "expected_reason"),
    [
        (3, 1000, 4, 0, "Maximum execution steps exceeded: 4 > 3"),
        (10, 10, 0, 20, "Maximum execution time exceeded:"),
    ],
)
def test_execution_limits_layer_sends_abort_when_limit_is_exceeded(
    max_steps: int,
    max_time: int,
    step_count: int,
    elapsed_time: int,
    expected_reason: str,
) -> None:
    """Verify the event hook emits an abort for step and elapsed-time limits.

    The parameter sets isolate one exceeded limit at a time so the assertion
    covers the production event path without exposing a test-only layer method.
    """
    layer = ExecutionLimitsLayer(max_steps=max_steps, max_time=max_time)
    command_channel = MagicMock()
    layer.command_channel = command_channel
    layer.on_graph_start()
    layer.step_count = step_count
    layer.start_time = time() - elapsed_time

    layer.on_event(
        NodeRunSucceededEvent(
            id="node-run-1",
            node_id="node-1",
            node_type=BuiltinNodeTypes.CODE,
            start_at=datetime.now(UTC).replace(tzinfo=None),
        )
    )

    abort_command = command_channel.send_command.call_args.args[0]
    assert isinstance(abort_command, AbortCommand)
    assert abort_command.reason is not None
    assert abort_command.reason.startswith(expected_reason)


def test_execution_limits_layer_matches_subclassed_node_start_event() -> None:
    class CustomNodeRunStartedEvent(NodeRunStartedEvent):
        pass

    layer = ExecutionLimitsLayer(max_steps=3, max_time=10)
    layer.on_graph_start()

    layer.on_event(
        CustomNodeRunStartedEvent(
            id="node-run-1",
            node_id="node-1",
            node_type=BuiltinNodeTypes.CODE,
            node_title="Code",
            start_at=datetime.now(UTC).replace(tzinfo=None),
        ),
    )

    assert layer.step_count == 1
