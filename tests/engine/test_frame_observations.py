"""Public execution observations used by delayed event consumers."""

from __future__ import annotations

import json
from collections.abc import Generator, Sequence
from datetime import UTC, datetime
from threading import Event, Thread
from typing import Any

import pytest

from graphon import engine_events
from graphon.dsl import loads
from graphon.engine.command import (
    CommandProcessor,
    UpdateVariablesCommand,
)
from graphon.engine.command.builtin.in_memory import InMemoryChannel
from graphon.engine.filter import EngineEventFilterContext, filter_engine_events
from graphon.engine.filter.builtin.response_stream import ResponseStreamFilter
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.graph import GraphRunFailedEvent, GraphRunPausedEvent
from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeEventPayload, NodeRunResult
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.container_effects import (
    ContainerAwaitRequest,
    ContainerExecutionResult,
    ContainerNodeRunResult,
    LoopFrameRequest,
)
from graphon.nodes.human_input.entities import Completed, HITLContext, PauseRequested
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from graphon.runtime.container_state import (
    ContainerFrameState,
    CustomContainerFrameState,
    CustomContainerRunState,
)
from graphon.runtime.ready_queue import ResumeTask
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import ObjectVariable, StringVariable
from tests.engine.test_runtime_state_serialization import (
    _complete_iteration_hitl,
    _hitl_engine,
    _iteration_dsl,
    _new_runtime_state,
)


def _node(node_id: str, node_type: str, **data: Any) -> dict[str, Any]:
    return {"id": node_id, "data": {"type": node_type, **data}}


def _dsl(nodes: list[dict[str, Any]], edges: list[tuple[str, str]]) -> str:
    return json.dumps({
        "kind": "graph",
        "graph": {
            "nodes": [_node("start", "start", variables=[]), *nodes],
            "edges": [{"source": source, "target": target} for source, target in edges],
        },
    })


def _events(events: Sequence[EngineEvent], name: str) -> list[Any]:
    # Names allow the pre-implementation run to fail on missing observations.
    return [event for event in events if type(event).__name__ == name]


def _values(event: Any) -> dict[tuple[str, ...], Any]:
    return {tuple(variable.selector): variable.value for variable in event.variables}


def _assert_frame_lifetimes(events: Sequence[EngineEvent]) -> None:
    starts = _events(events, "FrameStartedEvent")
    finishes = _events(events, "FrameFinishedEvent")
    assert starts, "the engine must publish a root frame baseline"
    assert {event.frame_id for event in finishes} == {
        event.frame_id for event in starts
    }
    assert len(finishes) == len(starts)
    frame_ids = {event.frame_id for event in starts}
    for event in events:
        if isinstance(event, NodeEvent) or hasattr(event, "frame_id"):
            assert event.frame_id in frame_ids
    for start in starts:
        finish = next(event for event in finishes if event.frame_id == start.frame_id)
        assert events.index(start) < events.index(finish)
        for event in events:
            if event is start or event is finish:
                continue
            if getattr(event, "frame_id", None) == start.frame_id:
                assert events.index(start) < events.index(event) < events.index(finish)
            if isinstance(event, NodeEvent) and event.frame_id == start.frame_id:
                assert event.id == start.node_execution_ids[event.node_id]
    assert events.index(finishes[-1]) < len(events) - 1


def test_root_baseline_commits_identity_and_detached_values() -> None:
    engine = loads(
        _dsl([_node("answer", "answer", answer="ok")], [("start", "answer")]),
        start_inputs={"payload": {"items": ["before"]}, "empty": "", "null": None},
        workflow_id="same-workflow",
    )
    context = EngineEventFilterContext.from_engine(engine)
    events = list(engine.run())
    _assert_frame_lifetimes(events)
    start = _events(events, "FrameStartedEvent")[0]
    assert start.frame_id == "root"
    assert start.parent_frame_id is None
    assert start.invocation_id is None
    assert start.container_execution_id is None
    assert start.root_node_id == "start"
    assert start.index is None
    assert start.reason == WorkflowStartReason.INITIAL
    assert set(start.node_execution_ids) == {"start", "answer"}
    assert context.run_id == start.node_execution_ids["start"]
    assert context.graph.graph_config is not None
    assert context.run_id == EngineEventFilterContext.from_engine(engine).run_id
    other = loads(_dsl([], []), workflow_id="same-workflow")
    assert EngineEventFilterContext.from_engine(other).run_id != context.run_id

    commits = _events(events, "FrameVariablesUpdatedEvent")
    committed = next(
        event for event in commits if ("start", "payload") in _values(event)
    )
    traversal = _events(events, "GraphEdgeTakenEvent")[0]
    committed_values = {
        selector: value
        for event in commits
        if events.index(event) < events.index(traversal)
        for selector, value in _values(event).items()
    }
    assert committed_values["start", "empty"] == ""
    assert committed_values["start", "null"] is None
    success = next(
        event
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "start"
    )
    assert events.index(committed) < events.index(traversal) < events.index(success)
    variable = engine.runtime_state.variable_pool.get_variable(("start", "payload"))
    assert isinstance(variable, ObjectVariable)
    variable.value["items"].append("after")
    assert _values(start)["start", "payload"] == {"items": ["before"]}
    assert _values(committed)["start", "payload"] == {"items": ["before"]}
    assert success.node_run_result.outputs["payload"] == {"items": ["before"]}
    for name in (
        "FrameStartedEvent",
        "FrameFinishedEvent",
        "FrameVariablesUpdatedEvent",
    ):
        assert name in engine_events.__all__
        event = _events(events, name)[0]
        assert type(event).model_validate_json(event.model_dump_json()) == event


@pytest.mark.parametrize("items", [[], ["a", "b"]])
def test_nested_parallel_frames_include_empty_bodies_and_invocation_identity(
    items: list[str],
) -> None:
    engine = loads(
        _dsl(
            [
                _node(
                    "iteration",
                    "iteration",
                    start_node_id="item-start",
                    iterator_selector=["start", "items"],
                    output_selector=["loop", "loop_round"],
                    is_parallel=True,
                    parallel_nums=2,
                ),
                _node("item-start", "iteration-start", container_id="iteration"),
                _node(
                    "loop",
                    "loop",
                    container_id="iteration",
                    start_node_id="loop-start",
                    loop_count=2,
                    break_conditions=[],
                    logical_operator="and",
                ),
                _node("loop-start", "loop-start", container_id="loop"),
            ],
            [("start", "iteration"), ("item-start", "loop")],
        ),
        start_inputs={"items": items},
        workers=1,
    )
    events = list(engine.run())
    _assert_frame_lifetimes(events)
    starts = _events(events, "FrameStartedEvent")
    assert len(starts) == 1 + 3 * len(items)
    by_id = {event.frame_id: event for event in starts}
    for start in starts[1:]:
        parent = by_id[start.parent_frame_id]
        assert start.invocation_id
        assert (
            start.container_execution_id
            == parent.node_execution_ids[start.container_id]
        )
        assert start.index in {0, 1}
        if start.container_id == "iteration":
            assert _values(start)["iteration", "item"] == items[start.index]
            assert _values(start)["iteration", "index"] == start.index
        else:
            assert start.container_id == "loop"
            assert start.parent_frame_id != "root"
    assert all(
        event.outcome == "succeeded" for event in _events(events, "FrameFinishedEvent")
    )
    assert not any(isinstance(event, NodeRunStreamChunkEvent) for event in events)
    assert engine.runtime_state.container_frames() == ()


def _loop_dsl(*, invalid_initial_value: bool = False) -> str:
    variables = [
        {
            "label": "counter",
            "var_type": "number",
            "value_type": "constant",
            "value": 0,
        },
        {
            "label": "copy",
            "var_type": "number",
            "value_type": "variable",
            "value": ["missing", "value"]
            if invalid_initial_value
            else ["loop", "counter"],
        },
    ]
    return _dsl(
        [
            _node(
                "loop",
                "loop",
                start_node_id="loop-start",
                loop_count=2,
                break_conditions=[],
                logical_operator="and",
                loop_variables=variables,
            ),
            _node("loop-start", "loop-start", container_id="loop"),
            _node(
                "increment",
                "assigner",
                version="2",
                container_id="loop",
                items=[
                    {
                        "variable_selector": ["loop", "counter"],
                        "input_type": "constant",
                        "operation": "+=",
                        "value": 1,
                    }
                ],
            ),
            _node(
                "body-value",
                "template-transform",
                container_id="loop",
                template="round",
                variables=[],
            ),
        ],
        [("start", "loop"), ("loop-start", "increment"), ("increment", "body-value")],
    )


def test_loop_initialization_assignments_and_resets_are_observable() -> None:
    engine = loads(_loop_dsl())
    engine.runtime_state.variable_pool.add(("body-value", "output"), "stale")
    events = list(engine.run())
    _assert_frame_lifetimes(events)
    children = _events(events, "FrameStartedEvent")[1:]
    assert [_values(event)["loop", "counter"] for event in children] == [0, 1]
    assert [_values(event)["loop", "copy"] for event in children] == [0, 0]
    assert all(("body-value", "output") not in _values(event) for event in children)
    commits = _events(events, "FrameVariablesUpdatedEvent")
    initial = next(
        event for event in commits if _values(event).get(("loop", "counter")) == 0
    )
    assert initial.frame_id == "root"
    assert events.index(initial) < events.index(children[0])
    for index, child in enumerate(children):
        updates = [
            event
            for event in commits
            if _values(event).get(("loop", "counter")) == index + 1
        ]
        assert {event.frame_id for event in updates} >= {"root", child.frame_id}
        request = next(
            event
            for event in _events(events, "NodeRunVariableUpdatedEvent")
            if event.node_id == "increment" and event.frame_id == child.frame_id
        )
        assert all(
            events.index(event) < events.index(request)
            for event in updates
            if event.frame_id == child.frame_id
        )
        if index == 0:
            assert all(
                events.index(event) < events.index(children[1]) for event in updates
            )


def test_failed_loop_initialization_keeps_only_successful_commits() -> None:
    engine = loads(_loop_dsl(invalid_initial_value=True))
    events: list[EngineEvent] = []
    with pytest.raises(RuntimeError, match="Invalid value for loop variable copy"):
        events.extend(engine.run())
    commits = _events(events, "FrameVariablesUpdatedEvent")
    assert any(_values(event).get(("loop", "counter")) == 0 for event in commits)
    assert all(("loop", "copy") not in _values(event) for event in commits)
    _assert_frame_lifetimes(events)
    assert _events(events, "FrameFinishedEvent")[-1].outcome == "failed"
    assert isinstance(events[-1], GraphRunFailedEvent)


@pytest.mark.parametrize("rebuild", [False, True])
def test_paused_parallel_frames_resume_with_the_same_identity(rebuild: bool) -> None:
    resume = Event()

    def decide(context: HITLContext) -> Completed | PauseRequested:
        return (
            _complete_iteration_hitl(context)
            if resume.is_set()
            else PauseRequested(session_id="pending")
        )

    engine = _hitl_engine(
        _iteration_dsl(),
        runtime_state=_new_runtime_state({"items": ["a", "b", "c"]}),
        callback=decide,
    )
    run_id = EngineEventFilterContext.from_engine(engine).run_id
    paused = list(engine.run())
    assert isinstance(paused[-1], GraphRunPausedEvent)
    assert not _events(paused, "FrameFinishedEvent")
    starts = {event.frame_id: event for event in _events(paused, "FrameStartedEvent")}
    assert len(starts) == 3
    if rebuild:
        state = RuntimeState.from_snapshot(engine.runtime_state.dumps())
        engine = _hitl_engine(
            _iteration_dsl(), runtime_state=state, callback=_complete_iteration_hitl
        )
    else:
        resume.set()
    assert EngineEventFilterContext.from_engine(engine).run_id == run_id
    resumed = list(engine.run())
    _assert_frame_lifetimes(resumed)
    resumed_starts = _events(resumed, "FrameStartedEvent")
    for start in resumed_starts:
        if start.frame_id in starts:
            assert start.reason == WorkflowStartReason.RESUMPTION
            assert start.node_execution_ids == starts[start.frame_id].node_execution_ids
        else:
            assert start.reason == WorkflowStartReason.INITIAL
    assert len(resumed_starts) == 4


@pytest.mark.parametrize("in_child", [False, True])
def test_raw_chunks_are_live_and_attempts_keep_the_reserved_id(  # ruff: ignore[too-many-statements]
    monkeypatch: pytest.MonkeyPatch,
    in_child: bool,
) -> None:
    nodes = [
        _node(
            "source",
            "template-transform",
            template="unused",
            variables=[],
            retry_config={"retry_enabled": True, "max_retries": 1, "retry_interval": 0},
        ),
        _node("answer", "answer", answer="{{#source.output#}}"),
    ]
    edges = [("start", "source"), ("source", "answer")]
    if in_child:
        for node in nodes:
            node["data"]["container_id"] = "iteration"
        nodes.extend([
            _node(
                "iteration",
                "iteration",
                start_node_id="item-start",
                iterator_selector=["start", "items"],
                output_selector=["answer", "answer"],
            ),
            _node("item-start", "iteration-start", container_id="iteration"),
        ])
        edges = [("start", "iteration"), ("item-start", "source"), ("source", "answer")]
    engine = loads(_dsl(nodes, edges), start_inputs={"items": ["only"]}, workers=1)
    release = Event()
    received = Event()
    attempts = 0

    def stream(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        nonlocal attempts
        attempts += 1
        yield StreamChunkEvent(selector=["source", "output"], chunk=str(attempts))
        if attempts == 1:
            message = "retry this"
            raise ValueError(message)
        assert release.wait(5)
        yield StreamChunkEvent(selector=["source", "output"], chunk="", is_final=True)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED, outputs={"output": "2"}
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "retry", property(lambda _: True))
    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    events: list[EngineEvent] = []
    errors: list[Exception] = []

    def consume() -> None:
        try:
            for event in engine.run():
                events.append(event)
                if isinstance(event, NodeRunStreamChunkEvent) and event.chunk == "2":
                    received.set()
        except Exception as error:  # ruff: ignore[blind-except]
            errors.append(error)

    consumer = Thread(target=consume, daemon=True)
    consumer.start()
    try:
        assert received.wait(3), "producer chunks must arrive before the node completes"
        assert not any(
            isinstance(event, NodeRunSucceededEvent) and event.node_id == "source"
            for event in events
        )
    finally:
        release.set()
        consumer.join(5)
    assert not consumer.is_alive()
    assert not errors
    chunks = [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]
    assert {event.node_id for event in chunks} == {"source"}
    assert [event.retry_index for event in chunks] == [0, 1, 1]
    assert all((event.frame_id != "root") == in_child for event in chunks)
    success = next(
        event
        for event in events
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "source"
    )
    assert success.retry_index == 1
    assert all(
        event.retry_index == 0 for event in _events(events, "NodeRunStartedEvent")
    )
    retry = _events(events, "NodeRunRetryEvent")[0]
    assert retry.retry_index == 1
    assert events.index(chunks[0]) < events.index(retry) < events.index(chunks[1])
    _assert_frame_lifetimes(events)


@pytest.mark.parametrize("abort", [False, True])
def test_failed_or_aborted_child_closes_all_open_frames(abort: bool) -> None:
    if abort:
        engine = _hitl_engine(
            _iteration_dsl(),
            runtime_state=_new_runtime_state({"items": ["a", "b"]}),
            callback=lambda _: PauseRequested(session_id="pending"),
        )
        paused = list(engine.run())
        assert isinstance(paused[-1], GraphRunPausedEvent)
        engine.request_abort("stop unfinished children")
    else:
        # Invalid assignment is an ordinary child execution failure.
        document = json.loads(_loop_dsl())
        document["graph"]["nodes"][3]["data"]["items"][0]["variable_selector"] = [
            "missing",
            "value",
        ]
        engine = loads(json.dumps(document))
    events: list[EngineEvent] = []
    if abort:
        events.extend(engine.run())
    else:
        with pytest.raises(RuntimeError):
            events.extend(engine.run())
    _assert_frame_lifetimes(events)
    finishes = _events(events, "FrameFinishedEvent")
    assert all(
        event.outcome == ("aborted" if abort else "failed") for event in finishes
    )
    assert all(event.error for event in finishes)
    assert len(finishes) >= 2


def test_filtered_events_do_not_expose_frame_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = InMemoryChannel()
    channel.send_command(
        UpdateVariablesCommand(
            updates=[
                StringVariable(
                    name="secret", selector=["env", "secret"], value="updated-secret"
                )
            ]
        )
    )
    engine = loads(
        _dsl([_node("answer", "answer", answer="safe")], [("start", "answer")]),
        command_channel=channel,
    )
    # This root reports no outputs; only the observations contain the secrets.
    monkeypatch.setattr(
        engine.graph.nodes["start"],
        "_run",
        lambda: NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED),
    )
    engine.runtime_state.variable_pool.add(("env", "secret"), "do-not-forward")
    context = EngineEventFilterContext.from_engine(engine)
    raw = list(engine.run())
    assert (
        _values(_events(raw, "FrameStartedEvent")[0])["env", "secret"]
        == "do-not-forward"
    )
    assert any(
        _values(event).get(("env", "secret")) == "updated-secret"
        for event in _events(raw, "FrameVariablesUpdatedEvent")
    )
    filtered = list(
        filter_engine_events(raw, context=context, filters=[ResponseStreamFilter()])
    )
    assert not _events(filtered, "FrameStartedEvent")
    assert not _events(filtered, "FrameVariablesUpdatedEvent")
    assert "do-not-forward" not in "".join(
        event.model_dump_json() for event in filtered
    )
    assert "updated-secret" not in "".join(
        event.model_dump_json() for event in filtered
    )
    assert (
        "".join(
            event.chunk
            for event in filtered
            if isinstance(event, NodeRunStreamChunkEvent)
        )
        == "safe"
    )


def test_custom_frames_and_partial_commands_observe_shared_pool_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = loads(_loop_dsl())
    registry = engine._frame_registry
    root = engine.runtime_state
    root.variable_pool.add(("host", "value"), "before")
    root.variable_pool.add(("host", "second"), "before")
    run = CustomContainerRunState(
        invocation_id="opaque-invocation",
        frame_id="root",
        node_id="loop",
        started_at=datetime.now(UTC),
        payload="{}",
    )
    root.put_container_run(run)
    for frame_id, pool in [
        ("opaque-shared", root.variable_pool),
        ("opaque-copy", root.variable_pool.model_copy(deep=True)),
    ]:
        frame = registry.create_child(
            frame_id=frame_id,
            parent_frame_id="root",
            container_id="loop",
            root_node_id="loop-start",
            variable_pool=pool,
        )
        root.put_container_frame(
            CustomContainerFrameState(
                frame_id=frame_id,
                parent_invocation_id=run.invocation_id,
                runtime_data=frame.state.snapshot_frame(
                    variable_pool_scope="parent"
                    if pool is root.variable_pool
                    else "local"
                ),
            )
        )
        frame.scheduler.enqueue_node("loop-start")

    calls: list[tuple[int, tuple[str, ...]]] = []
    add = VariablePool.add
    isolated_pool = registry["opaque-copy"].state.variable_pool

    def reject_one(pool: VariablePool, selector: Sequence[str], value: Any) -> None:
        if pool is isolated_pool and tuple(selector) == (
            "host",
            "second",
        ):
            message = "rejected in isolated frame"
            raise ValueError(message)
        calls.append((id(pool), tuple(selector)))
        add(pool, selector, value)

    monkeypatch.setattr(VariablePool, "add", reject_one)
    channel = InMemoryChannel()
    channel.send_command(
        UpdateVariablesCommand(
            updates=[
                StringVariable(name="bad", selector=["bad"], value="ignored"),
                StringVariable(
                    name="value", selector=["host", "value"], value="committed"
                ),
                StringVariable(
                    name="second", selector=["host", "second"], value="partial"
                ),
            ]
        )
    )
    CommandProcessor(
        command_channel=channel,
        graph_execution=root.graph_execution,
        frame_registry=registry,
    ).process_commands()
    registry.remove("opaque-shared")
    registry.remove("opaque-copy")
    engine._event_stream.mark_complete()
    events = list(engine._event_stream.emit_events())
    commits = _events(events, "FrameVariablesUpdatedEvent")
    assert {event.frame_id for event in commits} == {
        "root",
        "opaque-shared",
        "opaque-copy",
    }
    assert all(("bad",) not in _values(event) for event in commits)
    assert {
        event.frame_id
        for event in commits
        if _values(event).get(("host", "value")) == "committed"
    } == {"root", "opaque-shared", "opaque-copy"}
    assert {
        event.frame_id
        for event in commits
        if _values(event).get(("host", "second")) == "partial"
    } == {"root", "opaque-shared"}
    assert calls.count((id(root.variable_pool), ("host", "value"))) == 1
    assert calls.count((id(root.variable_pool), ("host", "second"))) == 1
    child_starts = [
        event
        for event in _events(events, "FrameStartedEvent")
        if event.frame_id != "root"
    ]
    assert len(child_starts) == 2
    assert all(event.root_node_id in {None, "loop-start"} for event in child_starts)
    assert all(event.index is None for event in child_starts)
    assert all(
        event.invocation_id == run.invocation_id and event.parent_frame_id == "root"
        for event in child_starts
    )
    assert len(_events(events, "FrameFinishedEvent")) == 2
    assert not any(isinstance(event, NodeEvent) for event in events)
    root.variable_pool.add(("host", "value"), "later")
    assert {
        _values(event)["host", "value"]
        for event in commits
        if ("host", "value") in _values(event)
    } == {"committed"}


def test_handled_failure_commits_defaults_before_traversal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = loads(
        _dsl(
            [
                _node(
                    "source",
                    "template-transform",
                    template="unused",
                    variables=[],
                    error_strategy="default-value",
                    default_value=[
                        {"key": "output", "type": "string", "value": "fallback"}
                    ],
                ),
                _node("answer", "answer", answer="{{#source.output#}}"),
            ],
            [("start", "source"), ("source", "answer")],
        )
    )

    def fail() -> NodeRunResult:
        message = "failed source"
        raise ValueError(message)

    monkeypatch.setattr(engine.graph.nodes["source"], "_run", fail)
    events = list(engine.run())
    commits = [
        event
        for event in _events(events, "FrameVariablesUpdatedEvent")
        if _values(event).get(("source", "output")) == "fallback"
    ]
    assert len(commits) == 1
    traversal = next(
        event
        for event in _events(events, "GraphEdgeTakenEvent")
        if event.source_node_id == "source"
    )
    exception = _events(events, "NodeRunExceptionEvent")[0]
    assert events.index(commits[0]) < events.index(traversal) < events.index(exception)
    _assert_frame_lifetimes(events)


def test_loop_assignment_commits_each_changed_ancestor() -> None:
    document = json.loads(_loop_dsl())
    nodes = document["graph"]["nodes"]
    nodes[1]["data"]["container_id"] = "outer"
    nodes[3]["data"]["items"].append({
        "variable_selector": ["host", "counter"],
        "input_type": "constant",
        "operation": "+=",
        "value": 1,
    })
    nodes.extend([
        _node(
            "outer",
            "loop",
            start_node_id="outer-start",
            loop_count=1,
            break_conditions=[],
            logical_operator="and",
        ),
        _node("outer-start", "loop-start", container_id="outer"),
    ])
    document["graph"]["edges"][0]["target"] = "outer"
    document["graph"]["edges"].append({"source": "outer-start", "target": "loop"})
    engine = loads(json.dumps(document))
    engine.runtime_state.variable_pool.add(("host", "counter"), 0)
    events = list(engine.run())
    _assert_frame_lifetimes(events)
    starts = _events(events, "FrameStartedEvent")
    outer = next(event for event in starts if event.container_id == "outer")
    inner = [event for event in starts if event.container_id == "loop"]
    assert len(inner) == 2
    commits = _events(events, "FrameVariablesUpdatedEvent")
    for index, child in enumerate(inner, start=1):
        matching = [
            event
            for event in commits
            if _values(event).get(("host", "counter")) == index
        ]
        assert {event.frame_id for event in matching} == {
            "root",
            outer.frame_id,
            child.frame_id,
        }
        finish = next(
            event
            for event in _events(events, "FrameFinishedEvent")
            if event.frame_id == child.frame_id
        )
        assert all(events.index(event) < events.index(finish) for event in matching)


@pytest.mark.parametrize(
    ("outer_type", "inner_type"),
    [("loop", "iteration"), ("iteration", "loop"), ("iteration", "iteration")],
)
def test_iteration_host_writes_commit_each_changed_ancestor(
    outer_type: str, inner_type: str
) -> None:
    nodes = []
    for node_id, node_type, rounds in (
        ("outer", outer_type, 1),
        ("inner", inner_type, 2),
    ):
        settings: dict[str, Any] = (
            {"loop_count": rounds, "break_conditions": [], "logical_operator": "and"}
            if node_type == "loop"
            else {
                "iterator_selector": ["start", f"{node_id}_items"],
                "output_selector": ["host", "counter"],
                "flatten_output": False,
            }
        )
        if node_id == "inner":
            settings["container_id"] = "outer"
        nodes.extend([
            _node(node_id, node_type, start_node_id=f"{node_id}-start", **settings),
            _node(f"{node_id}-start", f"{node_type}-start", container_id=node_id),
        ])
    nodes.append(
        _node(
            "assign",
            "assigner",
            version="2",
            container_id="inner",
            items=[
                {
                    "variable_selector": ["host", "counter"],
                    "input_type": "constant",
                    "operation": "+=",
                    "value": 1,
                }
            ],
        )
    )
    engine = loads(
        _dsl(
            nodes,
            [("start", "outer"), ("outer-start", "inner"), ("inner-start", "assign")],
        ),
        start_inputs={"outer_items": [0], "inner_items": [0, 1]},
    )
    engine.runtime_state.variable_pool.add(("host", "counter"), 0)
    events = list(engine.run())
    _assert_frame_lifetimes(events)
    counter = engine.runtime_state.variable_pool.get_variable(("host", "counter"))
    assert counter is not None
    assert counter.value == 2
    starts = _events(events, "FrameStartedEvent")
    outer = next(event for event in starts if event.container_id == "outer")
    inner = [event for event in starts if event.container_id == "inner"]
    assert len(inner) == 2
    for index, child in enumerate(inner, start=1):
        assert _values(child)["host", "counter"] == index - 1
        commits = [
            event
            for event in _events(events, "FrameVariablesUpdatedEvent")
            if _values(event).get(("host", "counter")) == index
        ]
        assert len(commits) == 3
        assert {event.frame_id for event in commits} == {
            "root",
            outer.frame_id,
            child.frame_id,
        }
        assignment = next(
            event
            for event in _events(events, "NodeRunVariableUpdatedEvent")
            if event.frame_id == child.frame_id and event.node_id == "assign"
        )
        assert all(events.index(event) < events.index(assignment) for event in commits)


def test_partial_output_writes_report_the_committed_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = loads(
        _dsl(
            [_node("source", "template-transform", template="unused", variables=[])],
            [("start", "source")],
        )
    )
    shared = StringVariable(name="old", selector=["old", "value"], value="")
    engine.runtime_state.variable_pool.add(("host", "copy"), shared)

    def outputs() -> NodeRunResult:
        return NodeRunResult(
            status=WorkflowNodeExecutionStatus.SUCCEEDED,
            outputs={"renamed": shared, "null": None, "invalid": object()},
        )

    monkeypatch.setattr(engine.graph.nodes["source"], "_run", outputs)
    events: list[EngineEvent] = []
    with pytest.raises(ValueError, match="not supported value"):
        events.extend(engine.run())
    commits = _events(events, "FrameVariablesUpdatedEvent")
    assert any(_values(event).get(("source", "renamed")) == "" for event in commits)
    assert any(
        ("source", "null") in _values(event)
        and _values(event)["source", "null"] is None
        for event in commits
    )
    assert all(("source", "invalid") not in _values(event) for event in commits)
    assert all(("old", "value") not in _values(event) for event in commits)
    _assert_frame_lifetimes(events)
    assert _values(_events(events, "FrameStartedEvent")[0])["host", "copy"] == ""
    engine.runtime_state.variable_pool.add(("source", "renamed"), "later")
    assert all(_values(event).get(("source", "renamed"), "") == "" for event in commits)


def test_existing_custom_handler_delivers_child_events_without_optional_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CustomHandler:
        node_type = BuiltinNodeTypes.LOOP

        def __init__(self, registry: FrameRegistry) -> None:
            self.registry = registry

        def restore_frame(self, frame_state: ContainerFrameState) -> None:
            _ = frame_state
            message = "this fixture does not resume"
            raise AssertionError(message)

        def handle_request(
            self, *, invocation_id: str, request: ContainerAwaitRequest
        ) -> None:
            assert isinstance(request, LoopFrameRequest)
            root = self.registry["root"].state
            run = root.get_container_run(invocation_id)
            child = self.registry.create_child(
                frame_id="opaque-child",
                parent_frame_id=run.frame_id,
                container_id=run.node_id,
                root_node_id=request.root_node_id,
                variable_pool=self.registry[
                    run.frame_id
                ].state.variable_pool.model_copy(deep=True),
            )
            root.put_container_frame(
                CustomContainerFrameState(
                    frame_id=child.frame_id,
                    parent_invocation_id=invocation_id,
                    runtime_data=child.state.snapshot_frame(),
                )
            )
            child.scheduler.enqueue_node(request.root_node_id)

        def prepare_frame_event(
            self, *, frame: ExecutionFrame, event: NodeEvent
        ) -> None:
            _ = frame, event

        def should_emit(self, *, event: NodeEvent) -> bool:
            _ = event
            return True

        def record_frame_failure(
            self, *, frame: ExecutionFrame, event: NodeRunFailedEvent
        ) -> None:
            _ = frame
            raise AssertionError(event.error)

        def complete_frame_if_ready(self, frame: ExecutionFrame) -> None:
            if not frame.scheduler.is_execution_complete():
                return
            root = self.registry["root"].state
            state = root.pop_container_frame(frame.frame_id)
            root.enqueue_ready_task(
                ResumeTask(
                    invocation_id=state.parent_invocation_id,
                    result=ContainerExecutionResult(
                        metadata={},
                        steps=1,
                        node_run_result=ContainerNodeRunResult(
                            status=WorkflowNodeExecutionStatus.SUCCEEDED
                        ),
                    ),
                )
            )
            self.registry.remove(frame.frame_id)

    def stream(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        yield StreamChunkEvent(
            selector=["source", "output"], chunk="custom", is_final=True
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": "custom"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    engine = loads(
        _dsl(
            [
                _node(
                    "loop",
                    "loop",
                    start_node_id="loop-start",
                    loop_count=1,
                    break_conditions=[],
                    logical_operator="and",
                ),
                _node("loop-start", "loop-start", container_id="loop"),
                _node(
                    "source",
                    "template-transform",
                    container_id="loop",
                    template="unused",
                    variables=[],
                ),
            ],
            [("start", "loop"), ("loop-start", "source")],
        ),
        container_handler_factories=[CustomHandler],
    )
    events = list(engine.run())
    assert any(
        isinstance(event, NodeRunStreamChunkEvent) and event.chunk == "custom"
        for event in events
    )
    _assert_frame_lifetimes(events)
    child = next(
        event
        for event in _events(events, "FrameStartedEvent")
        if event.frame_id == "opaque-child"
    )
    assert child.parent_frame_id == "root"
    assert child.index is None
    assert child.root_node_id in {None, "loop-start"}
    assert child.invocation_id
    chunk = next(
        event for event in events if isinstance(event, NodeRunStreamChunkEvent)
    )
    assert chunk.frame_id == child.frame_id
    assert chunk.id == child.node_execution_ids["source"]


def test_parallel_raw_chunks_do_not_wait_for_an_earlier_item(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()
    first_started = Event()
    second_received = Event()

    def stream(node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        index = node.runtime_state.variable_pool.get(("iteration", "index"))
        assert index is not None
        if index.value == 0:
            first_started.set()
            assert release.wait(5)
        yield StreamChunkEvent(
            selector=["source", "output"], chunk=str(index.value), is_final=True
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": str(index.value)},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    engine = loads(
        _dsl(
            [
                _node(
                    "iteration",
                    "iteration",
                    start_node_id="item-start",
                    iterator_selector=["start", "items"],
                    output_selector=["source", "output"],
                    is_parallel=True,
                    parallel_nums=2,
                ),
                _node("item-start", "iteration-start", container_id="iteration"),
                _node(
                    "source",
                    "template-transform",
                    container_id="iteration",
                    template="unused",
                    variables=[],
                ),
            ],
            [("start", "iteration"), ("item-start", "source")],
        ),
        start_inputs={"items": ["a", "b"]},
        workers=2,
    )
    events: list[EngineEvent] = []
    errors: list[Exception] = []

    def consume() -> None:
        try:
            for event in engine.run():
                events.append(event)
                if isinstance(event, NodeRunStreamChunkEvent) and event.chunk == "1":
                    second_received.set()
        except Exception as error:  # ruff: ignore[blind-except]
            errors.append(error)

    consumer = Thread(target=consume, daemon=True)
    consumer.start()
    try:
        assert first_started.wait(3)
        assert second_received.wait(3), (
            "independent producer chunks must not be ordered by item"
        )
        assert not release.is_set()
    finally:
        release.set()
        consumer.join(5)
    assert not consumer.is_alive()
    assert not errors
    chunks = [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]
    assert [event.chunk for event in chunks] == ["1", "0"]
    _assert_frame_lifetimes(events)
    assert chunks[0].frame_id != chunks[1].frame_id
