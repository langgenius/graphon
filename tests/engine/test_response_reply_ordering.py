"""Presentation order must not constrain raw container execution."""

from __future__ import annotations

import json
from collections.abc import Generator
from datetime import UTC, datetime
from queue import Queue
from threading import Event, Thread
from typing import Any

import pytest

from graphon.dsl import loads
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent, NodeEvent
from graphon.engine_events.frame import (
    FrameFinishedEvent,
    FrameStartedEvent,
    FrameVariablesUpdatedEvent,
)
from graphon.engine_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunPartialSucceededEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunFailedEvent,
    NodeRunRetryEvent,
    NodeRunStartedEvent,
    NodeRunSucceededEvent,
)
from graphon.engine_events.traversal import GraphEdgeTakenEvent
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeEventPayload, NodeRunResult
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from tests.engine.test_container_response_streaming import (
    _join_stream_text,
    _make_body_dsl,
    _make_edge_event,
    _make_filter_and_start,
    _make_source_chunk,
    _make_source_completion,
    _make_variable_commit,
    _select_stream_chunks,
)
from tests.engine.test_frame_observations import _dsl, _node


def _make_container_event(
    frame_id: str, node_id: str, *, finished: bool = False
) -> EngineEvent:
    fields: dict[str, Any] = {
        "id": f"{frame_id}-{node_id}",
        "frame_id": frame_id,
        "node_id": node_id,
        "node_type": BuiltinNodeTypes.ITERATION,
        "start_at": datetime.now(UTC),
    }
    if finished:
        return NodeRunSucceededEvent(**fields)
    return NodeRunStartedEvent(**fields, node_title=node_id)


def _make_item_frame(start: FrameStartedEvent, index: int) -> FrameStartedEvent:
    return start.model_copy(
        update={
            "frame_id": f"item-{index}",
            "index": index,
            "node_execution_ids": {
                node: f"item-{index}-{node}" for node in start.node_execution_ids
            },
        }
    )


def _copy_event_to_frame(event: EngineEvent, frame: FrameStartedEvent) -> EngineEvent:
    updates: dict[str, object] = {"frame_id": frame.frame_id}
    if isinstance(event, NodeEvent):
        updates["id"] = frame.node_execution_ids[event.node_id]
    return event.model_copy(update=updates)


def test_parallel_item_stays_live_while_later_item_finishes(  # ruff: ignore[complex-structure]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = json.loads(_make_body_dsl(second_answer="next:{{#source.output#}};"))
    config["graph"]["nodes"][1]["data"].update(is_parallel=True, parallel_nums=2)
    engine = loads(json.dumps(config), start_inputs={"items": [0, 1]}, workers=2)
    release_first, first_visible, later_finished = Event(), Event(), Event()

    def stream(node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        index = node.runtime_state.variable_pool.get(("container", "index"))
        assert index is not None
        yield StreamChunkEvent(
            selector=["source", "private"], chunk="secret", is_final=True
        )
        yield StreamChunkEvent(selector=["source", "output"], chunk=str(index.value))
        if index.value == 0:
            assert release_first.wait(10)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": str(index.value)},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    raw: list[EngineEvent] = []
    output: list[EngineEvent] = []
    errors: list[Exception] = []
    frames: dict[str, int | None] = {}

    def record() -> Generator[EngineEvent]:
        for event in engine.run():
            raw.append(event)
            if isinstance(event, FrameStartedEvent):
                frames[event.frame_id] = event.index
            yield event
            if isinstance(event, FrameFinishedEvent) and frames[event.frame_id] == 1:
                later_finished.set()

    def consume() -> None:
        try:
            for event in filter_engine_events(
                record(),
                filters=[ResponseStreamFilter()],
                context=EngineEventFilterContext.from_engine(engine),
            ):
                output.append(event)
                if (
                    _join_stream_text(
                        item
                        for item in output
                        if getattr(item, "frame_id", None) in frames
                        and frames[getattr(item, "frame_id", "")] == 0
                    )
                    == "result: 0"
                ):
                    first_visible.set()
        except Exception as error:  # ruff: ignore[blind-except]
            errors.append(error)

    consumer = Thread(target=consume, daemon=True)
    consumer.start()
    try:
        assert first_visible.wait(3), "item 0 must stream while its producer is gated"
        assert later_finished.wait(3), (
            "item 1 raw events must reach the filter while item 0 is gated"
        )
        assert _join_stream_text(output) == "result: 0"
        assert any(event.chunk == "1" for event in _select_stream_chunks(raw))
    finally:
        release_first.set()
        consumer.join(10)
    assert not consumer.is_alive()
    assert not errors
    assert _join_stream_text(output) == "result: 0!next:0;result: 1!next:1;done"
    assert all(event.node_id == "source" for event in _select_stream_chunks(raw))
    result = next(
        event
        for event in output
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "container"
    )
    assert result.node_run_result.outputs["output"].value == [
        "result: 0!",
        "result: 1!",
    ]
    assert not any(
        isinstance(event, (FrameStartedEvent, FrameVariablesUpdatedEvent))
        for event in output
    )
    assert isinstance(output[-1], GraphRunSucceededEvent)


def test_two_replies_and_frame_completion_precede_next_item() -> None:
    stream_filter, start = _make_filter_and_start(
        second_answer="next:{{#source.output#}};"
    )
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("1", final=True), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_edge_event("answer", "second", 4), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("0", final=True), first)
        )
    )
    assert _join_stream_text(output) == "[0]"
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_edge_event("answer", "second", 4), first)
        )
    )
    assert _join_stream_text(output) == "[0]next:0;"
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == "[0]next:0;[1]next:1;"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 4


def test_active_root_reply_stays_contiguous_before_child_and_following_root_reply() -> (
    None
):
    config = json.loads(_make_body_dsl())
    config["graph"]["nodes"].extend([
        _node("root-source", "template-transform", template="unused", variables=[]),
        _node("intro", "answer", answer="root:{{#root-source.output#}}!"),
    ])
    config["graph"]["edges"].extend([
        {"source": "start", "target": "root-source"},
        {"source": "root-source", "target": "intro"},
    ])
    engine = loads(json.dumps(config))
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(EngineEventFilterContext.from_engine(engine))
    _, start = _make_filter_and_start()
    start = start.model_copy(
        update={"container_execution_id": "root-container", "index": 0}
    )
    output = list(
        stream_filter.on_event(
            FrameStartedEvent(
                frame_id="root",
                node_execution_ids={
                    "intro": "intro-run",
                    "root-source": "root-source-run",
                },
            )
        )
    )
    output.extend(stream_filter.on_event(_make_container_event("root", "container")))
    output.extend(stream_filter.on_event(start))
    output.extend(stream_filter.on_event(_make_source_chunk("child", final=True)))
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id="child", outcome="succeeded")
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_container_event("root", "container", finished=True)
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_edge_event("container", "done", 1).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "root:"
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("live", final=True).model_copy(
                update={
                    "frame_id": "root",
                    "node_id": "root-source",
                    "id": "root-source-run",
                    "selector": ["root-source", "output"],
                }
            )
        )
    )
    assert _join_stream_text(output) == "root:live!result: child!done"
    assert list(stream_filter.flush()) == []


def _make_nested_filter(
    *, stream_parent: bool = False, fail_branch: bool = False
) -> ResponseStreamFilter:
    engine = loads(
        _dsl(
            [
                _node(
                    "outer",
                    "iteration",
                    start_node_id="outer-start",
                    iterator_selector=["start", "items"],
                    output_selector=["parent-answer", "answer"],
                ),
                _node("outer-start", "iteration-start", container_id="outer"),
                _node(
                    "inner",
                    "iteration",
                    container_id="outer",
                    start_node_id="inner-start",
                    iterator_selector=["start", "items"],
                    output_selector=["inner-answer", "answer"],
                    error_strategy="fail-branch" if fail_branch else None,
                ),
                _node(
                    "gate",
                    "template-transform",
                    container_id="outer",
                    template="unused",
                    variables=[],
                )
                if stream_parent
                else _node("gate", "if-else", container_id="outer", cases=[]),
                _node(
                    "parent-answer",
                    "answer",
                    container_id="outer",
                    answer="parent:{{#gate.output#}};" if stream_parent else "parent;",
                ),
                _node("inner-start", "iteration-start", container_id="inner"),
                _node(
                    "source",
                    "template-transform",
                    container_id="inner",
                    template="unused",
                    variables=[],
                ),
                _node(
                    "inner-answer",
                    "answer",
                    container_id="inner",
                    answer="[{{#source.output#}}]",
                ),
                _node("done", "answer", answer="done"),
            ],
            [
                ("start", "outer"),
                ("outer", "done"),
                ("outer-start", "inner"),
                ("outer-start", "gate"),
                ("gate", "parent-answer"),
                ("inner-start", "source"),
                ("source", "inner-answer"),
            ],
        )
    )
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(EngineEventFilterContext.from_engine(engine))
    list(
        stream_filter.on_event(
            FrameStartedEvent(
                frame_id="root",
                node_execution_ids={"outer": "root-outer", "done": "root-done"},
            )
        )
    )
    return stream_filter


def test_streaming_child_ancestor_finishes_reply_before_ready_descendant() -> None:
    stream_filter = _make_nested_filter(stream_parent=True)
    output = list(stream_filter.on_event(_make_container_event("root", "outer")))
    parent = FrameStartedEvent(
        frame_id="parent",
        parent_frame_id="root",
        invocation_id="outer-run",
        container_execution_id="root-outer",
        container_id="outer",
        root_node_id="outer-start",
        index=0,
        node_execution_ids={
            node: f"parent-{node}"
            for node in ["outer-start", "inner", "gate", "parent-answer"]
        },
    )
    child = FrameStartedEvent(
        frame_id="descendant",
        parent_frame_id="parent",
        invocation_id="inner-run",
        container_execution_id="parent-inner",
        container_id="inner",
        root_node_id="inner-start",
        index=0,
        node_execution_ids={
            node: f"descendant-{node}"
            for node in ["inner-start", "source", "inner-answer"]
        },
    )
    parent_chunk = _make_source_chunk("live").model_copy(
        update={
            "frame_id": "parent",
            "node_id": "gate",
            "id": "parent-gate",
            "selector": ["gate", "output"],
        }
    )
    output.extend(stream_filter.on_event(parent))
    output.extend(stream_filter.on_event(parent_chunk))
    output.extend(stream_filter.on_event(_make_container_event("parent", "inner")))
    output.extend(stream_filter.on_event(child))
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("child", final=True), child)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=child.frame_id, outcome="succeeded")
        )
    )
    output.extend(
        stream_filter.on_event(_make_container_event("parent", "inner", finished=True))
    )
    assert _join_stream_text(output) == "parent:live"
    output.extend(
        stream_filter.on_event(
            parent_chunk.model_copy(update={"chunk": " end", "is_final": True})
        )
    )
    assert _join_stream_text(output) == "parent:live end;[child]"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 2


@pytest.mark.parametrize("fail_branch", [False, True])
def test_nested_group_reserves_position_before_delayed_first_child_reply(
    fail_branch: bool,
) -> None:
    stream_filter = _make_nested_filter(fail_branch=fail_branch)
    output = list(stream_filter.on_event(_make_container_event("root", "outer")))
    parents = [
        FrameStartedEvent(
            frame_id=f"outer-{index}",
            parent_frame_id="root",
            invocation_id="outer-run",
            container_execution_id="root-outer",
            container_id="outer",
            root_node_id="outer-start",
            index=index,
            node_execution_ids={
                node: f"outer-{index}-{node}"
                for node in ["outer-start", "inner", "gate", "parent-answer"]
            },
        )
        for index in range(2)
    ]
    children = [
        FrameStartedEvent(
            frame_id=f"inner-{index}",
            parent_frame_id=parent.frame_id,
            invocation_id=f"inner-run-{index}",
            container_execution_id=f"{parent.frame_id}-inner",
            container_id="inner",
            root_node_id="inner-start",
            index=0,
            node_execution_ids={
                node: f"inner-{index}-{node}"
                for node in ["inner-start", "source", "inner-answer"]
            },
        )
        for index, parent in enumerate(parents)
    ]
    for parent in parents:
        output.extend(stream_filter.on_event(parent))
        output.extend(
            stream_filter.on_event(_make_container_event(parent.frame_id, "inner"))
        )
        output.extend(
            stream_filter.on_event(
                GraphEdgeTakenEvent(
                    frame_id=parent.frame_id,
                    edge_id="edge_4",
                    source_node_id="gate",
                    target_node_id="parent-answer",
                )
            )
        )
    assert _join_stream_text(output) == "", (
        "container start reserves a position even before its child starts"
    )
    for index in [1, 0]:
        child, parent = children[index], parents[index]
        output.extend(stream_filter.on_event(child))
        output.extend(
            stream_filter.on_event(
                _copy_event_to_frame(_make_source_chunk(str(index), final=True), child)
            )
        )
        output.extend(
            stream_filter.on_event(
                FrameFinishedEvent(frame_id=child.frame_id, outcome="succeeded")
            )
        )
        if index == 0:
            assert _join_stream_text(output) == "[0]", (
                "parent reply must wait for the inner container to complete"
            )
        else:
            assert _join_stream_text(output) == ""
        output.extend(
            stream_filter.on_event(
                _make_container_event(parent.frame_id, "inner", finished=True)
            )
        )
        output.extend(
            stream_filter.on_event(
                FrameFinishedEvent(frame_id=parent.frame_id, outcome="succeeded")
            )
        )
    output.extend(
        stream_filter.on_event(_make_container_event("root", "outer", finished=True))
    )
    output.extend(
        stream_filter.on_event(
            GraphEdgeTakenEvent(
                frame_id="root",
                edge_id="edge_1",
                source_node_id="outer",
                target_node_id="done",
            )
        )
    )
    assert _join_stream_text(output) == "[0]parent;[1]parent;done"
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("skipped", [False, True])
def test_frame_with_no_reached_reply_releases_waiting_item_only_at_finish(
    skipped: bool,
) -> None:
    stream_filter, start = _make_filter_and_start(branch=True)
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("1", final=True), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_edge_event("branch", "answer", 4), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    if skipped:
        output.extend(
            stream_filter.on_event(
                _copy_event_to_frame(
                    _make_edge_event("branch", "answer", 4, skipped=True), first
                )
            )
        )
    assert _join_stream_text(output) == ""
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == "[1]"


@pytest.mark.parametrize("items", [[], [0, 1]])
def test_single_worker_and_bounded_consumer_queue_finish(items: list[int]) -> None:
    config = json.loads(_make_body_dsl(answer="{{#container.index#}};"))
    config["graph"]["nodes"][1]["data"].update(is_parallel=True, parallel_nums=2)
    engine = loads(json.dumps(config), start_inputs={"items": items}, workers=1)
    queue: Queue[EngineEvent | Exception | None] = Queue(maxsize=1)

    def produce() -> None:
        try:
            for event in engine.run():
                queue.put(event, timeout=5)
        except Exception as error:  # ruff: ignore[blind-except]
            queue.put(error, timeout=5)
        finally:
            queue.put(None, timeout=5)

    def consume() -> Generator[EngineEvent]:
        while (event := queue.get(timeout=5)) is not None:
            if isinstance(event, Exception):
                raise event
            yield event

    producer = Thread(target=produce, daemon=True)
    producer.start()
    output = list(
        filter_engine_events(
            consume(),
            filters=[ResponseStreamFilter()],
            context=EngineEventFilterContext.from_engine(engine),
        )
    )
    producer.join(5)
    assert not producer.is_alive()
    assert _join_stream_text(output) == ("0;1;done" if items else "done")
    assert isinstance(output[-1], GraphRunSucceededEvent)


@pytest.mark.parametrize(
    "terminal", [GraphRunFailedEvent(error="failed"), GraphRunAbortedEvent()]
)
def test_terminal_discards_waiting_replies_and_closes_only_displayed_partial(
    terminal: EngineEvent,
) -> None:
    stream_filter, start = _make_filter_and_start()
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("partial"), first)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("hidden", final=True), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    output.extend(stream_filter.on_event(terminal))
    assert _join_stream_text(output) == "[partial"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1
    assert output[-1] is terminal
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("streamed", [False, True])
def test_handled_failure_behind_prior_item_drains_before_partial_success(
    streamed: bool,
) -> None:
    stream_filter, start = _make_filter_and_start()
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    if streamed:
        output.extend(
            stream_filter.on_event(
                _copy_event_to_frame(_make_source_chunk("partial"), second)
            )
        )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_variable_commit("default"), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_completion(failure=True), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("0", final=True), first)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    terminal = GraphRunPartialSucceededEvent(exceptions_count=1)
    output.extend(stream_filter.on_event(terminal))
    assert _join_stream_text(output) == ("[0][partial]" if streamed else "[0][default]")
    assert output[-1] is terminal
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 2
    assert list(stream_filter.flush()) == []


def test_retry_before_reply_is_reached_does_not_replay_failed_attempt() -> None:
    stream_filter, start = _make_filter_and_start(branch=True)
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(_copy_event_to_frame(_make_source_chunk("old"), second))
    )
    retry = NodeRunRetryEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
        retry_index=1,
        error="retry",
        node_title="source",
        start_at=datetime.now(UTC),
    )
    output.extend(stream_filter.on_event(_copy_event_to_frame(retry, second)))
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(
                _make_source_chunk("new", attempt=1, final=True), second
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_edge_event("branch", "answer", 4), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == ""
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == "[new]"


@pytest.mark.parametrize(
    "terminal",
    [GraphRunSucceededEvent(), GraphRunPartialSucceededEvent(exceptions_count=1)],
)
def test_success_terminal_drains_unfinished_sources_before_success(
    terminal: EngineEvent,
) -> None:
    stream_filter, start = _make_filter_and_start()
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(_copy_event_to_frame(_make_source_chunk("1"), second))
    )
    output.extend(
        stream_filter.on_event(_copy_event_to_frame(_make_source_chunk("0"), first))
    )
    output.extend(stream_filter.on_event(terminal))
    assert _join_stream_text(output) == "[0][1]"
    assert output[-1] is terminal
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 2
    assert list(stream_filter.flush()) == []


def test_item_indices_are_numeric_even_when_frames_arrive_out_of_order() -> None:
    stream_filter, start = _make_filter_and_start()
    frames = [_make_item_frame(start, index) for index in range(12)]
    output: list[EngineEvent] = []
    for frame in reversed(frames):
        output.extend(stream_filter.on_event(frame))
        output.extend(
            stream_filter.on_event(
                _copy_event_to_frame(
                    _make_source_chunk(str(frame.index), final=True), frame
                )
            )
        )
        output.extend(
            stream_filter.on_event(
                FrameFinishedEvent(frame_id=frame.frame_id, outcome="succeeded")
            )
        )
        if frame.index != 0:
            assert _join_stream_text(output) == ""
    assert _join_stream_text(output) == "".join(f"[{index}]" for index in range(12))


@pytest.mark.parametrize("aborted", [False, True])
def test_failed_frame_closures_do_not_display_buffered_partial_before_iterator_raises(
    aborted: bool,
) -> None:
    engine = loads(_make_body_dsl(answer="[{{#source.output#}}]"))
    _, start = _make_filter_and_start()
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    terminal = (
        GraphRunAbortedEvent() if aborted else GraphRunFailedEvent(error="failed")
    )

    def events() -> Generator[EngineEvent]:
        yield FrameStartedEvent(frame_id="root")
        yield first
        yield second
        yield _copy_event_to_frame(_make_source_chunk("partial"), first)
        yield _copy_event_to_frame(_make_source_chunk("hidden"), second)
        for frame_id in [second.frame_id, first.frame_id, "root"]:
            yield FrameFinishedEvent(
                frame_id=frame_id, outcome="aborted" if aborted else "failed"
            )
        yield terminal
        if not aborted:
            message = "execution failed after terminal event"
            raise RuntimeError(message)

    stream_filter = ResponseStreamFilter()
    output: list[EngineEvent] = []
    filtered = filter_engine_events(
        events(),
        filters=[stream_filter],
        context=EngineEventFilterContext.from_engine(engine),
    )
    if aborted:
        output.extend(filtered)
    else:
        with pytest.raises(RuntimeError, match="execution failed after terminal event"):
            output.extend(filtered)
    assert _join_stream_text(output) == "[partial"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1
    assert output[-1] is terminal
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("failed", [False, True])
def test_failed_item_waits_for_container_outcome_before_releasing_finished_sibling(
    failed: bool,
) -> None:
    stream_filter, start = _make_filter_and_start()
    list(
        stream_filter.on_event(
            _make_container_event("root", "container").model_copy(
                update={"id": start.container_execution_id}
            )
        )
    )
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("partial"), first)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("hidden", final=True), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="failed")
        )
    )
    assert _join_stream_text(output) == "[partial"
    container_result = (
        NodeRunFailedEvent(
            id=start.container_execution_id or "container-run",
            frame_id="root",
            node_id="container",
            node_type=BuiltinNodeTypes.ITERATION,
            start_at=datetime.now(UTC),
            error="failed item",
        )
        if failed
        else _make_container_event("root", "container", finished=True).model_copy(
            update={"id": start.container_execution_id}
        )
    )
    output.extend(stream_filter.on_event(container_result))
    assert _join_stream_text(output) == ("[partial" if failed else "[partial[hidden]")
    terminal = (
        GraphRunFailedEvent(error="failed")
        if failed
        else GraphRunPartialSucceededEvent(exceptions_count=1)
    )
    output.extend(stream_filter.on_event(terminal))
    assert _join_stream_text(output) == ("[partial" if failed else "[partial[hidden]")
    assert output[-1] is terminal
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("source_final", [False, True])
def test_retry_replaces_hidden_attempt_without_changing_other_occurrences(
    source_final: bool,
) -> None:
    stream_filter, start = _make_filter_and_start(
        answer="{{#sys.value#}}:{{#source.output#}}"
    )
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    output = [*stream_filter.on_event(first), *stream_filter.on_event(second)]
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(
                _make_variable_commit("before", selector=("sys", "value")), second
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("old", final=source_final), second)
        )
    )
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(
                _make_variable_commit("after", selector=("sys", "value")), second
            )
        )
    )
    retry = NodeRunRetryEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
        retry_index=1,
        error="retry",
        node_title="source",
        start_at=datetime.now(UTC),
    )
    output.extend(stream_filter.on_event(_copy_event_to_frame(retry, second)))
    output.extend(
        stream_filter.on_event(
            _copy_event_to_frame(
                _make_source_chunk("new", attempt=1, final=True), second
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == ""
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == "before:new"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1


def test_root_snapshot_stays_live_beside_container_without_child_responses() -> None:
    config = json.loads(_make_body_dsl())
    answer = next(node for node in config["graph"]["nodes"] if node["id"] == "answer")
    answer["data"] = {
        "type": "template-transform",
        "container_id": "container",
        "template": "unused",
        "variables": [],
    }
    config["graph"]["nodes"].extend([
        _node("gate", "if-else", cases=[]),
        _node("other", "answer", answer="root saved"),
    ])
    config["graph"]["edges"].extend([
        {"source": "start", "target": "gate"},
        {"source": "gate", "target": "other"},
    ])
    engine = loads(json.dumps(config))
    context = EngineEventFilterContext.from_engine(engine)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(context)
    _, start = _make_filter_and_start()
    start = start.model_copy(update={"container_execution_id": "root-container"})
    output = list(stream_filter.on_event(FrameStartedEvent(frame_id="root")))
    output.extend(stream_filter.on_event(_make_container_event("root", "container")))
    output.extend(stream_filter.on_event(start))
    output.extend(
        stream_filter.on_event(
            GraphEdgeTakenEvent(
                frame_id="root",
                edge_id="edge_5",
                source_node_id="gate",
                target_node_id="other",
            )
        )
    )
    assert _join_stream_text(output) == "root saved"
    restored = ResponseStreamFilter()
    restored.loads(stream_filter.dumps())
    restored.initialize(context)
    output.extend(
        restored.on_event(
            FrameStartedEvent(frame_id="root", reason=WorkflowStartReason.RESUMPTION)
        )
    )
    output.extend(
        restored.on_event(
            start.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
        )
    )
    output.extend(
        restored.on_event(
            FrameFinishedEvent(frame_id=start.frame_id, outcome="succeeded")
        )
    )
    output.extend(
        restored.on_event(_make_container_event("root", "container", finished=True))
    )
    output.extend(
        restored.on_event(
            _make_edge_event("container", "done", 1).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    output.extend(restored.on_event(GraphRunSucceededEvent()))
    assert _join_stream_text(output) == "root saveddone"


def test_container_reservation_snapshot_is_rejected_until_empty_group_finishes() -> (
    None
):
    stream_filter, _ = _make_filter_and_start()
    snapshot = stream_filter.dumps()
    list(stream_filter.on_event(_make_container_event("root", "container")))
    with pytest.raises(ValueError, match="child"):
        stream_filter.dumps()
    with pytest.raises(ValueError, match="child"):
        stream_filter.loads(snapshot)
    list(
        stream_filter.on_event(
            _make_container_event("root", "container", finished=True)
        )
    )
    assert stream_filter.dumps()


def test_root_snapshot_after_zero_item_container_keeps_active_and_waiting_replies() -> (
    None
):
    config = json.loads(_make_body_dsl())
    config["graph"]["nodes"].append(
        _node("intro", "answer", answer="root:{{#sys.tail#}}")
    )
    config["graph"]["edges"].append({"source": "start", "target": "intro"})
    engine = loads(json.dumps(config))
    context = EngineEventFilterContext.from_engine(engine)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(context)
    output = list(stream_filter.on_event(FrameStartedEvent(frame_id="root")))
    output.extend(stream_filter.on_event(_make_container_event("root", "container")))
    output.extend(
        stream_filter.on_event(
            _make_container_event("root", "container", finished=True)
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_edge_event("container", "done", 1).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "root:"
    restored = ResponseStreamFilter()
    restored.loads(stream_filter.dumps())
    restored.initialize(context)
    output.extend(
        restored.on_event(
            FrameStartedEvent(frame_id="root", reason=WorkflowStartReason.RESUMPTION)
        )
    )
    output.extend(
        restored.on_event(
            _make_variable_commit("tail", selector=("sys", "tail")).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "root:taildone"
