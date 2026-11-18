"""Paired pause snapshots preserve presentation independently of engine frames."""

from __future__ import annotations

import json
from collections.abc import Generator
from dataclasses import replace
from threading import Event

import pytest

from graphon.dsl import loads
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import FrameFinishedEvent, FrameStartedEvent
from graphon.engine_events.graph import (
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunPauseRequestedEvent,
)
from graphon.entities.pause_reason import SchedulingPause
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import WorkflowNodeExecutionStatus
from graphon.node_events.base import NodeEventPayload, NodeRunResult
from graphon.node_events.node import (
    PauseRequestedEvent,
    StreamChunkEvent,
    StreamCompletedEvent,
)
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from graphon.runtime import RuntimeState
from graphon.variables.variables import FileVariable
from tests.engine.test_container_response_streaming import (
    _join_stream_text,
    _make_body_dsl,
    _make_edge_event,
    _make_filter_and_start,
    _make_source_chunk,
    _select_stream_chunks,
)
from tests.engine.test_file_runtime import _make_file, _make_file_adapter
from tests.engine.test_frame_observations import _node
from tests.engine.test_response_reply_ordering import (
    _copy_event_to_frame,
    _make_container_event,
    _make_item_frame,
)
from tests.engine.test_runtime_state_serialization import (
    _complete_loop_hitl,
    _hitl_engine,
    _new_runtime_state,
)


@pytest.mark.parametrize("rebuild", [False, True])
@pytest.mark.parametrize("active_sibling", [False, True])
def test_drained_parallel_pause_preserves_displayed_and_pending_replies(  # ruff: ignore[complex-structure, too-many-statements, too-many-locals]
    monkeypatch: pytest.MonkeyPatch, rebuild: bool, active_sibling: bool
) -> None:
    config = json.loads(
        _make_body_dsl(answer="[{{#source.output#}}/{{#source.output#}}]")
    )
    config["graph"]["nodes"][1]["data"].update(is_parallel=True, parallel_nums=2)
    dsl = json.dumps(config)
    phase = "pause"
    first_started, later_observed = Event(), Event()

    def stream(node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        item = node.runtime_state.variable_pool.get(("container", "index"))
        assert item is not None
        index = item.value
        paused_item = index == 0 or active_sibling
        if phase != "resume" or not paused_item:
            yield StreamChunkEvent(selector=["source", "output"], chunk=f"a{index}")
        if phase == "pause" and paused_item:
            if index == 0:
                first_started.set()
                assert later_observed.wait(5)
            else:
                assert first_started.wait(5)
            yield PauseRequestedEvent(
                reason=SchedulingPause(message="save reply progress")
            )
            return
        yield StreamChunkEvent(
            selector=["source", "output"], chunk=f"b{index}", is_final=True
        )
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": f"a{index}b{index}"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    engine = _hitl_engine(
        dsl,
        runtime_state=_new_runtime_state({"items": [0, 1]}),
        callback=_complete_loop_hitl,
    )
    response_filter = ResponseStreamFilter()
    raw: list[EngineEvent] = []
    starts: dict[str, FrameStartedEvent] = {}

    def record() -> Generator[EngineEvent]:
        for event in engine.run():
            raw.append(event)
            if isinstance(event, FrameStartedEvent):
                starts[event.frame_id] = event
            yield event
            if (
                isinstance(event, (FrameFinishedEvent, NodeRunPauseRequestedEvent))
                and starts[event.frame_id].index == 1
            ):
                later_observed.set()

    before = list(
        filter_engine_events(
            record(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[response_filter],
        )
    )
    assert isinstance(before[-1], GraphRunPausedEvent)
    assert _join_stream_text(before) == "[a0"
    assert any(chunk.chunk == "a1" for chunk in _select_stream_chunks(raw))
    remaining_frames = engine.runtime_state.container_frames()
    assert len(remaining_frames) == (2 if active_sibling else 1)
    if not active_sibling:
        later = next(start for start in starts.values() if start.index == 1)
        assert later.frame_id not in {frame.frame_id for frame in remaining_frames}
        assert any(
            isinstance(event, FrameFinishedEvent) and event.frame_id == later.frame_id
            for event in raw
        )

    # Both iterators have drained and engine execution is quiescent before either save.
    runtime_snapshot = engine.runtime_state.dumps()
    filter_snapshot = response_filter.dumps() if rebuild else None
    phase = "resume"
    if rebuild:
        engine = _hitl_engine(
            dsl,
            runtime_state=RuntimeState.from_snapshot(runtime_snapshot),
            callback=_complete_loop_hitl,
        )
        response_filter = ResponseStreamFilter()
        assert filter_snapshot is not None
        response_filter.loads(filter_snapshot)
    after = list(
        filter_engine_events(
            record(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[response_filter],
        )
    )
    assert isinstance(after[-1], GraphRunSucceededEvent)
    combined = before + after
    assert _join_stream_text(combined) == "[a0b0/a0b0][a1b1/a1b1]done"
    for start in starts.values():
        if start.parent_frame_id is None:
            continue
        chunks = [
            chunk
            for chunk in _select_stream_chunks(combined)
            if chunk.frame_id == start.frame_id
        ]
        assert {chunk.id for chunk in chunks} == {start.node_execution_ids["answer"]}
        assert all(chunk.selector == ["answer", "answer"] for chunk in chunks)
        assert sum(chunk.is_final for chunk in chunks) == 1
        assert chunks[-1].is_final

    phase = "uninterrupted"
    engine = _hitl_engine(
        dsl,
        runtime_state=_new_runtime_state({"items": [0, 1]}),
        callback=_complete_loop_hitl,
    )
    uninterrupted = list(
        filter_engine_events(
            engine.run(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[ResponseStreamFilter()],
        )
    )
    assert [
        (chunk.chunk, chunk.selector, chunk.is_final)
        for chunk in _select_stream_chunks(combined)
    ] == [
        (chunk.chunk, chunk.selector, chunk.is_final)
        for chunk in _select_stream_chunks(uninterrupted)
    ]


@pytest.mark.parametrize("rebuild", [False, True])
def test_pause_between_rounds_keeps_the_next_numeric_position(
    monkeypatch: pytest.MonkeyPatch, rebuild: bool
) -> None:

    config = json.loads(_make_body_dsl("loop"))
    config["graph"]["nodes"].append(
        _node("pause", "template-transform", template="unused", variables=[])
    )
    config["graph"]["edges"].append({"source": "start", "target": "pause"})
    dsl = json.dumps(config)
    answer_started, pause_observed, resumed = Event(), Event(), Event()

    def stream(node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        if not resumed.is_set() and node.id == "pause":
            assert answer_started.wait(5)
            yield PauseRequestedEvent(reason=SchedulingPause(message="between rounds"))
            return
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": "second" if resumed.is_set() else "first"},
            )
        )

    answer_run = AnswerNode._run

    def answer(node: AnswerNode) -> NodeRunResult:
        if node.id == "answer" and not resumed.is_set():
            answer_started.set()
            assert pause_observed.wait(5)
        return answer_run(node)

    monkeypatch.setattr(AnswerNode, "_run", answer)
    monkeypatch.setattr(TemplateTransformNode, "_run", stream)
    engine = _hitl_engine(
        dsl, runtime_state=_new_runtime_state({}), callback=_complete_loop_hitl
    )
    response_filter = ResponseStreamFilter()

    def record() -> Generator[EngineEvent]:
        for event in engine.run():
            yield event
            if isinstance(event, NodeRunPauseRequestedEvent):
                pause_observed.set()

    before = list(
        filter_engine_events(
            record(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[response_filter],
        )
    )
    assert isinstance(before[-1], GraphRunPausedEvent)
    assert _join_stream_text(before) == "result: first!"
    assert not engine.runtime_state.container_frames()
    runtime_snapshot = engine.runtime_state.dumps()
    filter_snapshot = response_filter.dumps() if rebuild else None
    resumed.set()
    if rebuild:
        engine = _hitl_engine(
            dsl,
            runtime_state=RuntimeState.from_snapshot(runtime_snapshot),
            callback=_complete_loop_hitl,
        )
        response_filter = ResponseStreamFilter()
        assert filter_snapshot is not None
        response_filter.loads(filter_snapshot)
    after = list(
        filter_engine_events(
            engine.run(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[response_filter],
        )
    )
    resumed_reply = next(
        chunk for chunk in _select_stream_chunks(after) if chunk.chunk == "second"
    )
    resumed_finish = next(
        event
        for event in after
        if isinstance(event, FrameFinishedEvent)
        and event.frame_id == resumed_reply.frame_id
    )
    assert after.index(resumed_reply) < after.index(resumed_finish)
    assert isinstance(after[-1], GraphRunSucceededEvent)
    assert _join_stream_text(before + after) == "result: first!result: second!done"
    assert (
        sum(
            chunk.is_final
            for chunk in _select_stream_chunks(before + after)
            if chunk.node_id == "answer"
        )
        == 2
    )


def test_reobserving_frames_and_initializing_keeps_progress_but_new_runs_reset() -> (
    None
):

    dsl = _make_body_dsl(answer="[{{#source.output#}}]")
    engine = loads(dsl)
    context = EngineEventFilterContext.from_engine(engine)
    response_filter = ResponseStreamFilter()
    _, start = _make_filter_and_start()
    before = list(
        filter_engine_events(
            [start, _make_source_chunk("old")],
            context=context,
            filters=[response_filter],
        )
    )
    response_filter.initialize(context)
    after = list(
        filter_engine_events(
            [start, _make_source_chunk("end", final=True)],
            context=context,
            filters=[response_filter],
        )
    )
    assert _join_stream_text(before + after) == "[oldend]"
    assert sum(chunk.is_final for chunk in _select_stream_chunks(before + after)) == 1
    new_context = EngineEventFilterContext.from_engine(loads(dsl))
    assert new_context.run_id != context.run_id
    fresh = list(
        filter_engine_events(
            [start, _make_source_chunk("new", final=True)],
            context=new_context,
            filters=[response_filter],
        )
    )
    assert _join_stream_text(fresh) == "[new]"


@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_legacy_resume_suppresses_child_responses_but_preserves_root(
    version: str,
) -> None:

    engine = loads(_make_body_dsl())
    response_filter = ResponseStreamFilter()
    response_filter.loads(
        json.dumps({
            "type": "ResponseStreamFilter",
            "version": version,
            "response_nodes": ["done"],
            "pending_sessions": [{"node_id": "done", "index": 0}],
            "paths_map": {"done": [["edge_1"]]},
        })
    )
    _, child = _make_filter_and_start()
    child = child.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
    next_child = child.model_copy(
        update={"frame_id": "next", "index": 1, "reason": WorkflowStartReason.INITIAL}
    )
    output = list(
        filter_engine_events(
            [
                GraphRunStartedEvent(reason=WorkflowStartReason.RESUMPTION),
                FrameStartedEvent(
                    frame_id="root", reason=WorkflowStartReason.RESUMPTION
                ),
                # Legacy frames have no recoverable child display history.
                FrameFinishedEvent(frame_id="completed-legacy", outcome="succeeded"),
                child,
                _make_source_chunk("ambiguous", final=True),
                FrameFinishedEvent(frame_id="child", outcome="succeeded"),
                next_child,
                _make_source_chunk("later", final=True).model_copy(
                    update={"frame_id": "next"}
                ),
                FrameFinishedEvent(frame_id="next", outcome="succeeded"),
                _make_container_event("root", "container", finished=True),
                _make_edge_event("container", "done", 1).model_copy(
                    update={"frame_id": "root"}
                ),
                GraphRunSucceededEvent(),
            ],
            context=EngineEventFilterContext.from_engine(engine),
            filters=[response_filter],
        )
    )
    assert _join_stream_text(output) == "done"
    assert any(
        isinstance(event, FrameFinishedEvent) and event.frame_id == "next"
        for event in output
    )


def test_mismatched_filter_progress_is_explicit_and_atomic() -> None:

    context = EngineEventFilterContext.from_engine(
        loads(_make_body_dsl(answer="[{{#source.output#}}]"))
    )
    response_filter = ResponseStreamFilter()
    _, child = _make_filter_and_start()
    before = list(
        filter_engine_events(
            [child, _make_source_chunk("kept")],
            context=context,
            filters=[response_filter],
        )
    )
    saved = response_filter.dumps()
    invalid = json.loads(saved)
    invalid["version"] = "4.99"
    with pytest.raises(ValueError, match="version"):
        response_filter.loads(json.dumps(invalid))
    assert response_filter.dumps() == saved
    invalid = json.loads(saved)
    invalid["run_id"] = "another-run"
    with pytest.raises(ValueError, match="run"):
        response_filter.loads(json.dumps(invalid))
    assert response_filter.dumps() == saved
    invalid = json.loads(saved)
    invalid["frames"]["child"]["active_session"]["node_id"] = "missing-answer"
    with pytest.raises(ValueError, match="response node"):
        response_filter.loads(json.dumps(invalid))
    assert response_filter.dumps() == saved
    restored_child = ResponseStreamFilter()
    restored_child.loads(saved)
    restored_child.initialize(context)
    restored_state = restored_child.dumps()
    for updates in (
        {"frame_id": "missing-child"},
        {
            "node_execution_ids": {
                **child.node_execution_ids,
                "answer": "other-answer-run",
            }
        },
        {"container_execution_id": "other-container-run"},
        {"container_id": "unknown-container"},
    ):
        invalid_start = child.model_copy(
            update={"reason": WorkflowStartReason.RESUMPTION, **updates}
        )
        with pytest.raises(ValueError, match="progress"):
            list(restored_child.on_event(invalid_start))
        assert restored_child.dumps() == restored_state
    assert (
        list(
            restored_child.on_event(
                child.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
            )
        )
        == []
    )
    restored_output = list(
        restored_child.on_event(_make_source_chunk("tail", final=True))
    )
    assert _join_stream_text(before + restored_output) == "[kepttail]"
    assert (
        sum(chunk.is_final for chunk in _select_stream_chunks(before + restored_output))
        == 1
    )
    assert _select_stream_chunks(restored_output)[-1].is_final
    output = list(response_filter.on_event(_make_source_chunk("tail", final=True)))
    assert _join_stream_text(before + output) == "[kepttail]"
    assert sum(chunk.is_final for chunk in _select_stream_chunks(before + output)) == 1
    assert _select_stream_chunks(output)[-1].is_final
    restored = ResponseStreamFilter()
    restored.loads(saved)
    with pytest.raises(ValueError, match="run"):
        restored.initialize(
            EngineEventFilterContext.from_engine(loads(_make_body_dsl()))
        )
    assert restored.dumps() == saved
    with pytest.raises(RuntimeError, match="initialized"):
        list(restored.flush())
    restored.initialize(context)
    continuation = list(restored.on_event(_make_source_chunk("tail", final=True)))
    assert _join_stream_text(before + continuation) == "[kepttail]"
    assert (
        sum(chunk.is_final for chunk in _select_stream_chunks(before + continuation))
        == 1
    )
    assert _select_stream_chunks(continuation)[-1].is_final


def test_resumption_requires_filter_progress() -> None:
    context = EngineEventFilterContext.from_engine(loads(_make_body_dsl()))
    missing = ResponseStreamFilter()
    with pytest.raises(ValueError, match="progress"):
        list(
            filter_engine_events(
                [GraphRunStartedEvent(reason=WorkflowStartReason.RESUMPTION)],
                context=context,
                filters=[missing],
            )
        )


def test_snapshot_keeps_container_reservation_before_first_child() -> None:

    config = json.loads(_make_body_dsl(answer="[{{#source.output#}}]"))
    config["graph"]["nodes"].append(_node("other", "answer", answer="later"))
    config["graph"]["edges"].append({"source": "container", "target": "other"})
    context = EngineEventFilterContext.from_engine(loads(json.dumps(config)))
    response_filter = ResponseStreamFilter()
    _, child = _make_filter_and_start()
    child = child.model_copy(
        update={"container_execution_id": "root-container", "index": 0}
    )
    before = list(
        filter_engine_events(
            [
                FrameStartedEvent(frame_id="root"),
                _make_container_event("root", "container"),
                _make_edge_event("container", "other", 4).model_copy(
                    update={"frame_id": "root"}
                ),
            ],
            context=context,
            filters=[response_filter],
        )
    )
    assert _join_stream_text(before) == ""
    restored = ResponseStreamFilter()
    restored.loads(response_filter.dumps())
    after = list(
        filter_engine_events(
            [
                child,
                _make_source_chunk("child", final=True),
                FrameFinishedEvent(frame_id="child", outcome="succeeded"),
                _make_container_event("root", "container", finished=True),
                _make_edge_event("container", "done", 1).model_copy(
                    update={"frame_id": "root"}
                ),
                GraphRunSucceededEvent(),
            ],
            context=context,
            filters=[restored],
        )
    )
    assert _join_stream_text(after) == "[child]laterdone"


def test_snapshot_rebinds_file_adapter_for_unread_detached_values() -> None:

    context = EngineEventFilterContext.from_engine(
        loads(_make_body_dsl(answer="[{{#source.output#}}]{{#sys.file#}}"))
    )
    context = replace(context, file_runtime=_make_file_adapter("before"))
    _, child = _make_filter_and_start()
    child = child.model_copy(
        update={
            "variables": (
                FileVariable(name="file", selector=["sys", "file"], value=_make_file()),
            )
        }
    )
    response_filter = ResponseStreamFilter()
    before = list(
        filter_engine_events(
            [child, _make_source_chunk("partial")],
            context=context,
            filters=[response_filter],
        )
    )
    restored = ResponseStreamFilter()
    restored.loads(response_filter.dumps())
    after = list(
        filter_engine_events(
            [_make_source_chunk("end", final=True)],
            context=replace(context, file_runtime=_make_file_adapter("restored")),
            filters=[restored],
        )
    )
    assert (
        _join_stream_text(before + after)
        == "[partialend][report.txt](https://restored.example/report.txt)"
    )
    assert sum(chunk.is_final for chunk in _select_stream_chunks(before + after)) == 1


@pytest.mark.parametrize("repeat_before_drain", [False, True])
def test_repeated_finished_frame_observation_keeps_queued_reply(
    repeat_before_drain: bool,
) -> None:
    context = EngineEventFilterContext.from_engine(
        loads(_make_body_dsl(answer="[{{#source.output#}}]"))
    )
    stream_filter, start = _make_filter_and_start(context=context)
    first, second = _make_item_frame(start, 0), _make_item_frame(start, 1)
    container = _make_container_event("root", "container").model_copy(
        update={"id": start.container_execution_id}
    )
    before = [
        *stream_filter.on_event(FrameStartedEvent(frame_id="root")),
        *stream_filter.on_event(container),
        *stream_filter.on_event(first),
        *stream_filter.on_event(second),
    ]
    before.extend(
        stream_filter.on_event(_copy_event_to_frame(_make_source_chunk("0"), first))
    )
    before.extend(
        stream_filter.on_event(
            _copy_event_to_frame(_make_source_chunk("1", final=True), second)
        )
    )
    before.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id=second.frame_id, outcome="succeeded")
        )
    )
    assert _join_stream_text(before) == "[0"
    restored = ResponseStreamFilter()
    restored.loads(stream_filter.dumps())
    restored.initialize(context)
    if repeat_before_drain:
        assert list(restored.on_event(second)) == []
    after = list(
        restored.on_event(
            _copy_event_to_frame(_make_source_chunk("", final=True), first)
        )
    )
    after.extend(
        restored.on_event(
            FrameFinishedEvent(frame_id=first.frame_id, outcome="succeeded")
        )
    )
    if not repeat_before_drain:
        assert list(restored.on_event(second)) == []
    after.extend(
        restored.on_event(
            _make_container_event("root", "container", finished=True).model_copy(
                update={"id": start.container_execution_id}
            )
        )
    )
    after.extend(
        restored.on_event(
            _make_edge_event("container", "done", 1).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(before + after) == "[0][1]done"
    after.extend(restored.on_event(GraphRunSucceededEvent()))
    assert _join_stream_text(before + after) == "[0][1]done"
    assert (
        sum(
            chunk.is_final
            for chunk in _select_stream_chunks(before + after)
            if chunk.node_id == "answer"
        )
        == 2
    )
    assert list(restored.flush()) == []
