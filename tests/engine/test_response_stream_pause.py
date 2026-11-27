"""Resume paired historical runtime/filter snapshots across real root pauses.

Fixtures were captured by the actual c3870df and 069cd55 writers, respectively
runtime v2/filter v1 and runtime v3/filter v2. The generator is
tests/helpers/generate_response_pause_fixtures.py; existing fixture bytes must
not be regenerated from the current implementation or relabeled JSON.
"""

from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel

from graphon.engine import Engine
from graphon.engine.command import PauseCommand
from graphon.engine.command.builtin.in_memory import InMemoryChannel
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import (
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.entities.pause_reason import HitlRequired, SchedulingPause  # ty: ignore[deprecated]
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.nodes.human_input.entities import (
    Completed,
    HITLCallback,
    HITLContext,
    PauseRequested,
)
from graphon.runtime.runtime_state import RuntimeState
from tests.engine.test_runtime_state_serialization import (
    _completed_hitl,
    _hitl_engine,
    _new_runtime_state,
)
from tests.helpers.workflow_events import final_outputs

_FIXTURES = Path(__file__).with_name("fixtures")
_DSL = (_FIXTURES / "response_pause_workflow.yml").read_text()
_COMPLETE_TEXT = "before|alpha|middle|beta|after|tail"


class _SavedChunk(BaseModel):
    """Root stream data; synthetic presentation IDs are not a resume contract."""

    node_id: str
    selector: list[str]
    chunk: str
    is_final: bool


class _SavedPause(BaseModel):
    runtime: str
    response_filter: str
    chunks: list[_SavedChunk]
    pause: GraphRunPausedEvent
    started_nodes: dict[str, str]
    completed_nodes: list[str]


class _PauseCheckpoints(BaseModel):
    writer_commit: str
    first: _SavedPause
    second: _SavedPause
    command: _SavedPause


def _collect_stream_chunks(events: list[EngineEvent]) -> list[_SavedChunk]:
    return [
        _SavedChunk.model_validate(event.model_dump())
        for event in events
        if isinstance(event, NodeRunStreamChunkEvent)
    ]


def _run_with_response_filter(
    engine: Engine, stream_filter: ResponseStreamFilter
) -> list[EngineEvent]:
    return list(
        filter_engine_events(
            engine.run(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[stream_filter],
        )
    )


def _capture_pause(engine: Engine, stream_filter: ResponseStreamFilter) -> _SavedPause:
    events = _run_with_response_filter(engine, stream_filter)
    assert isinstance(events[-1], GraphRunPausedEvent)
    return _SavedPause(
        completed_nodes=[
            event.node_id
            for event in events
            if isinstance(event, NodeRunSucceededEvent)
        ],
        runtime=engine.runtime_state.dumps(),
        response_filter=stream_filter.dumps(),
        chunks=_collect_stream_chunks(events),
        pause=events[-1],
        started_nodes={
            event.node_id: event.id
            for event in events
            if isinstance(event, NodeRunStartedEvent)
        },
    )


def _restore_pause(
    saved: _SavedPause, callback: HITLCallback
) -> tuple[Engine, ResponseStreamFilter]:
    engine = _hitl_engine(
        _DSL, runtime_state=RuntimeState.from_snapshot(saved.runtime), callback=callback
    )
    stream_filter = ResponseStreamFilter()
    stream_filter.loads(saved.response_filter)
    return engine, stream_filter


def _request_hitl_pause(context: HITLContext) -> PauseRequested:
    return PauseRequested(session_id=f"session-{context.node_id}")


def _complete_first(context: HITLContext) -> Completed | PauseRequested:
    return (
        _completed_hitl("alpha")
        if context.node_id == "first"
        else _request_hitl_pause(context)
    )


@pytest.fixture(scope="module", params=["c3870df", "069cd55", "current"])
def checkpoints(request: pytest.FixtureRequest) -> _PauseCheckpoints:
    if request.param != "current":
        return _PauseCheckpoints.model_validate_json(
            (_FIXTURES / f"response_pause_{request.param}.json").read_text()
        )
    first = _capture_pause(
        _hitl_engine(
            _DSL, runtime_state=_new_runtime_state({}), callback=_request_hitl_pause
        ),
        ResponseStreamFilter(),
    )
    engine, stream_filter = _restore_pause(first, _complete_first)
    second = _capture_pause(engine, stream_filter)
    channel = InMemoryChannel()
    channel.send_command(PauseCommand(reason="host checkpoint"))
    command = _capture_pause(
        _hitl_engine(
            _DSL,
            runtime_state=_new_runtime_state({}),
            callback=_request_hitl_pause,
            command_channel=channel,
        ),
        ResponseStreamFilter(),
    )
    return _PauseCheckpoints(
        writer_commit="current", first=first, second=second, command=command
    )


def _assert_resumed_stream(
    events: list[EngineEvent],
    saved: _SavedPause,
    expected_chunks: list[tuple[str, str, bool]],
) -> None:
    assert isinstance(events[0], GraphRunStartedEvent)
    assert events[0].reason == WorkflowStartReason.RESUMPTION
    started_node_run_ids = {
        event.node_id: event.id
        for event in events
        if isinstance(event, NodeRunStartedEvent)
    }
    for node_id in started_node_run_ids.keys() & saved.started_nodes.keys():
        assert started_node_run_ids[node_id] == saved.started_nodes[node_id]
    chunks = _collect_stream_chunks(events)
    assert [
        (chunk.node_id, chunk.selector, chunk.chunk, chunk.is_final) for chunk in chunks
    ] == [
        (node_id, [node_id, "answer"], text, final)
        for node_id, text, final in expected_chunks
    ]


@pytest.mark.parametrize("paused_at", ["first", "second"])
def test_hitl_snapshots_resume_stream_without_replaying_prefix(
    checkpoints: _PauseCheckpoints, paused_at: Literal["first", "second"]
) -> None:
    saved = checkpoints.first if paused_at == "first" else checkpoints.second
    assert saved.pause.reasons == [
        HitlRequired(  # ty: ignore[deprecated]
            session_id=f"session-{paused_at}",
            node_id=paused_at,
            node_title=f"{paused_at.title()} approval",
        )
    ]
    prior_chunks = list(checkpoints.first.chunks)
    callback_node_ids: list[str] = []

    def resolve(context: HITLContext) -> Completed | PauseRequested:
        callback_node_ids.append(context.node_id)
        if paused_at == "first" and context.node_id == "second":
            return _request_hitl_pause(context)
        return _completed_hitl("alpha" if context.node_id == "first" else "beta")

    engine, stream_filter = _restore_pause(saved, resolve)
    events = _run_with_response_filter(engine, stream_filter)
    assert "start" not in [
        event.node_id for event in events if isinstance(event, NodeRunStartedEvent)
    ]
    if paused_at == "first":
        assert callback_node_ids == ["first", "second"]
        _assert_resumed_stream(
            events,
            saved,
            [("first", "alpha", False), ("reply", "|middle|", False)],
        )
        assert isinstance(events[-1], GraphRunPausedEvent)
        assert events[-1].reasons == checkpoints.second.pause.reasons
        prior_chunks.extend(_collect_stream_chunks(events))
        saved = _SavedPause(
            completed_nodes=[
                event.node_id
                for event in events
                if isinstance(event, NodeRunSucceededEvent)
            ],
            runtime=engine.runtime_state.dumps(),
            response_filter=stream_filter.dumps(),
            chunks=_collect_stream_chunks(events),
            pause=events[-1],
            started_nodes={
                event.node_id: event.id
                for event in events
                if isinstance(event, NodeRunStartedEvent)
            },
        )
        engine, stream_filter = _restore_pause(
            saved, lambda _context: _completed_hitl("beta")
        )
        events = _run_with_response_filter(engine, stream_filter)
    else:
        assert callback_node_ids == ["second"]
        prior_chunks.extend(checkpoints.second.chunks)
    _assert_resumed_stream(
        events,
        saved,
        [("second", "beta", False), ("reply", "|after", True), ("tail", "|tail", True)],
    )
    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert [
        event.node_id for event in events if isinstance(event, NodeRunStartedEvent)
    ] == ["second", "reply", "tail"]
    assert final_outputs(events)["answer"] == _COMPLETE_TEXT
    assert (
        "".join(
            chunk.chunk for chunk in [*prior_chunks, *_collect_stream_chunks(events)]
        )
        == _COMPLETE_TEXT
    )
    assert list(stream_filter.flush()) == []


def test_command_pause_restores_deferred_work_and_stream(
    checkpoints: _PauseCheckpoints,
) -> None:
    saved = checkpoints.command
    assert saved.pause.reasons == [SchedulingPause(message="host checkpoint")]
    callback_node_ids: list[str] = []

    def resolve(context: HITLContext) -> Completed:
        callback_node_ids.append(context.node_id)
        return _completed_hitl("alpha" if context.node_id == "first" else "beta")

    engine, stream_filter = _restore_pause(saved, resolve)
    events = _run_with_response_filter(engine, stream_filter)
    assert callback_node_ids == ["first", "second"]
    assert not set(saved.completed_nodes) & {
        event.node_id for event in events if isinstance(event, NodeRunStartedEvent)
    }
    _assert_resumed_stream(
        events,
        saved,
        [
            ("first", "alpha", False),
            ("reply", "|middle|", False),
            ("second", "beta", False),
            ("reply", "|after", True),
            ("tail", "|tail", True),
        ],
    )
    assert isinstance(events[-1], GraphRunSucceededEvent)
    assert final_outputs(events)["answer"] == _COMPLETE_TEXT
    assert (
        "".join(
            chunk.chunk for chunk in [*saved.chunks, *_collect_stream_chunks(events)]
        )
        == _COMPLETE_TEXT
    )
    assert list(stream_filter.flush()) == []
