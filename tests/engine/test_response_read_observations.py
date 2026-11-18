from __future__ import annotations

import pytest

from graphon import engine_events
from graphon.dsl import loads
from graphon.engine import Engine
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events import FrameStartedEvent, NodeRunSucceededEvent
from graphon.variables.segments import ArrayFileSegment, FileSegment, StringSegment
from tests.engine.test_file_runtime import _make_file, _make_file_adapter
from tests.engine.test_frame_observations import _dsl, _events, _node


def test_answer_observes_each_explicit_read_using_its_saved_markdown() -> None:
    template = (
        "missing.text{{#file.text#}}/{{#missing.text#}}/"
        "{{#empty.text#}}/{{#file.text#}}"
    )
    loaded = loads(
        _dsl([_node("answer", "answer", answer=template)], [("start", "answer")])
    )
    adapter = _make_file_adapter("answer")
    urls = ["https://files.example/first", "https://files.example/second"]
    adapter.resolve_file_url.side_effect = urls
    engine = Engine(loaded.graph, loaded.runtime_state, file_runtime=adapter)
    file = _make_file()
    engine.runtime_state.variable_pool.add(("file", "text"), file)
    engine.runtime_state.variable_pool.add(("empty", "text"), "")

    raw = list(engine.run())

    success = next(
        event
        for event in raw
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer"
    )
    first, second = (f"[report.txt]({url})" for url in urls)
    assert success.node_run_result.outputs == {
        "answer": f"missing.text{first}/missing.text//{second}",
        "files": ArrayFileSegment(value=[file, file]),
    }
    assert success.node_run_result.inputs == success.node_run_result.process_data == {}
    read_events = _events(raw, "NodeRunResponseReadEvent")
    assert len(read_events) == 1, "raw consumers need the Answer's individual reads"
    observed = read_events[0]
    assert (observed.id, observed.frame_id, observed.retry_index) == (
        success.id,
        success.frame_id,
        success.retry_index,
    )
    assert [(read.position, read.selector, read.value) for read in observed.reads] == [
        (1, ("file", "text"), first),
        (3, ("missing", "text"), None),
        (5, ("empty", "text"), ""),
        (7, ("file", "text"), second),
    ]
    assert type(observed).model_validate_json(observed.model_dump_json()) == observed


@pytest.mark.parametrize("emit_frame_events", [True, False])
def test_end_observes_overwritten_reads_as_detached_segments(
    emit_frame_events: bool,
) -> None:
    fields = [
        {"variable": "result", "value_selector": [source, "text"]}
        for source in ["file", "empty", "missing"]
    ]
    loaded = loads(_dsl([_node("end", "end", outputs=fields)], [("start", "end")]))
    adapter = _make_file_adapter("end")
    engine = Engine(
        loaded.graph,
        loaded.runtime_state,
        file_runtime=adapter,
        emit_frame_events=emit_frame_events,
    )
    file = _make_file()
    engine.runtime_state.variable_pool.add(("file", "text"), file)
    engine.runtime_state.variable_pool.add(("empty", "text"), "")

    raw = list(engine.run())

    success = next(
        event
        for event in raw
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "end"
    )
    assert (
        success.node_run_result.inputs
        == success.node_run_result.outputs
        == {"result": None}
    )
    assert success.node_run_result.process_data == {}
    adapter.resolve_file_url.assert_not_called()
    read_events = _events(raw, "NodeRunResponseReadEvent")
    if not emit_frame_events:
        assert read_events == []
        return

    assert len(read_events) == 1, (
        "End must expose each read even when later fields overwrite its output"
    )
    observed = read_events[0]
    file.filename = "later.txt"
    assert [(read.position, read.selector, read.value) for read in observed.reads] == [
        (0, ("file", "text"), FileSegment(value=_make_file())),
        (2, ("empty", "text"), StringSegment(value="")),
        (4, ("missing", "text"), None),
    ]
    assert type(observed).model_validate_json(observed.model_dump_json()) == observed
    assert "NodeRunResponseReadEvent" in engine_events.__all__
    assert any(
        getattr(event, "observes_response_reads", False)
        for event in raw
        if isinstance(event, FrameStartedEvent)
    )
    filtered = list(
        filter_engine_events(
            raw,
            context=EngineEventFilterContext.from_engine(engine),
            filters=[ResponseStreamFilter()],
        )
    )
    assert not _events(filtered, "NodeRunResponseReadEvent")
    assert success in filtered
