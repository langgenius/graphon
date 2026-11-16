"""Filter-owned child replies from live and delayed frame observations."""

from __future__ import annotations

import json
from collections.abc import Generator, Iterable
from dataclasses import replace
from datetime import UTC, datetime
from threading import Event, Thread
from typing import Any

import pytest

from graphon.dsl import loads
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.frame import (
    FrameFinishedEvent,
    FrameStartedEvent,
    FrameVariablesUpdatedEvent,
)
from graphon.engine_events.graph import (
    GraphRunAbortedEvent,
    GraphRunFailedEvent,
    GraphRunStartedEvent,
)
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunFailedEvent,
    NodeRunReasoningChunkEvent,
    NodeRunRetryEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.enums import (
    BuiltinNodeTypes,
    NodeExecutionType,
    NodeState,
    WorkflowNodeExecutionStatus,
)
from graphon.file.runtime import use_workflow_file_runtime
from graphon.node_events.base import NodeEventPayload, NodeRunResult
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.base.template import Template
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from graphon.variables.variables import StringVariable
from tests.engine.test_file_runtime import _make_file, _make_file_adapter
from tests.engine.test_frame_observations import _dsl, _node
from tests.engine.test_response_stream_filter import (
    _context,
    _TestEdge,
    _TestGraph,
    _TestNode,
)


def _make_body_dsl(
    container_type: str = "iteration",
    *,
    answer: str = "result: {{#source.output#}}!",
    branch: bool = False,
    second_answer: str | None = None,
) -> str:
    container_data: dict[str, Any] = {"start_node_id": "body-start"}
    if container_type == "iteration":
        container_data.update(
            iterator_selector=["start", "items"], output_selector=["answer", "answer"]
        )
    else:
        container_data.update(loop_count=2, break_conditions=[], logical_operator="and")
    nodes = [
        _node("container", container_type, **container_data),
        _node("body-start", f"{container_type}-start", container_id="container"),
        _node(
            "source",
            "template-transform",
            container_id="container",
            template="value",
            variables=[],
        ),
        _node("answer", "answer", container_id="container", answer=answer),
        _node("done", "answer", answer="done"),
    ]
    edges = [
        ("start", "container"),
        ("container", "done"),
        ("body-start", "source"),
        ("source", "answer"),
    ]
    if branch:
        nodes.append(_node("branch", "if-else", container_id="container", cases=[]))
        edges[-1] = ("source", "branch")
        edges.append(("branch", "answer"))
    if second_answer is not None:
        nodes.append(
            _node("second", "answer", container_id="container", answer=second_answer)
        )
        edges.append(("answer", "second"))
    return _dsl(nodes, edges)


def _select_stream_chunks(
    events: Iterable[EngineEvent],
) -> list[NodeRunStreamChunkEvent]:
    return [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]


def _join_stream_text(events: Iterable[EngineEvent]) -> str:
    return "".join(event.chunk for event in _select_stream_chunks(events))


@pytest.mark.parametrize("container_type", ["loop", "iteration"])
def test_child_prefix_and_tokens_arrive_before_each_body_completes(  # ruff: ignore[complex-structure, too-many-statements]
    monkeypatch: pytest.MonkeyPatch, container_type: str
) -> None:
    engine = loads(
        _make_body_dsl(container_type), start_inputs={"items": [1, 2]}, workers=1
    )
    prefix_events = [Event(), Event()]
    token_events = [Event(), Event()]
    completion_gates = [Event(), Event()]
    invocation_count = 0

    def stream_source(_node: TemplateTransformNode) -> Generator[NodeEventPayload]:
        nonlocal invocation_count
        index = invocation_count
        invocation_count += 1
        yield StreamChunkEvent(
            selector=["source", "secret"], chunk="private", is_final=True
        )
        yield StreamChunkEvent(selector=["source", "output"], chunk=f"item {index}")
        assert completion_gates[index].wait(10)
        yield StreamChunkEvent(selector=["source", "output"], chunk="", is_final=True)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                outputs={"output": f"item {index}"},
            )
        )

    monkeypatch.setattr(TemplateTransformNode, "_run", stream_source)
    raw_events: list[EngineEvent] = []
    filtered_events: list[EngineEvent] = []
    errors: list[Exception] = []
    reply_text: dict[str, str] = {}

    def consume_events() -> None:
        def record_events() -> Generator[EngineEvent]:
            for event in engine.run():
                raw_events.append(event)
                yield event

        try:  # ruff: ignore[too-many-statements-in-try-clause]
            for event in filter_engine_events(
                record_events(),
                filters=[ResponseStreamFilter()],
                context=EngineEventFilterContext.from_engine(engine),
            ):
                filtered_events.append(event)
                if (
                    isinstance(event, NodeRunStreamChunkEvent)
                    and event.node_id == "answer"
                ):
                    index = (
                        len([
                            item
                            for item in raw_events
                            if isinstance(item, FrameStartedEvent)
                            and item.parent_frame_id
                        ])
                        - 1
                    )
                    reply_text[event.frame_id] = (
                        reply_text.get(event.frame_id, "") + event.chunk
                    )
                    if reply_text[event.frame_id].startswith("result: "):
                        prefix_events[index].set()
                    if reply_text[event.frame_id].startswith(f"result: item {index}"):
                        token_events[index].set()
        except Exception as error:  # ruff: ignore[blind-except]
            errors.append(error)

    consumer = Thread(target=consume_events, daemon=True)
    consumer.start()
    try:
        for index in range(2):
            assert prefix_events[index].wait(3), (
                "child prefix must arrive while the producer is gated"
            )
            assert token_events[index].wait(3), (
                "selected token must arrive while the producer is gated"
            )
            assert (
                len([
                    event
                    for event in raw_events
                    if isinstance(event, NodeRunSucceededEvent)
                    and event.node_id == "source"
                ])
                == index
            )
            completion_gates[index].set()
    finally:
        for gate in completion_gates:
            gate.set()
        consumer.join(10)
    assert not consumer.is_alive()
    assert not errors
    assert _join_stream_text(filtered_events) == "result: item 0!result: item 1!done"
    assert all(event.node_id == "source" for event in _select_stream_chunks(raw_events))
    starts = [
        event
        for event in raw_events
        if isinstance(event, FrameStartedEvent) and event.parent_frame_id
    ]
    for start in starts:
        reply = [
            event
            for event in _select_stream_chunks(filtered_events)
            if event.frame_id == start.frame_id
        ]
        assert {event.id for event in reply} == {start.node_execution_ids["answer"]}
        assert all(
            event.selector == ["answer", "answer"] and event.container_id == "container"
            for event in reply
        )
        assert sum(event.is_final for event in reply) == 1
        assert reply[-1].is_final
    assert not any(
        isinstance(event, (FrameStartedEvent, FrameVariablesUpdatedEvent))
        for event in filtered_events
    )


def _make_filter_and_start(
    *,
    answer: str = "[{{#source.output#}}]",
    branch: bool = False,
    second_answer: str | None = None,
) -> tuple[ResponseStreamFilter, FrameStartedEvent]:
    engine = loads(
        _make_body_dsl(answer=answer, branch=branch, second_answer=second_answer)
    )
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(EngineEventFilterContext.from_engine(engine))
    start = FrameStartedEvent(
        frame_id="child",
        parent_frame_id="root",
        container_id="container",
        root_node_id="body-start",
        invocation_id="invocation",
        container_execution_id="container-run",
        node_execution_ids={
            node: f"{node}-run"
            for node in ["body-start", "source", "branch", "answer", "second"]
        },
    )
    return stream_filter, start


def _make_source_chunk(
    text: str,
    *,
    attempt: int = 0,
    final: bool = False,
    selector: list[str] | None = None,
) -> NodeRunStreamChunkEvent:
    return NodeRunStreamChunkEvent(
        id="source-run",
        frame_id="child",
        container_id="container",
        node_id="source",
        node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
        retry_index=attempt,
        selector=selector or ["source", "output"],
        chunk=text,
        is_final=final,
    )


def _make_variable_commit(
    value: str, *, selector: tuple[str, str] = ("source", "output")
) -> FrameVariablesUpdatedEvent:
    return FrameVariablesUpdatedEvent(
        frame_id="child",
        variables=(StringVariable(name=selector[1], selector=selector, value=value),),
    )


def _make_edge_event(
    source: str, target: str, index: int, *, skipped: bool = False
) -> EngineEvent:
    event_type = GraphEdgeSkippedEvent if skipped else GraphEdgeTakenEvent
    return event_type(
        frame_id="child",
        edge_id=f"edge_{index}",
        source_node_id=source,
        target_node_id=target,
    )


def _make_source_completion(*, failure: bool = False) -> EngineEvent:
    fields: dict[str, Any] = {
        "id": "source-run",
        "node_id": "source",
        "node_type": BuiltinNodeTypes.TEMPLATE_TRANSFORM,
        "frame_id": "child",
        "start_at": datetime.now(UTC),
    }
    if failure:
        return NodeRunExceptionEvent(**fields, error="handled")
    return NodeRunSucceededEvent(**fields)


def test_each_template_occurrence_and_response_reads_its_own_source() -> None:
    stream_filter, start = _make_filter_and_start(
        answer="{{#source.output#}}/{{#source.output#}}!",
        second_answer="next:{{#source.output#}}",
    )
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("value", final=True)),
        *stream_filter.on_event(_make_edge_event("answer", "second", 4)),
    ]
    assert _join_stream_text(output) == "value/value!next:value"
    for node_id in ["answer", "second"]:
        chunks = [
            event for event in _select_stream_chunks(output) if event.node_id == node_id
        ]
        assert sum(event.is_final for event in chunks) == 1


def test_reverse_arrival_waits_for_template_order_without_replaying_final_value() -> (
    None
):
    stream_filter, start = _make_filter_and_start(
        answer="{{#source.first#}}/{{#source.second#}}"
    )
    output = list(stream_filter.on_event(start))
    assert (
        list(
            stream_filter.on_event(
                _make_source_chunk("second", selector=["source", "second"], final=True)
            )
        )
        == []
    )
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("first", selector=["source", "first"])
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_variable_commit("first-full", selector=("source", "first"))
        )
    )
    output.extend(stream_filter.on_event(_make_source_completion()))
    assert _join_stream_text(output) == "first/second"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1


@pytest.mark.parametrize("streamed", [False, True])
def test_handled_failure_settles_source_without_replaying_fallback(
    streamed: bool,
) -> None:
    stream_filter, start = _make_filter_and_start()
    output = list(stream_filter.on_event(start))
    if streamed:
        output.extend(stream_filter.on_event(_make_source_chunk("partial")))
    output.extend(stream_filter.on_event(_make_variable_commit("default")))
    output.extend(stream_filter.on_event(_make_source_completion(failure=True)))
    assert _join_stream_text(output) == ("[partial]" if streamed else "[default]")
    assert _select_stream_chunks(output)[-1].is_final
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("already_displayed", [False, True])
def test_retry_replaces_only_undisplayed_attempt(already_displayed: bool) -> None:
    stream_filter, start = _make_filter_and_start(branch=not already_displayed)
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("old")),
    ]
    retry = NodeRunRetryEvent(
        id="source-run",
        node_id="source",
        node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
        frame_id="child",
        retry_index=1,
        error="retry",
        node_title="source",
        start_at=datetime.now(UTC),
    )
    output.extend(stream_filter.on_event(retry))
    output.extend(
        stream_filter.on_event(_make_source_chunk("new", attempt=1, final=True))
    )
    if not already_displayed:
        output.extend(stream_filter.on_event(_make_edge_event("branch", "answer", 4)))
    assert _join_stream_text(output) == ("[old]" if already_displayed else "[new]")
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1


def test_committed_replacement_applies_once_and_later_responses_see_overwrite() -> None:
    stream_filter, start = _make_filter_and_start(
        second_answer="next:{{#source.output#}}"
    )
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("old", final=True)),
    ]
    output.extend(stream_filter.on_event(_make_variable_commit("old")))
    output.extend(stream_filter.on_event(_make_source_completion()))
    output.extend(stream_filter.on_event(_make_variable_commit("new")))
    request = NodeRunVariableUpdatedEvent(
        id="assign-run",
        node_id="assign",
        node_type=BuiltinNodeTypes.VARIABLE_ASSIGNER,
        frame_id="child",
        variable=StringVariable(
            name="output", selector=["source", "output"], value="reported-request"
        ),
    )
    output.extend(stream_filter.on_event(request))
    reported = _make_source_completion().model_copy(
        update={"node_run_result": NodeRunResult(outputs={"output": "reported-result"})}
    )
    output.extend(stream_filter.on_event(reported))
    output.extend(stream_filter.on_event(_make_edge_event("answer", "second", 4)))
    assert _join_stream_text(output) == "[old]next:new"


def test_skipped_reply_never_activates_from_registered_ids_or_other_frame_edges() -> (
    None
):
    stream_filter, start = _make_filter_and_start(branch=True)
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("secret", final=True)),
    ]
    output.extend(
        stream_filter.on_event(
            _make_edge_event("branch", "answer", 4).model_copy(
                update={"frame_id": "other-child"}
            )
        )
    )
    output.extend(
        stream_filter.on_event(_make_edge_event("branch", "answer", 4, skipped=True))
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id="child", outcome="succeeded")
        )
    )
    output.extend(stream_filter.flush())
    assert _join_stream_text(output) == ""


@pytest.mark.parametrize(
    "terminal", [GraphRunFailedEvent(error="failed"), GraphRunAbortedEvent()]
)
def test_terminal_event_closes_displayed_reply_and_discards_pending_templates(
    terminal: EngineEvent,
) -> None:
    stream_filter, start = _make_filter_and_start(second_answer="unreached")
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("partial")),
        *stream_filter.on_event(terminal),
    ]
    assert _join_stream_text(output) == "[partial"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1
    assert output[-1] is terminal
    assert list(stream_filter.flush()) == []


@pytest.mark.parametrize("answer", ["", "{{#sys.empty#}}"])
def test_empty_child_answer_has_exactly_one_final_marker(answer: str) -> None:
    stream_filter, start = _make_filter_and_start(answer=answer)
    start = start.model_copy(
        update={
            "variables": (
                StringVariable(name="empty", selector=["sys", "empty"], value=""),
            )
        }
    )
    chunks = _select_stream_chunks(stream_filter.on_event(start))
    assert len(chunks) == 1
    assert chunks[0].is_final
    assert chunks[0].chunk == ""
    assert chunks[0].id == "answer-run"
    assert chunks[0].selector == ["answer", "answer"]


@pytest.mark.parametrize("container_type", ["loop", "iteration"])
def test_delayed_filter_uses_removed_frame_values_and_preserves_files(
    monkeypatch: pytest.MonkeyPatch, container_type: str
) -> None:
    adapter = _make_file_adapter("child")
    file = _make_file()
    answer = "{{#source.output#}} {{#sys.file#}}"
    with use_workflow_file_runtime(adapter):
        engine = loads(
            _make_body_dsl(container_type, answer=answer),
            start_inputs={"items": [1, 2]},
            workers=1,
        )
        engine.runtime_state.variable_pool.add(["sys", "file"], file)
        engine.runtime_state.variable_pool.add(["env", "secret"], "secret")
        raw_events = list(engine.run())
        context = EngineEventFilterContext.from_engine(engine)

    def reject_node_creation(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("filter discovery must not construct executable nodes")

    assert engine.graph.node_factory is not None
    monkeypatch.setattr(
        type(engine.graph.node_factory), "create_node", reject_node_creation
    )
    engine.runtime_state.variable_pool.add(["source", "output"], "read-ahead")
    with use_workflow_file_runtime(_make_file_adapter("caller")):
        output = list(
            filter_engine_events(
                raw_events, filters=[ResponseStreamFilter()], context=context
            )
        )
    assert (
        _join_stream_text(output)
        == "value [report.txt](https://child.example/report.txt)" * 2 + "done"
    )
    assert not any(
        isinstance(event, (FrameStartedEvent, FrameVariablesUpdatedEvent))
        for event in output
    )
    results = [
        event.node_run_result.outputs
        for event in output
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer"
    ]
    assert len(results) == 2
    assert all(
        result["answer"] == "value [report.txt](https://child.example/report.txt)"
        and result["files"].value == [file]
        for result in results
    )


def test_unselected_chunks_and_reasoning_stay_hidden() -> None:
    stream_filter, start = _make_filter_and_start()
    output = list(stream_filter.on_event(start))
    output.extend(
        stream_filter.on_event(
            _make_variable_commit("secret", selector=("env", "secret"))
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("secret", selector=["source", "private"], final=True)
        )
    )
    output.extend(
        stream_filter.on_event(
            NodeRunReasoningChunkEvent(
                id="source-run",
                node_id="source",
                node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
                frame_id="child",
                selector=["source", "reasoning_content"],
                chunk="secret",
            )
        )
    )
    output.extend(stream_filter.on_event(_make_source_chunk("visible", final=True)))
    assert _join_stream_text(output) == "[visible]"
    assert not any(
        isinstance(event, (NodeRunReasoningChunkEvent, FrameVariablesUpdatedEvent))
        for event in output
    )


@pytest.mark.parametrize("missing_root", [False, True])
def test_child_baseline_and_commits_supply_values_without_live_frame(
    missing_root: bool,
) -> None:
    stream_filter, start = _make_filter_and_start(
        answer="{{#container.item#}}:{{#sys.value#}}", branch=True
    )
    start = start.model_copy(
        update={
            "root_node_id": None if missing_root else "body-start",
            "variables": (
                StringVariable(
                    name="item", selector=["container", "item"], value="item"
                ),
                StringVariable(
                    name="value", selector=["sys", "value"], value="initial"
                ),
            ),
        }
    )
    output = list(stream_filter.on_event(start))
    output.extend(
        stream_filter.on_event(
            _make_variable_commit("committed", selector=("sys", "value"))
        )
    )
    output.extend(stream_filter.on_event(_make_edge_event("branch", "answer", 4)))
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id="child", outcome="succeeded")
        )
    )
    assert _join_stream_text(output) == "item:committed"


def test_iteration_replies_use_each_items_baseline() -> None:
    engine = loads(
        _make_body_dsl(answer="{{#container.index#}}={{#container.item#}};"),
        start_inputs={"items": ["one", "two"]},
        workers=1,
    )
    context = EngineEventFilterContext.from_engine(engine)
    output = list(
        filter_engine_events(
            list(engine.run()), filters=[ResponseStreamFilter()], context=context
        )
    )
    assert _join_stream_text(output) == "0=one;1=two;done"


def test_child_end_template_uses_committed_values() -> None:
    config = json.loads(_make_body_dsl())
    answer = next(node for node in config["graph"]["nodes"] if node["id"] == "answer")
    answer["data"] = {
        "type": "end",
        "container_id": "container",
        "outputs": [{"variable": "result", "value_selector": ["source", "output"]}],
    }
    engine = loads(json.dumps(config), start_inputs={"items": [1, 2]}, workers=1)
    output = list(
        filter_engine_events(
            engine.run(),
            filters=[ResponseStreamFilter()],
            context=EngineEventFilterContext.from_engine(engine),
        )
    )
    assert _join_stream_text(output) == "valuevaluedone"


def test_child_progress_cannot_be_saved_as_a_root_only_snapshot() -> None:
    stream_filter, start = _make_filter_and_start()
    list(stream_filter.on_event(start))
    list(stream_filter.on_event(_make_source_chunk("partial")))
    with pytest.raises(ValueError, match="child"):
        stream_filter.dumps()


def test_root_frame_baseline_prevents_live_value_read_ahead() -> None:
    engine = loads(
        _dsl(
            [_node("answer", "answer", answer="{{#sys.value#}}")], [("start", "answer")]
        )
    )
    engine.runtime_state.variable_pool.add(["sys", "value"], "future")
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(EngineEventFilterContext.from_engine(engine))
    output = list(stream_filter.on_event(GraphRunStartedEvent()))
    output.extend(
        stream_filter.on_event(
            FrameStartedEvent(
                frame_id="root",
                root_node_id="start",
                node_execution_ids={"start": "start-run", "answer": "answer-run"},
                variables=(
                    StringVariable(
                        name="value", selector=["sys", "value"], value="baseline"
                    ),
                ),
            )
        )
    )
    assert _join_stream_text(output) == "baseline"


def test_interleaved_children_keep_sources_attempts_and_reply_ids_separate() -> None:
    stream_filter, start = _make_filter_and_start()
    sibling = start.model_copy(
        update={
            "frame_id": "sibling",
            "node_execution_ids": {
                node: f"sibling-{node}" for node in start.node_execution_ids
            },
        }
    )
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("first")),
        *stream_filter.on_event(sibling),
    ]
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("second", final=True).model_copy(
                update={"frame_id": "sibling", "id": "sibling-source"}
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            NodeRunRetryEvent(
                id="source-run",
                node_id="source",
                node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
                frame_id="child",
                retry_index=1,
                error="retry",
                node_title="source",
                start_at=datetime.now(UTC),
            )
        )
    )
    output.extend(
        stream_filter.on_event(_make_source_chunk("replacement", attempt=1, final=True))
    )
    for frame_id, expected, execution_id in [
        ("child", "[first]", "answer-run"),
        ("sibling", "[second]", "sibling-answer"),
    ]:
        chunks = [
            event
            for event in _select_stream_chunks(output)
            if event.frame_id == frame_id
        ]
        assert _join_stream_text(chunks) == expected
        assert {event.id for event in chunks} == {execution_id}
        assert sum(event.is_final for event in chunks) == 1


def test_failed_frame_settles_partial_reply_before_next_child_and_graph_terminal() -> (
    None
):
    stream_filter, start = _make_filter_and_start(second_answer="unreached")
    output = [
        *stream_filter.on_event(start),
        *stream_filter.on_event(_make_source_chunk("partial")),
        *stream_filter.on_event(
            NodeRunFailedEvent(
                id="source-run",
                node_id="source",
                node_type=BuiltinNodeTypes.TEMPLATE_TRANSFORM,
                frame_id="child",
                start_at=datetime.now(UTC),
                error="failed",
            )
        ),
        *stream_filter.on_event(
            FrameFinishedEvent(frame_id="child", outcome="failed", error="failed")
        ),
    ]
    assert _join_stream_text(output) == "[partial"
    assert sum(event.is_final for event in _select_stream_chunks(output)) == 1
    next_start = start.model_copy(
        update={
            "frame_id": "next-child",
            "node_execution_ids": {
                node: f"next-{node}" for node in start.node_execution_ids
            },
        }
    )
    output.extend(stream_filter.on_event(next_start))
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("next", final=True).model_copy(
                update={"frame_id": "next-child", "id": "next-source"}
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            FrameFinishedEvent(frame_id="next-child", outcome="succeeded")
        )
    )
    output.extend(stream_filter.on_event(GraphRunFailedEvent(error="failed")))
    assert _join_stream_text(output) == "[partial[next]"
    for frame_id in ["child", "next-child"]:
        assert (
            sum(
                event.is_final
                for event in _select_stream_chunks(output)
                if event.frame_id == frame_id
            )
            == 1
        )
    assert list(stream_filter.flush()) == []


def test_root_commits_precede_branch_activation() -> None:
    engine = loads(
        _dsl(
            [
                _node("branch", "if-else", cases=[]),
                _node("answer", "answer", answer="{{#sys.value#}}"),
            ],
            [("start", "branch"), ("branch", "answer")],
        )
    )
    engine.runtime_state.variable_pool.add(["sys", "value"], "future")
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(EngineEventFilterContext.from_engine(engine))
    output = list(stream_filter.on_event(GraphRunStartedEvent()))
    output.extend(
        stream_filter.on_event(
            FrameStartedEvent(
                frame_id="root",
                node_execution_ids={"answer": "answer-run"},
                variables=(
                    StringVariable(
                        name="value", selector=["sys", "value"], value="initial"
                    ),
                ),
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_variable_commit("committed", selector=("sys", "value")).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_edge_event("branch", "answer", 1).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "committed"


def test_root_v2_resume_keeps_partial_template_progress_with_observed_baseline() -> (
    None
):
    engine = loads(
        _dsl(
            [
                _node("source", "template-transform", template="unused", variables=[]),
                _node("answer", "answer", answer="[{{#source.output#}}]"),
            ],
            [("start", "source"), ("source", "answer")],
        )
    )
    context = EngineEventFilterContext.from_engine(engine)
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(context)
    start = FrameStartedEvent(
        frame_id="root",
        node_execution_ids={"source": "source-run", "answer": "answer-run"},
    )
    output = [
        *stream_filter.on_event(GraphRunStartedEvent()),
        *stream_filter.on_event(start),
        *stream_filter.on_event(
            _make_source_chunk("first").model_copy(update={"frame_id": "root"})
        ),
    ]
    assert _join_stream_text(output) == "[first"
    restored = ResponseStreamFilter()
    restored.loads(stream_filter.dumps())
    restored.initialize(context)
    resumed = start.model_copy(
        update={
            "reason": WorkflowStartReason.RESUMPTION,
            "variables": (
                StringVariable(
                    name="output", selector=["source", "output"], value="future"
                ),
            ),
        }
    )
    output.extend(
        restored.on_event(GraphRunStartedEvent(reason=WorkflowStartReason.RESUMPTION))
    )
    output.extend(restored.on_event(resumed))
    output.extend(
        restored.on_event(
            _make_source_chunk("last", final=True).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "[firstlast]"
    assert list(restored.flush()) == []


def test_resumed_child_without_response_progress_is_rejected() -> None:
    stream_filter, start = _make_filter_and_start()
    resumed = start.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
    with pytest.raises(ValueError, match="child"):
        list(stream_filter.on_event(resumed))


@pytest.mark.parametrize("legacy_snapshot", [False, True])
def test_root_resume_preserves_skipped_sources_without_reading_future_node_states(
    legacy_snapshot: bool,
) -> None:
    branch = _TestNode("branch", execution_type=NodeExecutionType.BRANCH)
    source = _TestNode("source")
    skipped = _TestNode("skipped")
    answer = _TestNode(
        "answer",
        execution_type=NodeExecutionType.RESPONSE,
        template=Template.from_answer_template(
            "[{{#source.output#}}/{{#skipped.output#}}]"
        ),
    )
    graph = _TestGraph(
        nodes={node.id: node for node in [branch, source, skipped, answer]},
        edges={
            edge.id: edge
            for edge in [
                _TestEdge("chosen", "branch", "source"),
                _TestEdge("skip", "branch", "skipped"),
                _TestEdge("source-answer", "source", "answer"),
                _TestEdge("skipped-answer", "skipped", "answer"),
            ]
        },
        root_node_id="branch",
    )
    context = replace(_context(graph), run_id="run")
    stream_filter = ResponseStreamFilter()
    stream_filter.initialize(context)
    baseline = FrameStartedEvent(
        frame_id="root",
        node_execution_ids={node: f"{node}-run" for node in graph.nodes},
    )
    output = [
        *stream_filter.on_event(GraphRunStartedEvent()),
        *stream_filter.on_event(baseline),
    ]
    output.extend(
        stream_filter.on_event(
            GraphEdgeSkippedEvent(
                frame_id="root",
                edge_id="skip",
                source_node_id="branch",
                target_node_id="skipped",
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            GraphEdgeTakenEvent(
                frame_id="root",
                edge_id="chosen",
                source_node_id="branch",
                target_node_id="source",
            )
        )
    )
    output.extend(
        stream_filter.on_event(
            _make_source_chunk("first").model_copy(update={"frame_id": "root"})
        )
    )
    assert _join_stream_text(output) == "[first"
    state = json.loads(stream_filter.dumps())
    skipped.state = NodeState.SKIPPED
    if legacy_snapshot:
        # Historical v2 snapshots delegated skip state to the paired graph.
        state.pop("skipped_edges", None)
    else:
        # Later runtime mutations must not affect captured filter traversal.
        source.state = NodeState.SKIPPED
    restored = ResponseStreamFilter()
    restored.loads(json.dumps(state))
    restored.initialize(context)
    output.extend(
        restored.on_event(GraphRunStartedEvent(reason=WorkflowStartReason.RESUMPTION))
    )
    output.extend(
        restored.on_event(
            baseline.model_copy(update={"reason": WorkflowStartReason.RESUMPTION})
        )
    )
    output.extend(
        restored.on_event(
            _make_source_chunk("last", final=True).model_copy(
                update={"frame_id": "root"}
            )
        )
    )
    assert _join_stream_text(output) == "[firstlast/]"
    assert list(restored.flush()) == []
