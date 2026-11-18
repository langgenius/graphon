"""Complete LLM results are displayed as read by each Answer or End."""

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest

from graphon.dsl import loads
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.frame import FrameStartedEvent, FrameVariablesUpdatedEvent
from graphon.engine_events.graph import (
    GraphRunPartialSucceededEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import (
    NodeRunExceptionEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.file.runtime import use_workflow_file_runtime
from graphon.variables.variables import StringVariable
from tests.dsl.test_node_factory import (
    _graph_dsl_for_nodes,
    _llm_data,
    _openai_credentials,
    _patch_fake_slim_llm,
)
from tests.engine.test_container_response_streaming import _join_stream_text
from tests.engine.test_file_runtime import _make_file, _make_file_adapter
from tests.engine.test_frame_observations import _node


def _edge(source: str, target: str, handle: str = "source") -> dict[str, str]:
    return {"source": source, "target": target, "sourceHandle": handle}


def _loop(**data: Any) -> dict[str, Any]:
    return _node(
        "loop",
        "loop",
        start_node_id="body",
        loop_count=1,
        break_conditions=[],
        logical_operator="and",
        **data,
    )


def _overwrite_draft_text(value: str, **data: Any) -> dict[str, Any]:
    return _node(
        "edit",
        "assigner",
        version="2",
        **data,
        items=[
            {
                "variable_selector": ["draft", "text"],
                "input_type": "constant",
                "operation": "over-write",
                "value": value,
            }
        ],
    )


def _run_reply_graph(
    monkeypatch: pytest.MonkeyPatch,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, str]],
    *,
    seed: str | None = None,
    workers: int = 1,
) -> tuple[dict[str, Mapping[str, Any]], str]:
    _patch_fake_slim_llm(monkeypatch).responses["chat-model"] = "draft"
    engine = loads(
        _graph_dsl_for_nodes(nodes=nodes, edges=edges),
        credentials=_openai_credentials(),
        start_inputs={"tail": "tail"},
        workers=workers,
    )
    if seed is not None:
        engine.runtime_state.variable_pool.add(["draft", "text"], seed)
    stream = ResponseStreamFilter()
    stream.initialize(EngineEventFilterContext.from_engine(engine))
    results = {}
    chunks = []
    for event in engine.run():
        if isinstance(event, NodeRunSucceededEvent):
            results[event.node_id] = event.node_run_result.outputs
        chunks.extend(
            chunk.chunk
            for chunk in stream.on_event(event)
            if isinstance(chunk, NodeRunStreamChunkEvent)
        )
    assert isinstance(event, GraphRunSucceededEvent)
    return results, "".join(chunks)


@pytest.mark.parametrize("in_loop", [False, True], ids=["root", "child"])
@pytest.mark.parametrize("kind", ["answer", "end"])
def test_queued_reply_keeps_text_it_read_before_an_edit(
    monkeypatch: pytest.MonkeyPatch, in_loop: bool, kind: str
) -> None:
    before = (
        _node("before", "answer", answer="Before: {{#draft.text#}}.")
        if kind == "answer"
        else _node(
            "before",
            "end",
            outputs=[
                {"variable": "value", "value_selector": ["draft", "text"]},
                {"variable": "value", "value_selector": ["start", "tail"]},
            ],
        )
    )
    nodes: list[dict[str, Any]] = [
        {"id": "draft", "data": _llm_data()},
        _node("after", "answer", answer="After: {{#draft.text#}}."),
        before,
        _overwrite_draft_text("edited"),
    ]
    edges = [
        _edge("draft", "before"),
        _edge("draft", "after"),
        _edge("before", "edit"),
        _edge("edit", "after"),
    ]
    if in_loop:
        for node in nodes:
            node["data"]["container_id"] = "loop"
        nodes.extend([_loop(), _node("body", "loop-start", container_id="loop")])
        edges.extend([_edge("start", "loop"), _edge("body", "draft")])
    else:
        edges.append(_edge("start", "draft"))
    results, text = _run_reply_graph(monkeypatch, nodes, edges, workers=4)
    assert results["after"]["answer"] == "After: edited."
    if kind == "answer":
        assert results["before"]["answer"] == "Before: draft."
        assert text == results["after"]["answer"] + results["before"]["answer"]
    else:
        assert results["before"] == {"value": "tail"}
        assert text == results["after"]["answer"] + "draft\ntail"


@pytest.mark.parametrize("replacement", ["edited", ""], ids=["overwrite", "clear"])
def test_answer_displays_text_written_by_fail_branch_loop(
    monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    results, text = _run_reply_graph(
        monkeypatch,
        [
            {"id": "draft", "data": _llm_data()},
            _loop(error_strategy="fail-branch"),
            _node("body", "loop-start", container_id="loop"),
            _overwrite_draft_text(replacement, container_id="loop"),
            _node("reply", "answer", answer="[{{#draft.text#}}]/{{#draft.text#}}"),
        ],
        [
            _edge("start", "draft"),
            _edge("draft", "reply"),
            _edge("draft", "loop"),
            _edge("body", "edit"),
            _edge("loop", "reply"),
        ],
    )
    assert results["reply"]["answer"] == f"[{replacement}]/{replacement}"
    assert text == results["reply"]["answer"]


def _skipped_llm_graph(
    reply: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    return (
        [
            _node(
                "route",
                "if-else",
                cases=[
                    {
                        "case_id": "run",
                        "logical_operator": "and",
                        "conditions": [
                            {
                                "variable_selector": ["start", "tail"],
                                "comparison_operator": "is",
                                "value": "run",
                            }
                        ],
                    }
                ],
            ),
            {"id": "draft", "data": _llm_data()},
            reply,
        ],
        [
            _edge("start", "route"),
            _edge("route", "draft", "run"),
            _edge("route", "reply", "false"),
            _edge("draft", "reply"),
        ],
    )


@pytest.mark.parametrize("seed", [None, "saved"], ids=["missing", "preseeded"])
def test_answer_uses_the_value_found_for_a_skipped_llm(
    monkeypatch: pytest.MonkeyPatch, seed: str | None
) -> None:
    nodes, edges = _skipped_llm_graph(
        _node("reply", "answer", answer="[{{#draft.text#}}]")
    )
    results, text = _run_reply_graph(monkeypatch, nodes, edges, seed=seed)
    assert "draft" not in results
    assert results["reply"]["answer"] == f"[{seed or 'draft.text'}]"
    assert text == f"[{seed or ''}]"


def test_handled_answer_failure_releases_the_next_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fake_slim_llm(monkeypatch).responses["chat-model"] = "draft"
    engine = loads(
        _graph_dsl_for_nodes(
            nodes=[
                {"id": "draft", "data": _llm_data()},
                _node(
                    "reply",
                    "answer",
                    answer="First: {{#draft.text#}}",
                    error_strategy="default-value",
                    default_value=[
                        {"key": "answer", "type": "string", "value": "fallback"}
                    ],
                ),
                _node("next", "answer", answer="Next: {{#reply.answer#}}"),
            ],
            edges=[
                _edge("start", "draft"),
                _edge("draft", "reply"),
                _edge("reply", "next"),
            ],
        ),
        credentials=_openai_credentials(),
    )

    def fail() -> None:
        msg = "reply could not be rendered"
        raise ValueError(msg)

    monkeypatch.setattr(engine.graph.nodes["reply"], "_run", fail)
    events = list(
        filter_engine_events(
            engine.run(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[ResponseStreamFilter()],
        )
    )
    assert any(
        isinstance(event, NodeRunExceptionEvent) and event.node_id == "reply"
        for event in events
    )
    assert isinstance(events[-1], GraphRunPartialSucceededEvent)
    assert _join_stream_text(events) == "First: Next: fallback"


def test_queued_end_renders_a_file_read_from_a_skipped_llm_with_the_filter_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fake_slim_llm(monkeypatch)
    nodes, edges = _skipped_llm_graph(
        _node(
            "reply",
            "end",
            outputs=[{"variable": "file", "value_selector": ["draft", "text"]}],
        )
    )
    nodes.extend([
        _node("intro", "answer", answer="<{{#tag.output#}}>"),
        _node("tag", "template-transform", template="ready", variables=[]),
    ])
    edges.extend([
        _edge("start", "intro"),
        _edge("reply", "tag"),
        _edge("tag", "intro"),
    ])
    engine = loads(
        _graph_dsl_for_nodes(nodes=nodes, edges=edges),
        credentials=_openai_credentials(),
        start_inputs={"tail": "tail"},
    )
    file = _make_file()
    engine.runtime_state.variable_pool.add(("draft", "text"), file)
    raw = list(engine.run())
    context = replace(
        EngineEventFilterContext.from_engine(engine),
        file_runtime=_make_file_adapter("filter"),
    )
    with use_workflow_file_runtime(_make_file_adapter("caller")):
        output = list(
            filter_engine_events(raw, context=context, filters=[ResponseStreamFilter()])
        )
    end = next(
        event
        for event in output
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "reply"
    )
    assert end.node_run_result.outputs == {"file": file}
    assert (
        _join_stream_text(output)
        == "<ready>[report.txt](https://filter.example/report.txt)"
    )


@pytest.mark.parametrize("direct", [True, False], ids=["direct", "from-engine"])
def test_unmarked_frame_displays_llm_text_when_it_is_committed(
    monkeypatch: pytest.MonkeyPatch, direct: bool
) -> None:
    _patch_fake_slim_llm(monkeypatch)
    engine = loads(
        _graph_dsl_for_nodes(
            nodes=[
                {"id": "draft", "data": _llm_data()},
                _node("reply", "answer", answer="{{#draft.text#}}"),
            ],
            edges=[_edge("start", "draft"), _edge("draft", "reply")],
        ),
        credentials=_openai_credentials(),
    )
    stream = ResponseStreamFilter()
    context = EngineEventFilterContext.from_engine(engine)
    stream.initialize(replace(context, run_id=None) if direct else context)
    list(stream.on_event(FrameStartedEvent(frame_id="root")))
    value = StringVariable(name="text", selector=["draft", "text"], value="committed")
    commit = FrameVariablesUpdatedEvent(frame_id="root", variables=(value,))
    chunks = [
        c for c in stream.on_event(commit) if isinstance(c, NodeRunStreamChunkEvent)
    ]
    assert [(chunk.selector, chunk.chunk) for chunk in chunks] == [
        (["draft", "text"], "committed")
    ]
