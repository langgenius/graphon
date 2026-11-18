"""Stream model tokens and display the complete values each Answer reads."""

from collections import defaultdict
from collections.abc import Iterator, Sequence
from threading import Event
from unittest.mock import Mock

import pytest

from graphon.dsl import loads
from graphon.engine.command import InMemoryChannel, UpdateVariablesCommand
from graphon.engine.filter import EngineEventFilterContext, ResponseStreamFilter
from graphon.engine_events.frame import FrameVariablesUpdatedEvent
from graphon.engine_events.node import (
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.model_runtime.entities.llm_entities import LLMResultChunk
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import Segment
from graphon.variables.variables import StringVariable
from tests.dsl.test_node_factory import (
    _graph_dsl_for_nodes,
    _llm_data,
    _openai_credentials,
    _patch_fake_slim_llm,
)
from tests.engine.test_frame_observations import _node
from tests.nodes.llm.test_node import _llm_result, _stream_chunk, _stream_results


def _two_model_answer_dsl(scope: str, *, edited_node_id: str = "draft") -> str:
    in_iteration = scope != "root"
    nodes = [
        _node(
            name,
            "llm",
            model={"provider": "openai", "name": name, "mode": "chat"},
            prompt_template=[{"role": "user", "text": "hello"}],
            context={"enabled": False},
        )
        for name in ("stable", "draft")
    ]
    nodes.extend([
        _node(
            "edit",
            "assigner",
            version="2",
            items=[
                {
                    "variable_selector": [edited_node_id, "text"],
                    "input_type": "variable",
                    "operation": "over-write",
                    "value": ["items", "item"] if in_iteration else ["start", "label"],
                }
            ],
        ),
        _node(
            "answer",
            "answer",
            answer="{{#stable.text#}}|{{#draft.text#}}/{{#draft.text#}}",
        ),
    ])
    edges = [
        ("stable", "draft"),
        ("draft", "answer"),
        ("draft", "edit"),
        ("edit", "answer"),
    ]
    if in_iteration:
        for node in nodes:
            if scope != "inherited" or node["id"] not in {"stable", "draft"}:
                node["data"]["container_id"] = "items"
        nodes.extend([
            _node(
                "items",
                "iteration",
                start_node_id="body",
                iterator_selector=["start", "labels"],
                output_selector=["answer", "answer"],
                is_parallel=True,
                parallel_nums=2,
            ),
            _node("body", "iteration-start", container_id="items"),
        ])
        if scope == "inherited":
            edges = [
                ("start", "stable"),
                ("stable", "draft"),
                ("draft", "items"),
                ("body", "answer"),
                ("body", "edit"),
                ("edit", "answer"),
            ]
        else:
            edges.extend([("start", "items"), ("body", "stable")])
    else:
        edges.append(("start", "stable"))

    return _graph_dsl_for_nodes(
        nodes=nodes, edges=[{"source": a, "target": b} for a, b in edges]
    )


@pytest.mark.parametrize("scope", ["root", "parallel-items", "inherited"])
def test_answer_waits_for_complete_model_values(
    monkeypatch: pytest.MonkeyPatch, scope: str
) -> None:
    model = _patch_fake_slim_llm(monkeypatch)
    model.responses.update(stable="public", draft="draft")
    engine = loads(
        _two_model_answer_dsl(scope),
        credentials=_openai_credentials(),
        start_inputs={"label": "one", "labels": ["one", "two"]},
    )
    stream = ResponseStreamFilter()
    stream.initialize(EngineEventFilterContext.from_engine(engine))
    text: dict[str, str] = defaultdict(str)
    text_before_answer_start: dict[str, str] = {}
    answers: dict[str, str] = {}
    for event in engine.run():
        if isinstance(event, NodeRunStartedEvent) and event.node_id == "answer":
            text_before_answer_start[event.frame_id] = text[event.frame_id]
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer":
            answers[event.frame_id] = event.node_run_result.outputs["answer"]
        for chunk in stream.on_event(event):
            if isinstance(chunk, NodeRunStreamChunkEvent):
                text[chunk.frame_id] += chunk.chunk
    assert set(answers.values()) == (
        {"public|one/one", "public|two/two"} if scope != "root" else {"public|one/one"}
    )
    assert dict(text) == answers
    assert text_before_answer_start == dict.fromkeys(answers, "")


@pytest.mark.parametrize("workers", [1, 4])
def test_answer_displays_tokens_and_each_read_of_a_complete_result(
    monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    model = _patch_fake_slim_llm(monkeypatch)
    monkeypatch.setattr(
        model,
        "invoke_llm",
        Mock(side_effect=[_stream_results(_stream_chunk("X")), _llm_result("first")]),
    )
    channel = InMemoryChannel()
    engine = loads(
        _two_model_answer_dsl("root", edited_node_id="stable"),
        credentials=_openai_credentials(),
        start_inputs={"label": "edited"},
        command_channel=channel,
        workers=workers,
    )
    replacement = StringVariable(name="text", selector=["draft", "text"], value="next")
    committed = Event()
    get = VariablePool.get

    def edit_after_first_read(
        pool: VariablePool, selector: Sequence[str]
    ) -> Segment | None:
        value = get(pool, selector)
        if (
            pool is engine.runtime_state.variable_pool
            and tuple(selector) == tuple(replacement.selector)
            and not committed.is_set()
        ):
            channel.send_command(UpdateVariablesCommand(updates=[replacement]))
            assert committed.wait(5), "the command must commit before the second read"
        return value

    monkeypatch.setattr(VariablePool, "get", edit_after_first_read)
    stream = ResponseStreamFilter()
    stream.initialize(EngineEventFilterContext.from_engine(engine))
    text = ""
    answer = None
    for event in engine.run():
        if (
            isinstance(event, FrameVariablesUpdatedEvent)
            and replacement in event.variables
        ):
            committed.set()
        text += "".join(
            chunk.chunk
            for chunk in stream.on_event(event)
            if isinstance(chunk, NodeRunStreamChunkEvent)
        )
        if isinstance(event, NodeRunStreamChunkEvent) and event.chunk == "X":
            assert text == "X"
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer":
            answer = event.node_run_result.outputs["answer"]
    assert answer == "edited|first/next"
    assert text == "X|first/next"


@pytest.mark.parametrize("stream_tokens", [True, False], ids=["tokens", "whole"])
def test_missing_read_waits_for_a_parallel_model_to_choose_its_stream(
    monkeypatch: pytest.MonkeyPatch, stream_tokens: bool
) -> None:
    model = _patch_fake_slim_llm(monkeypatch)
    monkeypatch.setattr(
        model,
        "invoke_llm",
        Mock(
            return_value=(
                _stream_results(_stream_chunk("late"))
                if stream_tokens
                else _llm_result("late")
            )
        ),
    )
    engine = loads(
        _graph_dsl_for_nodes(
            nodes=[
                _node("answer", "answer", answer="({{#draft.text#}})!"),
                _node("delay", "template-transform", template="ready", variables=[]),
                {"id": "draft", "data": _llm_data()},
            ],
            edges=[
                {"source": "start", "target": "answer"},
                {"source": "start", "target": "delay"},
                {"source": "delay", "target": "draft"},
            ],
        ),
        credentials=_openai_credentials(),
        workers=1,
    )
    stream = ResponseStreamFilter()
    stream.initialize(EngineEventFilterContext.from_engine(engine))
    text = ""
    saved = None
    for event in engine.run():
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer":
            saved = event.node_run_result.outputs["answer"]
        text += "".join(
            chunk.chunk
            for chunk in stream.on_event(event)
            if isinstance(chunk, NodeRunStreamChunkEvent)
        )
        if isinstance(event, NodeRunStreamChunkEvent) and not event.is_final:
            assert saved == "(draft.text)!"
            assert "late" in text
    assert saved == "(draft.text)!"
    assert text == ("(late)!" if stream_tokens else "()!")


@pytest.mark.parametrize("stream_retry", [False, True], ids=["complete", "tokens"])
def test_iteration_reply_after_an_empty_failed_attempt(
    monkeypatch: pytest.MonkeyPatch, stream_retry: bool
) -> None:
    def failed_attempt() -> Iterator[LLMResultChunk]:
        yield _stream_chunk("")
        msg = "retry after empty delta"
        raise ValueError(msg)

    model = _patch_fake_slim_llm(monkeypatch)
    retry = (
        _stream_results(_stream_chunk("next")) if stream_retry else _llm_result("next")
    )
    monkeypatch.setattr(
        model, "invoke_llm", Mock(side_effect=[failed_attempt(), retry])
    )
    engine = loads(
        _graph_dsl_for_nodes(
            nodes=[
                _node(
                    "items",
                    "iteration",
                    start_node_id="body",
                    iterator_selector=["start", "labels"],
                    output_selector=["answer", "answer"],
                ),
                _node("body", "iteration-start", container_id="items"),
                _node(
                    "llm",
                    "llm",
                    container_id="items",
                    model={"provider": "openai", "name": "test", "mode": "chat"},
                    prompt_template=[{"role": "user", "text": "hello"}],
                    context={"enabled": False},
                    retry_config={
                        "retry_enabled": True,
                        "max_retries": 1,
                        "retry_interval": 0,
                    },
                ),
                _node(
                    "answer",
                    "answer",
                    container_id="items",
                    answer="Reply: {{#llm.text#}}!",
                ),
            ],
            edges=[
                {"source": a, "target": b}
                for a, b in [("start", "items"), ("body", "llm"), ("llm", "answer")]
            ],
        ),
        credentials=_openai_credentials(),
        start_inputs={"labels": ["one"]},
    )
    stream = ResponseStreamFilter()
    stream.initialize(EngineEventFilterContext.from_engine(engine))
    text = []
    answers = []
    retry_chunks = []
    for event in engine.run():
        if isinstance(event, NodeRunSucceededEvent) and event.node_id == "answer":
            answers.append(event.node_run_result.outputs["answer"])
        text.extend(
            chunk.chunk
            for chunk in stream.on_event(event)
            if isinstance(chunk, NodeRunStreamChunkEvent)
        )
        if isinstance(event, NodeRunStreamChunkEvent) and event.retry_index == 1:
            retry_chunks.append((event.chunk, event.is_final))
            if not event.is_final:
                assert "".join(text) == "Reply: next"
    assert retry_chunks == (
        [("next", False), ("", True)] if stream_retry else [("next", True)]
    )
    assert answers == ["Reply: next!"]
    assert "".join(text) == answers[0]
