from collections.abc import Iterator, Sequence
from unittest.mock import Mock

import pytest

from graphon.engine import Engine
from graphon.engine.filter import (
    EngineEventFilterContext,
    ResponseStreamFilter,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import GraphRunPartialSucceededEvent
from graphon.engine_events.node import (
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
)
from graphon.entities.base_node_data import DefaultValue, RetryConfig
from graphon.enums import ErrorStrategy
from graphon.graph import Graph
from graphon.model_runtime.entities.llm_entities import LLMResultChunk
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.start import StartNode
from graphon.nodes.start.entities import StartNodeData
from tests.helpers import build_init_params
from tests.helpers.workflow_events import FakeSlimLLM
from tests.nodes.llm.test_node import _build_llm_node, _stream_chunk, _stream_results


def _run_answer_graph(
    monkeypatch: pytest.MonkeyPatch,
    first_chunk_text: str | None,
    *,
    retry_succeeds: bool = False,
    answer_template: str = "A[{{#llm.text#}}]END",
) -> list[EngineEvent]:
    def interrupted_stream() -> Iterator[LLMResultChunk]:
        if first_chunk_text is not None:
            yield _stream_chunk(first_chunk_text)
        msg = "disconnected"
        raise OSError(msg)

    model = FakeSlimLLM(
        config=None,
        plugin_id="test",
        provider="openai",
        model_name="chat",
        credentials={},
    )
    invoke_llm = Mock(
        side_effect=[
            interrupted_stream(),
            OSError("retry failed"),
            _stream_results(_stream_chunk("ok"))
            if retry_succeeds
            else OSError("retries exhausted"),
        ]
    )
    monkeypatch.setattr(model, "invoke_llm", invoke_llm)
    llm_node = _build_llm_node(model_instance=model)
    llm_node.node_data.retry_config = RetryConfig(
        retry_enabled=True, max_retries=2, retry_interval=0
    )
    llm_node.node_data.error_strategy = ErrorStrategy.DEFAULT_VALUE
    llm_node.node_data.default_value = [
        DefaultValue(type="string", key="text", value="fallback")
    ]
    node_args = {
        "init_params": build_init_params(),
        "runtime_state": llm_node.runtime_state,
    }
    graph = (
        Graph
        .new()
        .add_root(StartNode(node_id="start", data=StartNodeData(), **node_args))
        .add_node(llm_node)
        .add_node(
            AnswerNode(
                node_id="first",
                data=AnswerNodeData(answer=answer_template),
                **node_args,
            )
        )
        .add_node(
            AnswerNode(node_id="next", data=AnswerNodeData(answer="|NEXT"), **node_args)
        )
        .build()
    )
    engine = Engine(graph=graph, runtime_state=llm_node.runtime_state, workers=1)
    events = list(
        filter_engine_events(
            engine.run(),
            context=EngineEventFilterContext.from_engine(engine),
            filters=[ResponseStreamFilter()],
        )
    )
    assert invoke_llm.call_count == 3
    return events


def _streamed_text(events: Sequence[EngineEvent]) -> str:
    return "".join(
        event.chunk for event in events if isinstance(event, NodeRunStreamChunkEvent)
    )


@pytest.mark.parametrize(
    ("first_chunk_text", "expected_llm_text"),
    [("partial", "partial"), ("", "fallback"), (None, "fallback"), (" ", " ")],
)
def test_llm_default_finishes_the_answer_after_retries(
    monkeypatch: pytest.MonkeyPatch,
    first_chunk_text: str | None,
    expected_llm_text: str,
) -> None:
    events = _run_answer_graph(monkeypatch, first_chunk_text)
    first_answer = f"A[{expected_llm_text}]END"
    next_start_index = next(
        index
        for index, event in enumerate(events)
        if isinstance(event, NodeRunStartedEvent) and event.node_id == "next"
    )
    assert first_answer in _streamed_text(events[:next_start_index])
    assert _streamed_text(events) == f"{first_answer}|NEXT"
    terminal_event = events[-1]
    assert isinstance(terminal_event, GraphRunPartialSucceededEvent)
    assert terminal_event.outputs["answer"] == "A[fallback]END|NEXT"


def test_repeated_references_render_partial_text_when_llm_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _run_answer_graph(
        monkeypatch, "partial", answer_template="{{#llm.text#}}/{{#llm.text#}}"
    )
    assert _streamed_text(events) == "partial/partial|NEXT"


def test_successful_retry_appends_to_the_partial_llm_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert (
        _streamed_text(_run_answer_graph(monkeypatch, "partial", retry_succeeds=True))
        == "A[partialok]END|NEXT"
    )
