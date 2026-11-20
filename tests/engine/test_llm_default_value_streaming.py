from collections.abc import Generator, Iterator, Sequence
from typing import override
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
from graphon.engine_events.node import NodeRunStartedEvent, NodeRunStreamChunkEvent
from graphon.entities.base_node_data import BaseNodeData, DefaultValue, RetryConfig
from graphon.enums import ErrorStrategy, WorkflowNodeExecutionStatus
from graphon.graph import Graph
from graphon.model_runtime.entities.llm_entities import LLMResultChunk
from graphon.node_events import (
    NodeEventPayload,
    NodeRunResult,
    StreamChunkEvent,
    StreamCompletedEvent,
)
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.base.node import Node
from graphon.nodes.start import StartNode
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from tests.helpers import build_init_params
from tests.helpers.workflow_events import FakeSlimLLM
from tests.nodes.llm.test_node import _build_llm_node, _stream_chunk, _stream_results


class _HostStartNode(StartNode):
    """A host node that has already finished when the LLM runs."""


class _HostFinalChunkNode(Node[BaseNodeData]):
    node_type = "continue-text"

    @classmethod
    @override
    def version(cls) -> str:
        return "1"

    @override
    def _run(self) -> Generator[NodeEventPayload, None, None]:
        yield StreamChunkEvent(selector=["llm", "text"], chunk="!", is_final=True)
        yield StreamCompletedEvent(
            node_run_result=NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED)
        )


def _run_answer_graph(
    monkeypatch: pytest.MonkeyPatch,
    first_chunk_text: str | None,
    *,
    host_finishes_stream: bool = False,
    retry_succeeds: bool = False,
) -> list[EngineEvent]:
    def interrupted_stream() -> Iterator[LLMResultChunk]:
        if first_chunk_text is not None:
            yield _stream_chunk(first_chunk_text)
            yield _stream_chunk("")
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
    llm_node = _build_llm_node(
        model_instance=model, variables=[(("start", "label"), "old")]
    )
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
        .add_root(_HostStartNode(node_id="start", data=StartNodeData(), **node_args))
        .add_node(llm_node)
    )
    if host_finishes_stream:
        graph.add_node(
            _HostFinalChunkNode(
                node_id="continue", data=BaseNodeData(type="continue-text"), **node_args
            )
        )
    graph.add_node(
        AnswerNode(
            node_id="first",
            data=AnswerNodeData(answer="A[{{#llm.text#}}/{{#start.label#}}]END"),
            **node_args,
        )
    )
    graph.add_node(
        AnswerNode(node_id="next", data=AnswerNodeData(answer="|NEXT"), **node_args)
    )
    graph.add_node(
        VariableAssignerNode(
            node_id="edit",
            data=VariableAssignerNode.validate_node_data({
                "type": "assigner",
                "items": [
                    {
                        "variable_selector": ["start", "label"],
                        "input_type": "constant",
                        "operation": "over-write",
                        "value": "new",
                    }
                ],
            }),
            **node_args,
        )
    )
    engine = Engine(
        graph=graph.build(), runtime_state=llm_node.runtime_state, workers=1
    )
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


def _text_before_node_starts(events: Sequence[EngineEvent], node_id: str) -> str:
    start_index = next(
        index
        for index, event in enumerate(events)
        if isinstance(event, NodeRunStartedEvent) and event.node_id == node_id
    )
    return _streamed_text(events[:start_index])


@pytest.mark.parametrize(
    ("first_chunk_text", "expected_llm_text"),
    [("partial", "partial"), ("", "fallback"), (" ", " ")],
)
def test_answer_stream_continues_when_llm_uses_default_text(
    monkeypatch: pytest.MonkeyPatch, first_chunk_text: str, expected_llm_text: str
) -> None:
    events = _run_answer_graph(monkeypatch, first_chunk_text)
    first_answer = f"A[{expected_llm_text}/old]END"
    expected_before_first_answer = first_answer
    if not first_chunk_text:
        events_without_llm_chunks = _run_answer_graph(monkeypatch, None)
        expected_before_first_answer = _text_before_node_starts(
            events_without_llm_chunks, "first"
        )
    assert _text_before_node_starts(events, "first") == expected_before_first_answer
    assert _text_before_node_starts(events, "next") == f"{first_answer}|NEXT"
    assert _streamed_text(events) == f"{first_answer}|NEXT"
    terminal_event = events[-1]
    assert isinstance(terminal_event, GraphRunPartialSucceededEvent)
    assert terminal_event.outputs["answer"] == "A[fallback/old]END|NEXT"


@pytest.mark.parametrize("first_chunk_text", ["partial", ""])
def test_host_node_can_finish_the_llm_stream(
    monkeypatch: pytest.MonkeyPatch, first_chunk_text: str
) -> None:
    assert (
        _streamed_text(
            _run_answer_graph(monkeypatch, first_chunk_text, host_finishes_stream=True)
        )
        == f"A[{first_chunk_text}!/old]END|NEXT"
    )


def test_successful_retry_can_finish_the_llm_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert (
        _streamed_text(_run_answer_graph(monkeypatch, "partial", retry_succeeds=True))
        == "A[partialok/old]END|NEXT"
    )
