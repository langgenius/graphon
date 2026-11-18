import json
from pathlib import Path

import pytest

from graphon.engine import Engine
from graphon.engine.filter import ResponseStreamFilter
from graphon.engine_events.node import (
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.graph import Graph
from graphon.model_runtime.entities.llm_entities import (
    LLMPollingResult,
    LLMPollingStatus,
)
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.answer.entities import AnswerNodeData
from graphon.nodes.human_input.entities import HITLCallback, HumanInputNodeData
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.nodes.llm import LLMNode
from graphon.nodes.start import StartNode
from graphon.nodes.start.entities import StartNodeData
from graphon.nodes.variable_assigner.v2.node import VariableAssignerNode
from graphon.runtime.runtime_state import RuntimeState
from tests.engine.test_response_stream_pause import (
    _capture_pause,
    _request_hitl_pause,
    _run_with_response_filter,
)
from tests.engine.test_runtime_state_serialization import _completed_hitl
from tests.helpers import build_init_params
from tests.nodes.llm.test_node import (
    _build_llm_answer_engine,
    _build_llm_node,
    _llm_result,
    _PollingLLM,
)


def _queued_reply_engine(llm: LLMNode, callback: HITLCallback) -> Engine:
    args = {"init_params": build_init_params(), "runtime_state": llm.runtime_state}
    graph = Graph.new().add_root(
        StartNode(node_id="start", data=StartNodeData(title="Start"), **args)
    )
    graph.add_node(llm)
    graph.add_node(
        VariableAssignerNode(
            node_id="prepare",
            data=VariableAssignerNode.validate_node_data({
                "title": "Prepare",
                "items": [
                    {
                        "variable_selector": ["llm", "text"],
                        "operation": "over-write",
                        "input_type": "variable",
                        "value": ["start", "replacement"],
                    }
                ],
            }),
            **args,
        )
    )
    graph.add_node(
        AnswerNode(
            node_id="queued",
            data=AnswerNodeData(answer="{{#start.left#}}{{#llm.text#}}"),
            **args,
        )
    )
    graph.add_node(
        VariableAssignerNode(
            node_id="edit",
            data=VariableAssignerNode.validate_node_data({
                "title": "Edit",
                "items": [
                    {
                        "variable_selector": selector,
                        "input_type": "constant",
                        "operation": "over-write",
                        "value": "after",
                    }
                    for selector in (
                        ["llm", "text"],
                        ["start", "left"],
                        ["start", "replacement"],
                    )
                ],
            }),
            **args,
        )
    )
    graph.add_node(
        HumanInputNode(
            node_id="human",
            data=HumanInputNodeData(title="Approve"),
            hitl_callback=callback,
            **args,
        )
    )
    graph.add_node(
        AnswerNode(
            node_id="first", data=AnswerNodeData(answer="[{{#human.answer#}}]"), **args
        )
    )
    graph.connect(tail="start", head="first")
    return Engine(graph=graph.build(), runtime_state=llm.runtime_state, workers=1)


def test_resume_displays_the_value_a_queued_answer_read_before_an_overwrite() -> None:
    model = _PollingLLM([
        LLMPollingResult(status=LLMPollingStatus.SUCCEEDED, result=_llm_result("raw"))
    ])
    llm = _build_llm_node(
        model_instance=model,
        variables=[(("start", "left"), "left"), (("start", "replacement"), "before")],
    )
    engine = _queued_reply_engine(llm, _request_hitl_pause)
    saved = _capture_pause(engine, ResponseStreamFilter())
    assert "queued" in saved.completed_nodes
    assert "".join(chunk.chunk for chunk in saved.chunks) == "["

    restored = ResponseStreamFilter()
    restored.loads(saved.response_filter)
    engine = _queued_reply_engine(
        _build_llm_node(
            model_instance=model,
            runtime_state=RuntimeState.from_snapshot(saved.runtime),
        ),
        lambda _: _completed_hitl("ready"),
    )
    events = _run_with_response_filter(engine, restored)
    chunks = [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]
    assert "".join(chunk.chunk for chunk in chunks) == "ready]afterbefore"
    assert chunks[-1].id == saved.started_nodes["llm"]
    assert chunks[-1].is_final
    assert len(model.start_calls) == 1


@pytest.mark.parametrize("writer_revision", ["803dcac", "eb75e38"])
def test_resume_keeps_the_old_stream_cursor_when_the_queued_reads_are_ambiguous(
    writer_revision: str,
) -> None:
    # Both writers saved the same pair for ("A", "BC") and ("AB", "C").
    # Their readers displayed the overwritten left value and the empty LLM marker.
    checkpoint = json.loads(
        (
            Path(__file__).with_name("fixtures") / "llm_ambiguous_queued_reads.json"
        ).read_text()
    )
    model = _PollingLLM([])
    llm = _build_llm_node(
        model_instance=model,
        runtime_state=RuntimeState.from_snapshot(checkpoint["runtime"]),
    )
    engine = _queued_reply_engine(llm, lambda _: _completed_hitl("ready"))
    restored = ResponseStreamFilter()
    restored.loads(checkpoint["filters"][writer_revision])
    events = _run_with_response_filter(engine, restored)
    chunks = [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]
    assert [chunk.chunk for chunk in chunks] == ["ready", "]", "after", ""]
    assert chunks[-1].selector == ["llm", "text"]
    assert chunks[-1].is_final
    assert not model.start_calls


def _leaf_reply_engine(llm: LLMNode) -> Engine:
    args = {"init_params": build_init_params(), "runtime_state": llm.runtime_state}

    def overwrite(
        node_id: str, selector: list[str], value: str
    ) -> VariableAssignerNode:
        return VariableAssignerNode(
            node_id=node_id,
            data=VariableAssignerNode.validate_node_data({
                "title": node_id,
                "items": [
                    {
                        "variable_selector": selector,
                        "input_type": "constant",
                        "operation": "over-write",
                        "value": value,
                    }
                ],
            }),
            init_params=build_init_params(),
            runtime_state=llm.runtime_state,
        )

    graph = Graph.new().add_root(
        StartNode(node_id="start", data=StartNodeData(title="Start"), **args)
    )
    graph.add_node(llm)
    graph.add_node(
        AnswerNode(
            node_id="queued",
            data=AnswerNodeData(answer="{{#llm.text#}}/{{#start.tail#}}"),
            **args,
        )
    )
    graph.add_node(overwrite("edit", ["llm", "text"], "after"), from_node_id="llm")
    graph.add_node(
        HumanInputNode(
            node_id="human",
            data=HumanInputNodeData(title="human"),
            hitl_callback=lambda _: _completed_hitl("ready"),
            **args,
        )
    )
    graph.add_node(
        AnswerNode(
            node_id="first", data=AnswerNodeData(answer="[{{#human.answer#}}]"), **args
        )
    )
    graph.add_node(overwrite("post", ["start", "tail"], "late"))
    graph.connect(tail="start", head="first")
    return Engine(graph=graph.build(), runtime_state=llm.runtime_state, workers=1)


@pytest.mark.parametrize("writer_revision", ["803dcac", "eb75e38"])
@pytest.mark.parametrize("phase", ["completed", "pending"])
def test_resume_a_queued_leaf_answer(writer_revision: str, phase: str) -> None:
    # The original writers paused at human approval or while the model polled.
    # In both checkpoints the leaf was TAKEN and the filter had displayed "[".
    checkpoint = json.loads(
        (
            Path(__file__).with_name("fixtures") / "llm_leaf_answer_pauses.json"
        ).read_text()
    )[phase]
    state = RuntimeState.from_snapshot(checkpoint["runtime"])
    if phase == "pending":
        # Release the first reply on resume, before the queued Answer starts.
        state.variable_pool.add(("human", "answer"), "host")
    model = _PollingLLM([])
    engine = _leaf_reply_engine(
        _build_llm_node(model_instance=model, runtime_state=state)
    )
    restored = ResponseStreamFilter()
    restored.loads(checkpoint["filters"][writer_revision])
    events = _run_with_response_filter(engine, restored)
    text = "".join(
        event.chunk for event in events if isinstance(event, NodeRunStreamChunkEvent)
    )
    assert text == ("ready]/early" if phase == "completed" else "host]draft/early")
    if phase == "completed":
        approval_index = next(
            i
            for i, event in enumerate(events)
            if isinstance(event, NodeRunSucceededEvent) and event.node_id == "human"
        )
        released = [
            event
            for event in events[:approval_index]
            if isinstance(event, NodeRunStreamChunkEvent)
        ]
        assert [chunk.chunk for chunk in released] == ["ready", "]", "", "/", "early"]
        assert released[2].selector == ["llm", "text"]
        assert released[2].is_final
    else:
        answer_start_index = next(
            i
            for i, event in enumerate(events)
            if isinstance(event, NodeRunStartedEvent) and event.node_id == "queued"
        )
        assert [
            event.chunk
            for event in events[:answer_start_index]
            if isinstance(event, NodeRunStreamChunkEvent)
        ] == ["host", "]"]
    assert not model.start_calls


@pytest.mark.parametrize("writer_revision", ["eb75e38", "305c9d6"])
def test_resume_after_a_whole_retry_without_displaying_the_failed_tokens(
    writer_revision: str,
) -> None:
    # Both writers buffered "hel" from the failed attempt before approval.
    # The retry closed with either an empty final or its whole text "hello".
    checkpoint = json.loads(
        (
            Path(__file__).with_name("fixtures") / "llm_whole_retry_pause.json"
        ).read_text()
    )
    model = _PollingLLM([])
    llm = _build_llm_node(
        model_instance=model,
        runtime_state=RuntimeState.from_snapshot(checkpoint["runtime"]),
    )
    engine = _build_llm_answer_engine(
        llm,
        "{{#llm.text#}}/{{#llm.text#}}",
        replacement="edited",
        approval=lambda _: _completed_hitl("ready"),
    )
    restored = ResponseStreamFilter()
    restored.loads(checkpoint["filters"][writer_revision])
    events = _run_with_response_filter(engine, restored)
    chunks = [event for event in events if isinstance(event, NodeRunStreamChunkEvent)]
    assert "".join(chunk.chunk for chunk in chunks) == "edited/edited"
    assert [chunk.retry_index for chunk in chunks if chunk.node_id == "llm"] == [1, 1]
    assert not model.start_calls
