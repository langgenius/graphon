"""Regression coverage for citation metadata survival in LLM node context.

Dify chatflow citations ("Citations and Attributions") are built exclusively
from ``RunRetrieverResourceEvent.retriever_resources`` emitted by
``LLMNode._fetch_context``. The structured path must preserve the knowledge
source metadata attached by the Knowledge Retrieval node, while plain string
context (e.g. a Template/Code node output) legitimately carries none.
See langgenius/dify#43638 for the user-facing symptom this contract guards.
"""

from typing import Any, cast
from unittest.mock import MagicMock

from graphon.node_events.node import RunRetrieverResourceEvent
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.runtime.runtime_state import RuntimeState
from graphon.variables.segments import (
    ArrayAnySegment,
    ArrayObjectSegment,
    StringSegment,
)

from ...helpers import build_init_params, build_variable_pool

_KNOWLEDGE_SOURCE: dict[str, Any] = {
    "metadata": {
        "_source": "knowledge",
        "dataset_id": "ds-1",
        "dataset_name": "Handbook KB",
        "document_id": "doc-1",
        "document_name": "handbook.txt",
        "data_source_type": "upload_file",
        "segment_id": "seg-1",
        "retriever_from": "workflow",
        "score": 0.92,
        "child_chunks": [],
        "segment_hit_count": 3,
        "segment_word_count": 128,
        "segment_position": 2,
        "segment_index_node_hash": None,
        "doc_metadata": None,
        "position": 1,
    },
    "title": "handbook.txt",
    "files": None,
    "content": "The refund policy allows returns within 30 days.",
    "summary": "Refund policy",
}


def _build_context_node(
    *,
    selector: list[str],
    pool_value: object,
) -> LLMNode:
    return LLMNode(
        node_id="llm",
        data=LLMNodeData.model_validate({
            "title": "LLM",
            "model": {
                "provider": "openai",
                "name": "gpt-4o",
                "mode": "chat",
                "completion_params": {},
            },
            "prompt_template": [{"role": "user", "text": "Hello"}],
            "context": {"enabled": True, "variable_selector": selector},
        }),
        init_params=build_init_params(graph_config={"nodes": [], "edges": []}),
        runtime_state=RuntimeState(
            workflow_id="workflow",
            variable_pool=build_variable_pool(
                variables=[(tuple(selector), pool_value)],
            ),
            start_at=0.0,
        ),
        model_instance=cast(
            object,
            MagicMock(provider="openai", model_name="gpt-4o", parameters={}, stop=()),
        ),
        llm_file_saver=MagicMock(),
        prompt_message_serializer=MagicMock(serialize=MagicMock(return_value=[])),
    )


def test_structured_context_preserves_retriever_resources() -> None:
    """Knowledge Retrieval result bound directly to context keeps citation metadata."""
    node = _build_context_node(
        selector=["retrieval", "result"],
        pool_value=ArrayObjectSegment(value=[_KNOWLEDGE_SOURCE]),
    )
    node.bind_execution_id("llm-run")

    events = list(node._fetch_context(node.node_data))

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, RunRetrieverResourceEvent)
    assert len(event.retriever_resources) == 1
    resource = event.retriever_resources[0]
    assert resource["document_name"] == "handbook.txt"
    assert resource["dataset_id"] == "ds-1"
    assert resource["segment_id"] == "seg-1"
    assert resource["retriever_from"] == "workflow"
    assert "refund policy" in event.context.lower()


def test_string_context_emits_no_retriever_resources() -> None:
    """Plain string context (Template/Code node output) carries no citation metadata."""
    node = _build_context_node(
        selector=["template", "output"],
        pool_value=StringSegment(value="plain template output"),
    )
    node.bind_execution_id("llm-run")

    events = list(node._fetch_context(node.node_data))

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, RunRetrieverResourceEvent)
    assert event.retriever_resources == []
    assert event.context == "plain template output"


def test_structured_context_skips_plain_string_items() -> None:
    """Generic arrays keep resources for structured items and text for plain strings."""
    mixed = ArrayAnySegment(
        value=[
            "intro line",
            _KNOWLEDGE_SOURCE,
        ]
    )
    node = _build_context_node(
        selector=["retrieval", "result"],
        pool_value=mixed,
    )
    node.bind_execution_id("llm-run")

    events = list(node._fetch_context(node.node_data))

    assert len(events) == 1
    event = events[0]
    assert len(event.retriever_resources) == 1
    assert event.retriever_resources[0]["document_name"] == "handbook.txt"
    assert "intro line" in event.context
