from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.node import ModelInvokeCompletedEvent, StreamCompletedEvent
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params, build_variable_pool


def _build_llm_node() -> LLMNode:
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
            "prompt_template": [
                {
                    "role": "user",
                    "text": "Hello",
                }
            ],
            "context": {"enabled": False},
        }),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []}
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(),
            start_at=0.0,
        ),
        model_instance=MagicMock(
            provider="openai",
            model_name="gpt-4o",
            parameters={},
            stop=(),
        ),
        llm_file_saver=MagicMock(),
        prompt_message_serializer=MagicMock(),
    )


def test_run_emits_model_identity_in_node_result_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _build_llm_node()

    monkeypatch.setattr(node, "_fetch_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_fetch_jinja_inputs", lambda **_: {})
    monkeypatch.setattr(node, "_collect_run_context", lambda **_: iter(()))
    monkeypatch.setattr(
        LLMNode, "fetch_prompt_messages", staticmethod(lambda **_: ([], None))
    )
    monkeypatch.setattr(
        "graphon.nodes.llm.node.LLMNode.invoke_llm",
        lambda **_: iter([
            ModelInvokeCompletedEvent(
                text="Hello back",
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ]),
    )

    events = list(node._run())  # noqa: SLF001
    completed_event = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )

    assert completed_event.node_run_result.inputs["model_provider"] == "openai"
    assert completed_event.node_run_result.inputs["model_name"] == "gpt-4o"
