from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from graphon.dsl._provider import canonical_vendor
from graphon.dsl.agent_runtime import SlimAgentNodeRuntime
from graphon.dsl.slim.client import SlimClientConfig
from graphon.nodes.agent.entities import AgentNodeData
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage
from graphon.runtime.variable_pool import VariablePool


def _node_data(
    *,
    provider_name: str = "langgenius/agent/agent",
    strategy: str = "function_calling",
    plugin_id: str = "langgenius/agent:0.0.36@hash",
) -> AgentNodeData:
    return AgentNodeData(
        title="Agent",
        agent_strategy_provider_name=provider_name,
        agent_strategy_name=strategy,
        plugin_unique_identifier=plugin_id,
    )


def _record_invoker(
    chunks: list[Mapping[str, Any]],
    *,
    captured: list[tuple[str, str, Mapping[str, Any]]],
) -> Any:
    def invoker(
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        captured.append((plugin_id, action, dict(data)))
        return list(chunks)

    return invoker


def test_canonical_vendor_extracts_trailing_segment() -> None:
    assert canonical_vendor("langgenius/agent/agent") == "agent"
    assert canonical_vendor("openai") == "openai"
    assert canonical_vendor("//x//") == "x"
    assert canonical_vendor("") is None


def test_invoke_forwards_payload_with_plugin_id_and_strategy(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    runtime = SlimAgentNodeRuntime(
        config=SlimClientConfig(folder=tmp_path),
        action_invoker=_record_invoker([], captured=captured),
    )

    list(
        runtime.invoke(
            node_id="agent",
            node_data=_node_data(),
            agent_strategy_params={"instruction": "test", "query": "hi"},
            variable_pool=VariablePool.from_bootstrap(),
        ),
    )

    assert captured == [
        (
            "langgenius/agent:0.0.36@hash",
            "invoke_agent_strategy",
            {
                "agent_strategy_provider": "agent",
                "agent_strategy": "function_calling",
                "agent_strategy_params": {"instruction": "test", "query": "hi"},
            },
        ),
    ]


def test_invoke_decodes_text_messages_through_shared_decoder(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    runtime = SlimAgentNodeRuntime(
        config=SlimClientConfig(folder=tmp_path),
        action_invoker=_record_invoker(
            [{"type": "text", "message": {"text": "hello"}}],
            captured=captured,
        ),
    )

    messages = list(
        runtime.invoke(
            node_id="agent",
            node_data=_node_data(),
            agent_strategy_params={},
            variable_pool=None,
        ),
    )

    assert len(messages) == 1
    assert messages[0].type == ToolRuntimeMessage.MessageType.TEXT
    assert isinstance(messages[0].message, ToolRuntimeMessage.TextMessage)
    assert messages[0].message.text == "hello"


def test_invoke_uses_per_node_strategy_identifiers(tmp_path: Path) -> None:
    """Each call freshly assembles a SlimAgentStrategyClient from node_data,
    so the same runtime instance can serve different agent nodes.
    """
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    runtime = SlimAgentNodeRuntime(
        config=SlimClientConfig(folder=tmp_path),
        action_invoker=_record_invoker([], captured=captured),
    )

    list(
        runtime.invoke(
            node_id="agent-a",
            node_data=_node_data(
                provider_name="vendor/plugin/react_provider",
                strategy="ReAct",
                plugin_id="vendor/plugin:1.0@aaa",
            ),
            agent_strategy_params={},
            variable_pool=None,
        ),
    )
    list(
        runtime.invoke(
            node_id="agent-b",
            node_data=_node_data(
                provider_name="vendor/plugin/fc_provider",
                strategy="function_calling",
                plugin_id="vendor/plugin:1.0@bbb",
            ),
            agent_strategy_params={},
            variable_pool=None,
        ),
    )

    assert len(captured) == 2
    assert captured[0][0] == "vendor/plugin:1.0@aaa"
    assert captured[0][2]["agent_strategy_provider"] == "react_provider"
    assert captured[0][2]["agent_strategy"] == "ReAct"
    assert captured[1][0] == "vendor/plugin:1.0@bbb"
    assert captured[1][2]["agent_strategy_provider"] == "fc_provider"
    assert captured[1][2]["agent_strategy"] == "function_calling"
