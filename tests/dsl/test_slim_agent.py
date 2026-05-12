from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from graphon.dsl.slim.agent import (
    AgentRuntimeMessage,
    SlimAgentStrategyClient,
    SlimAgentStrategyError,
)
from graphon.dsl.slim.client import SlimClient, SlimClientConfig, SlimClientError
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage


def _config(tmp_path: Path) -> SlimClientConfig:
    return SlimClientConfig(folder=tmp_path)


def _record_invoker(
    chunks: Iterable[Mapping[str, Any]],
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


def test_invoke_passes_plugin_id_action_and_payload(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="langgenius/agent:0.0.36@hash",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker([], captured=captured),
    )

    list(client.invoke(agent_strategy_params={"instruction": "test", "query": "hi"}))

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


def test_invoke_decodes_text_message(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker(
            [{"type": "text", "message": {"text": "hello"}}],
            captured=captured,
        ),
    )

    messages = list(client.invoke(agent_strategy_params={}))

    assert len(messages) == 1
    message = messages[0]
    assert message.type == ToolRuntimeMessage.MessageType.TEXT
    assert isinstance(message.message, ToolRuntimeMessage.TextMessage)
    assert message.message.text == "hello"


def test_invoke_decodes_mixed_message_stream(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="ReAct",
        action_invoker=_record_invoker(
            [
                {
                    "type": "log",
                    "message": {
                        "id": "log-1",
                        "label": "thought",
                        "status": "start",
                        "data": {"reasoning": "calling tool"},
                    },
                },
                {
                    "type": "json",
                    "message": {"json_object": {"result": 42}},
                },
                {"type": "text", "message": {"text": "done"}},
            ],
            captured=captured,
        ),
    )

    messages = list(client.invoke(agent_strategy_params={}))

    assert [m.type for m in messages] == [
        ToolRuntimeMessage.MessageType.LOG,
        ToolRuntimeMessage.MessageType.JSON,
        ToolRuntimeMessage.MessageType.TEXT,
    ]
    log_message = messages[0].message
    assert isinstance(log_message, ToolRuntimeMessage.LogMessage)
    assert log_message.label == "thought"
    assert log_message.data == {"reasoning": "calling tool"}
    json_message = messages[1].message
    assert isinstance(json_message, ToolRuntimeMessage.JsonMessage)
    assert json_message.json_object == {"result": 42}


def test_invoke_propagates_meta(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker(
            [
                {
                    "type": "text",
                    "message": {"text": "hi"},
                    "meta": {"latency": 12, "tokens": 5},
                },
            ],
            captured=captured,
        ),
    )

    messages = list(client.invoke(agent_strategy_params={}))

    assert messages[0].meta == {"latency": 12, "tokens": 5}


def test_invoke_raises_on_unknown_message_type(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker(
            [{"type": "ufo", "message": {"text": "??"}}],
            captured=captured,
        ),
    )

    with pytest.raises(SlimAgentStrategyError, match="message type 'ufo'"):
        list(client.invoke(agent_strategy_params={}))


def test_invoke_raises_when_action_invoker_yields_slim_client_error(
    tmp_path: Path,
) -> None:
    error_message = "daemon offline"

    def failing_invoker(
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        _ = plugin_id, action, data
        raise SlimClientError(error_message)

    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=failing_invoker,
    )

    # SlimClientError raised inside the invoker propagates directly when an
    # action_invoker is injected (it bypasses the SlimClient bridge). This
    # captures the dependency-injection contract: callers can raise any error
    # they want from the invoker.
    with pytest.raises(SlimClientError, match="daemon offline"):
        list(client.invoke(agent_strategy_params={}))


def test_invoke_supports_partial_consumption(tmp_path: Path) -> None:
    """``invoke`` is a lazy generator — partial consumption is supported.

    The caller may take only the first ``N`` chunks and close the generator;
    the upstream action invoker must be a coroutine-aware generator that
    receives ``GeneratorExit`` cleanly when ``close()`` is called.
    """
    chunks_produced: list[int] = []

    def lazy_invoker(
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        _ = plugin_id, action, data
        for index in range(5):
            chunks_produced.append(index)
            yield {"type": "text", "message": {"text": f"chunk-{index}"}}

    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=lazy_invoker,
    )

    iterator = client.invoke(agent_strategy_params={})
    first = next(iterator)
    iterator.close()

    assert isinstance(first.message, ToolRuntimeMessage.TextMessage)
    assert first.message.text == "chunk-0"
    # Only the first chunk was produced before close() — confirms laziness.
    assert chunks_produced == [0]


def test_invoke_passes_through_empty_stream(tmp_path: Path) -> None:
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker([], captured=captured),
    )

    messages = list(client.invoke(agent_strategy_params={"x": 1}))

    assert messages == []
    assert len(captured) == 1


def test_invoke_isolates_params_from_caller_mutations(tmp_path: Path) -> None:
    """Caller-side mutation of ``agent_strategy_params`` after ``invoke`` must
    not leak into the data sent to the action invoker.

    Confirms the shallow ``dict(...)`` copy in ``SlimAgentStrategyClient.invoke``
    is sufficient defensive isolation for the top-level mapping. (Deep copies
    of nested mutable values are deliberately not made — callers must not
    mutate nested values mid-flight either, but that is out of contract.)
    """
    captured: list[tuple[str, str, Mapping[str, Any]]] = []
    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        action_invoker=_record_invoker([], captured=captured),
    )
    params: dict[str, Any] = {"instruction": "original", "max_iterations": 3}

    list(client.invoke(agent_strategy_params=params))
    params["instruction"] = "mutated-after-invoke"
    params["max_iterations"] = 99
    params["extra"] = "should-not-appear"

    assert captured[0][2]["agent_strategy_params"] == {
        "instruction": "original",
        "max_iterations": 3,
    }


def test_invoke_via_client_wraps_slim_client_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the real ``SlimClient`` path raises ``SlimClientError`` (for any
    reason — subprocess failure, payload serialization rejection, malformed
    chunks), ``SlimAgentStrategyClient`` wraps it into
    ``SlimAgentStrategyError``.

    This is the counterpart to
    ``test_invoke_raises_when_action_invoker_yields_slim_client_error``: the
    two paths have intentionally different error-handling contracts because
    the ``action_invoker`` is caller-owned test infrastructure that should
    propagate its own errors unchanged, while the ``SlimClient`` path is
    library-owned and normalizes errors to a single typed boundary.
    """
    fake_binary = tmp_path / "fake-slim"
    fake_binary.write_text("#!/bin/sh\nexit 0\n")
    fake_binary.chmod(0o755)
    monkeypatch.setenv("SLIM_BINARY_PATH", str(fake_binary))

    error_message = "slim subprocess crashed mid-payload"

    def failing_invoke_chunks(
        self: SlimClient,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Any]:
        _ = self, plugin_id, action, data
        raise SlimClientError(error_message)

    monkeypatch.setattr(SlimClient, "invoke_chunks", failing_invoke_chunks)

    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
        # action_invoker is None -> go through the real SlimClient path
    )

    with pytest.raises(SlimAgentStrategyError, match=error_message):
        list(client.invoke(agent_strategy_params={}))


def test_invoke_via_client_wraps_non_mapping_chunk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``_client`` path defends against non-Mapping chunk payloads by
    raising ``SlimAgentStrategyError``. The action_invoker path returns
    ``Iterable[Mapping[str, Any]]`` and has no such check (its type is the
    contract).
    """
    fake_binary = tmp_path / "fake-slim"
    fake_binary.write_text("#!/bin/sh\nexit 0\n")
    fake_binary.chmod(0o755)
    monkeypatch.setenv("SLIM_BINARY_PATH", str(fake_binary))

    def bogus_invoke_chunks(
        self: SlimClient,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Any]:
        _ = self, plugin_id, action, data
        yield "not-a-mapping"

    monkeypatch.setattr(SlimClient, "invoke_chunks", bogus_invoke_chunks)

    client = SlimAgentStrategyClient(
        config=_config(tmp_path),
        plugin_id="plugin",
        agent_strategy_provider="agent",
        agent_strategy="function_calling",
    )

    with pytest.raises(
        SlimAgentStrategyError, match="Unexpected slim agent_strategy chunk"
    ):
        list(client.invoke(agent_strategy_params={}))


def test_agent_runtime_message_is_tool_runtime_message_alias() -> None:
    """The ``AgentRuntimeMessage`` PEP 695 alias resolves to
    ``ToolRuntimeMessage`` at runtime.

    PEP 695 ``type X = Y`` creates a ``TypeAliasType`` wrapper rather than
    rebinding ``X`` to ``Y`` directly, so ``is`` does not hold; the underlying
    type is exposed via ``__value__``. This is the semantic-naming contract
    that PR-B and other agent consumers depend on.
    """
    assert AgentRuntimeMessage.__value__ is ToolRuntimeMessage
