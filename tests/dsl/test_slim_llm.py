from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest
from pydantic import JsonValue

import graphon.dsl.slim.llm as slim_llm_module
from graphon.dsl.slim import SlimClientConfig, SlimClientError, SlimLLM
from graphon.model_runtime.entities.llm_entities import LLMResult
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    SystemPromptMessage,
    TextPromptMessageContent,
)


class _RecordingSlimClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Mapping[str, Any]]] = []

    def invoke_chunks(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Any]:
        self.calls.append((plugin_id, action, data))
        if action == "get_llm_num_tokens":
            return [{"num_tokens": 7}]
        return [
            {
                "delta": {
                    "index": 0,
                    "message": {"content": "hello"},
                }
            }
        ]


class _FailingSlimClient:
    def invoke_chunks(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Any]:
        _ = plugin_id, action, data
        message = "daemon down"
        raise SlimClientError(message)


class _TimeoutSlimClient:
    def __init__(
        self,
        *,
        message: str = "token counting timed out",
        code: str | None = "killed_by_timeout",
    ) -> None:
        self.message = message
        self.code = code

    def invoke_chunks(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Any]:
        _ = plugin_id, action, data
        raise SlimClientError(
            self.message,
            code=self.code,
            stage="get_llm_num_tokens",
        )


def _patch_recording_slim_client(
    monkeypatch: pytest.MonkeyPatch,
) -> _RecordingSlimClient:
    client = _RecordingSlimClient()

    def slim_client_factory(*, config: SlimClientConfig) -> _RecordingSlimClient:
        _ = config
        return client

    monkeypatch.setattr(slim_llm_module, "SlimClient", slim_client_factory)
    return client


def _build_llm(tmp_path: Path) -> SlimLLM:
    return SlimLLM(
        config=SlimClientConfig(folder=tmp_path),
        plugin_id="author/provider:0.0.1@test",
        provider="provider",
        model_name="chat-model",
        credentials={"api_key": "secret"},
        parameters={"temperature": 0.2},
        stop=["END"],
    )


def test_slim_llm_constructs_slim_client_eagerly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    constructed_configs: list[SlimClientConfig] = []

    def slim_client_factory(*, config: SlimClientConfig) -> _RecordingSlimClient:
        constructed_configs.append(config)
        return _RecordingSlimClient()

    monkeypatch.setattr(slim_llm_module, "SlimClient", slim_client_factory)
    config = SlimClientConfig(folder=tmp_path)

    SlimLLM(
        config=config,
        plugin_id="author/provider:0.0.1@test",
        provider="provider",
        model_name="chat-model",
        credentials={"api_key": "secret"},
    )

    assert constructed_configs == [config]


def test_slim_llm_protects_parameter_and_stop_copies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_recording_slim_client(monkeypatch)
    parameters = {"temperature": 0.2}
    stop = ["END"]
    llm = SlimLLM(
        config=SlimClientConfig(folder=tmp_path),
        plugin_id="author/provider:0.0.1@test",
        provider="provider",
        model_name="chat-model",
        credentials={"api_key": "secret"},
        parameters=parameters,
        stop=stop,
    )

    parameters["temperature"] = 0.8
    stop.append("NEVER")
    returned_parameters = dict(llm.parameters)
    returned_parameters["top_p"] = 0.5
    returned_stop = list(llm.stop or [])
    returned_stop.append("MORE")

    assert llm.parameters == {"temperature": 0.2}
    assert llm.stop == ["END"]


def test_slim_llm_counts_tokens_and_collects_blocking_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _patch_recording_slim_client(monkeypatch)
    llm = _build_llm(tmp_path)

    token_count = llm.get_llm_num_tokens([])
    result = llm.invoke_llm(
        prompt_messages=[],
        model_parameters={"max_tokens": 8},
        tools=None,
        stop=None,
        stream=False,
    )

    assert token_count == 7
    assert isinstance(result, LLMResult)
    assert result.message.content == "hello"
    assert client.calls[0][0] == "author/provider:0.0.1@test"
    assert client.calls[0][1] == "get_llm_num_tokens"
    assert client.calls[1][1] == "invoke_llm"
    assert client.calls[1][2]["model_parameters"] == {
        "temperature": 0.2,
        "max_tokens": 8,
    }


@pytest.mark.parametrize("use_structured_output", [False, True])
@pytest.mark.parametrize("opaque_body", [{"signature": "s"}, {}, [], "", 0, False])
def test_slim_llm_collects_mixed_content_and_latest_opaque_body(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    use_structured_output: bool,
    opaque_body: JsonValue,
) -> None:
    client = _patch_recording_slim_client(monkeypatch)
    block = TextPromptMessageContent(data="", opaque_body={"signature": "block"})
    chunks: list[dict[str, Any]] = [
        {
            "delta": {
                "message": {"content": content, "opaque_body": chunk_opaque_body}
            },
            "structured_output": {"ok": True} if index == 1 else None,
        }
        for index, (content, chunk_opaque_body) in enumerate([
            ("before", {"partial": True}),
            ([block.model_dump()], None),
            ("after", None),
            ("", opaque_body),
            ("", None),
        ])
    ]
    for index, arguments in enumerate(['{"city":', '"Paris"}']):
        chunks[index]["delta"]["message"]["tool_calls"] = [
            {
                "id": "call-1" if index == 0 else "",
                "type": "function",
                "function": {
                    "name": "weather" if index == 0 else "",
                    "arguments": arguments,
                },
            },
        ]
    chunks[0]["system_fingerprint"] = "first"
    chunks[-2]["system_fingerprint"] = ""
    chunks[-1]["delta"]["usage"] = {"prompt_tokens": 3, "completion_tokens": 2}
    monkeypatch.setattr(client, "invoke_chunks", lambda **_: iter(chunks))
    llm = _build_llm(tmp_path)

    if use_structured_output:
        result = llm.invoke_llm_with_structured_output(
            prompt_messages=[],
            json_schema={"type": "object"},
            model_parameters={},
            stop=None,
            stream=False,
        )
        assert result.structured_output == {"ok": True}
    else:
        result = llm.invoke_llm(
            prompt_messages=[],
            model_parameters={},
            tools=None,
            stop=None,
            stream=False,
        )

    assert result.message == AssistantPromptMessage(
        content=[
            TextPromptMessageContent(data="before"),
            block,
            TextPromptMessageContent(data="after"),
        ],
        tool_calls=[
            AssistantPromptMessage.ToolCall(
                id="call-1",
                type="function",
                function=AssistantPromptMessage.ToolCall.ToolCallFunction(
                    name="weather", arguments='{"city":"Paris"}'
                ),
            ),
        ],
        opaque_body=opaque_body,
    )
    assert type(result.message.opaque_body) is type(opaque_body)
    assert (
        result.usage.prompt_tokens,
        result.usage.completion_tokens,
        result.usage.total_tokens,
    ) == (3, 2, 5)
    assert result.system_fingerprint == ""


def test_slim_llm_preserves_slim_client_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def slim_client_factory(*, config: SlimClientConfig) -> _FailingSlimClient:
        _ = config
        return _FailingSlimClient()

    monkeypatch.setattr(slim_llm_module, "SlimClient", slim_client_factory)
    llm = _build_llm(tmp_path)

    with pytest.raises(SlimClientError, match="daemon down"):
        llm.get_llm_num_tokens([])


def test_slim_llm_estimates_tokens_when_slim_token_count_times_out(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def slim_client_factory(*, config: SlimClientConfig) -> _TimeoutSlimClient:
        _ = config
        return _TimeoutSlimClient()

    monkeypatch.setattr(slim_llm_module, "SlimClient", slim_client_factory)
    llm = _build_llm(tmp_path)

    token_count = llm.get_llm_num_tokens([
        SystemPromptMessage(content="classify this short prompt"),
    ])

    assert token_count > 0


def test_slim_llm_estimates_tokens_when_timeout_is_only_in_message(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def slim_client_factory(*, config: SlimClientConfig) -> _TimeoutSlimClient:
        _ = config
        return _TimeoutSlimClient(message="killed by timeout", code=None)

    monkeypatch.setattr(slim_llm_module, "SlimClient", slim_client_factory)
    llm = _build_llm(tmp_path)

    token_count = llm.get_llm_num_tokens([
        SystemPromptMessage(content="classify this short prompt"),
    ])

    assert token_count > 0


def test_slim_llm_token_estimate_returns_zero_for_empty_prompt() -> None:
    assert slim_llm_module._estimate_prompt_message_tokens([]) == 0


def test_slim_llm_token_estimate_uses_length_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_tokenizer(text: str) -> int:
        _ = text
        msg = "tokenizer unavailable"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        slim_llm_module.GPT2Tokenizer,
        "get_num_tokens",
        fail_tokenizer,
    )

    token_count = slim_llm_module._estimate_prompt_message_tokens([
        SystemPromptMessage(content="x" * 20),
    ])

    assert token_count == 7
