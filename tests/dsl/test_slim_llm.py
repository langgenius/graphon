from __future__ import annotations

import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from threading import Event, Thread, current_thread
from types import SimpleNamespace
from typing import Any

import pytest

import graphon.dsl.slim.llm as slim_llm_module
from graphon.dsl.slim import SlimClientConfig, SlimClientError, SlimLLM
from graphon.model_runtime.entities.llm_entities import LLMResult
from graphon.model_runtime.entities.message_entities import SystemPromptMessage
from graphon.model_runtime.model_providers.base.tokenizers import gpt2_tokenizer


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
        chunk = {
            "delta": {
                "index": 0,
                "message": {"content": "hello"},
            },
        }
        model_parameters = data.get("model_parameters")
        if isinstance(model_parameters, Mapping) and "json_schema" in model_parameters:
            chunk["structured_output"] = {"ok": True}
        return [chunk]


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


def test_slim_llm_passes_merged_parameters_and_json_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _patch_recording_slim_client(monkeypatch)
    llm = _build_llm(tmp_path)
    schema = {"type": "object", "required": ["ok"]}

    result = llm.invoke_llm_with_structured_output(
        prompt_messages=[],
        json_schema=schema,
        model_parameters={"max_tokens": 8},
        stop=None,
        stream=False,
    )

    assert result.structured_output == {"ok": True}
    model_parameters = client.calls[-1][2]["model_parameters"]
    assert model_parameters["temperature"] == pytest.approx(0.2)
    assert model_parameters["max_tokens"] == 8
    assert json.loads(model_parameters["json_schema"]) == schema


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


@pytest.mark.parametrize("backend", ["tiktoken", "transformers"])
def test_slim_llms_lazily_load_and_reuse_separate_tokenizers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    backend: str,
) -> None:
    class Encoder:
        def __init__(self, token_count: int) -> None:
            self.token_count = token_count

        def encode(self, text: str) -> list[int]:
            _ = text
            return [0] * self.token_count

    encoders: list[tuple[str, Encoder]] = []

    def load_encoder(encoder_backend: str) -> Encoder:
        encoder = Encoder(len(encoders) + 1)
        encoders.append((encoder_backend, encoder))
        return encoder

    def load_tiktoken_encoder(name: str) -> Encoder:
        _ = name
        if backend == "transformers":
            msg = "tiktoken unavailable"
            raise RuntimeError(msg)
        return load_encoder("tiktoken")

    def load_transformers_encoder(name: str) -> Encoder:
        _ = name
        return load_encoder("transformers")

    def create_timeout_client(*, config: SlimClientConfig) -> _TimeoutSlimClient:
        _ = config
        return _TimeoutSlimClient()

    monkeypatch.setitem(
        sys.modules, "tiktoken", SimpleNamespace(get_encoding=load_tiktoken_encoder)
    )
    monkeypatch.setitem(
        sys.modules,
        "transformers",
        SimpleNamespace(
            GPT2Tokenizer=SimpleNamespace(from_pretrained=load_transformers_encoder)
        ),
    )
    monkeypatch.setattr(slim_llm_module, "SlimClient", create_timeout_client)
    first = _build_llm(tmp_path)
    second = _build_llm(tmp_path)
    messages = [SystemPromptMessage(content="classify this prompt")]

    assert encoders == []
    assert [
        llm.get_llm_num_tokens(messages) for llm in (first, second, first, second)
    ] == [1, 2, 1, 2]
    assert [encoder_backend for encoder_backend, _ in encoders] == [backend, backend]


def test_tokenizers_initialize_independently_and_reuse_concurrent_loads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_loading = Event()
    release_first = Event()
    repeat_started = Event()
    completed = {name: Event() for name in ("first", "second", "repeat")}
    loaded_by: list[str] = []
    results: dict[str, int | Exception] = {}

    class Encoder:
        def encode(self, text: str) -> list[int]:
            return [0] * len(text)

    def load_encoder() -> Encoder:
        name = current_thread().name
        loaded_by.append(name)
        if name == "first":
            first_loading.set()
            assert release_first.wait(timeout=5)
        return Encoder()

    def count_tokens(tokenizer: gpt2_tokenizer.GPT2Tokenizer) -> None:
        name = current_thread().name
        if name == "repeat":
            repeat_started.set()
        try:
            results[name] = tokenizer.get_num_tokens("prompt")
        except Exception as error:  # ruff:ignore[blind-except]
            results[name] = error
        finally:
            completed[name].set()

    monkeypatch.setattr(gpt2_tokenizer, "_try_load_tiktoken_encoder", load_encoder)
    first = gpt2_tokenizer.GPT2Tokenizer()
    second = gpt2_tokenizer.GPT2Tokenizer()
    threads = [
        Thread(name=name, target=count_tokens, args=(tokenizer,))
        for name, tokenizer in (("first", first), ("second", second), ("repeat", first))
    ]
    threads[0].start()
    try:
        assert first_loading.wait(timeout=2)
        threads[2].start()
        threads[1].start()
        assert repeat_started.wait(timeout=2)
        assert completed["second"].wait(timeout=2)
        assert not completed["repeat"].wait(timeout=0.1)
    finally:
        release_first.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=2)
        assert all(not thread.is_alive() for thread in threads)

    assert loaded_by == ["first", "second"]
    assert results == {"first": 6, "second": 6, "repeat": 6}


def test_slim_llm_token_estimate_returns_zero_for_empty_prompt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def create_timeout_client(*, config: SlimClientConfig) -> _TimeoutSlimClient:
        _ = config
        return _TimeoutSlimClient()

    monkeypatch.setattr(slim_llm_module, "SlimClient", create_timeout_client)

    assert _build_llm(tmp_path).get_llm_num_tokens([]) == 0


def test_slim_llm_token_estimate_uses_length_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def create_timeout_client(*, config: SlimClientConfig) -> _TimeoutSlimClient:
        _ = config
        return _TimeoutSlimClient()

    def fail_encoder_load() -> None:
        msg = "tokenizer unavailable"
        raise RuntimeError(msg)

    monkeypatch.setattr(slim_llm_module, "SlimClient", create_timeout_client)
    monkeypatch.setattr(
        gpt2_tokenizer,
        "_try_load_tiktoken_encoder",
        fail_encoder_load,
    )

    token_count = _build_llm(tmp_path).get_llm_num_tokens([
        SystemPromptMessage(content="x" * 20),
    ])

    assert token_count == 7
