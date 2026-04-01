from __future__ import annotations

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from graphon.model_runtime.entities.llm_entities import LLMResultWithStructuredOutput
from graphon.model_runtime.entities.message_entities import (
    PromptMessageTool,
    UserPromptMessage,
)
from graphon.model_runtime.entities.model_entities import ModelType
from graphon.model_runtime.slim import (
    SlimConfig,
    SlimLocalSettings,
    SlimPreparedLLM,
    SlimProviderBinding,
    SlimRuntime,
)


@pytest.fixture(autouse=True)
def clear_slim_binary_path_env(monkeypatch) -> None:
    monkeypatch.delenv("SLIM_BINARY_PATH", raising=False)


def _write_fake_plugin(plugin_root: Path) -> None:
    (plugin_root / "_assets").mkdir(parents=True, exist_ok=True)
    (plugin_root / "provider").mkdir(parents=True, exist_ok=True)
    (plugin_root / "models" / "llm").mkdir(parents=True, exist_ok=True)

    (plugin_root / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            plugins:
              models:
                - provider/provider.yaml
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "provider" / "provider.yaml").write_text(
        textwrap.dedent(
            """
            provider: fake-provider
            label:
              en_US: Fake Provider
            description:
              en_US: Fake provider for tests.
            icon_small:
              en_US: icon.svg
            supported_model_types:
              - llm
            configurate_methods:
              - predefined-model
            provider_credential_schema:
              credential_form_schemas:
                - variable: api_key
                  label:
                    en_US: API Key
                  type: secret-input
                  required: true
            model_credential_schema:
              model:
                label:
                  en_US: Model
              credential_form_schemas:
                - variable: api_key
                  label:
                    en_US: API Key
                  type: secret-input
                  required: true
            models:
              llm:
                predefined:
                  - models/llm/*.yaml
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "models" / "llm" / "fake-chat.yaml").write_text(
        textwrap.dedent(
            """
            model: fake-chat
            label:
              en_US: Fake Chat
            model_type: llm
            fetch_from: predefined-model
            model_properties:
              mode: chat
              context_size: 8192
            parameter_rules:
              - name: temperature
                use_template: temperature
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "_assets" / "icon.svg").write_text(
        "<svg xmlns='http://www.w3.org/2000/svg'></svg>\n",
        encoding="utf-8",
    )


def _write_multi_provider_plugin(plugin_root: Path) -> None:
    (plugin_root / "_assets").mkdir(parents=True, exist_ok=True)
    (plugin_root / "provider").mkdir(parents=True, exist_ok=True)
    (plugin_root / "models" / "llm").mkdir(parents=True, exist_ok=True)

    (plugin_root / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            plugins:
              models:
                - provider/first.yaml
                - provider/second.yaml
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (plugin_root / "_assets" / "icon.svg").write_text(
        "<svg xmlns='http://www.w3.org/2000/svg'></svg>\n",
        encoding="utf-8",
    )

    for provider_name, label, provider_file, model_file, model_name in (
        (
            "fake-provider",
            "Fake Provider",
            "provider/first.yaml",
            "models/llm/fake-chat.yaml",
            "fake-chat",
        ),
        (
            "other-provider",
            "Other Provider",
            "provider/second.yaml",
            "models/llm/other-chat.yaml",
            "other-chat",
        ),
    ):
        (plugin_root / provider_file).write_text(
            textwrap.dedent(
                f"""
                provider: {provider_name}
                label:
                  en_US: {label}
                description:
                  en_US: Provider for tests.
                icon_small:
                  en_US: icon.svg
                supported_model_types:
                  - llm
                configurate_methods:
                  - predefined-model
                provider_credential_schema:
                  credential_form_schemas:
                    - variable: api_key
                      label:
                        en_US: API Key
                      type: secret-input
                      required: true
                model_credential_schema:
                  model:
                    label:
                      en_US: Model
                  credential_form_schemas:
                    - variable: api_key
                      label:
                        en_US: API Key
                      type: secret-input
                      required: true
                models:
                  llm:
                    predefined:
                      - {model_file}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        (plugin_root / model_file).write_text(
            textwrap.dedent(
                f"""
                model: {model_name}
                label:
                  en_US: {label} Model
                model_type: llm
                fetch_from: predefined-model
                model_properties:
                  mode: chat
                  context_size: 8192
                parameter_rules:
                  - name: temperature
                    use_template: temperature
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )


def _write_fake_slim(binary_path: Path) -> None:
    binary_path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import sys

            def emit(event, data=None):
                payload = {"event": event}
                if data is not None:
                    payload["data"] = data
                print(json.dumps(payload), flush=True)

            args = sys.argv[1:]
            action = args[args.index("-action") + 1]
            request = json.loads(sys.stdin.read())
            data = request["data"]
            usage = {
                "prompt_tokens": 2,
                "prompt_unit_price": "0.0",
                "prompt_price_unit": "0.0",
                "prompt_price": "0.0",
                "completion_tokens": 2,
                "completion_unit_price": "0.0",
                "completion_price_unit": "0.0",
                "completion_price": "0.0",
                "total_tokens": 4,
                "total_price": "0.0",
                "currency": "USD",
                "latency": 0.01,
            }

            if "tenant_id" in request or "user_id" in request:
                emit(
                    "error",
                    {
                        "code": "UNEXPECTED_REQUEST_FIELD",
                        "message": "tenant_id and user_id are not supported",
                    },
                )
                sys.exit(1)

            if "tenant_id" in data or "user_id" in data:
                emit(
                    "error",
                    {
                        "code": "UNEXPECTED_REQUEST_FIELD",
                        "message": "tenant_id and user_id are not supported",
                    },
                )
                sys.exit(1)

            if action == "validate_provider_credentials":
                emit("chunk", {"result": True, "credentials": data["credentials"]})
                emit("done")
            elif action == "validate_model_credentials":
                emit("chunk", {"result": True, "credentials": data["credentials"]})
                emit("done")
            elif action == "get_llm_num_tokens":
                emit("chunk", {"num_tokens": 42})
                emit("done")
            elif action == "get_ai_model_schemas":
                emit(
                    "chunk",
                    {
                        "model_schema": {
                            "model": data["model"],
                            "label": {"en_US": "Custom Model"},
                            "model_type": "llm",
                            "fetch_from": "customizable-model",
                            "model_properties": {"mode": "chat", "context_size": 4096},
                            "parameter_rules": [],
                        }
                    },
                )
                emit("done")
            elif action == "invoke_llm":
                emit("message", {"stage": "init", "message": "ready"})
                if data["model_parameters"].get("json_schema"):
                    emit(
                        "chunk",
                        {
                            "delta": {
                                "index": 0,
                                "message": {"role": "assistant", "content": "done"},
                                "usage": usage,
                                "finish_reason": "stop",
                            },
                            "structured_output": {"answer": "structured"},
                        },
                    )
                elif data["tools"]:
                    emit(
                        "chunk",
                        {
                            "delta": {
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "tool_calls": [
                                        {
                                            "id": "call-1",
                                            "type": "function",
                                            "function": {
                                                "name": "extract",
                                                "arguments": "{\\"query\\": \\"Hel",
                                            },
                                        }
                                    ],
                                },
                            }
                        },
                    )
                    emit(
                        "chunk",
                        {
                            "delta": {
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "tool_calls": [
                                        {
                                            "id": "call-1",
                                            "type": "function",
                                            "function": {
                                                "name": "",
                                                "arguments": "lo\\"}",
                                            },
                                        }
                                    ],
                                },
                                "usage": usage,
                                "finish_reason": "tool_calls",
                            }
                        },
                    )
                else:
                    emit(
                        "chunk",
                        {
                            "delta": {
                                "index": 0,
                                "message": {"role": "assistant", "content": "hello"},
                            }
                        },
                    )
                    emit(
                        "chunk",
                        {
                            "delta": {
                                "index": 0,
                                "message": {"role": "assistant", "content": " world"},
                                "usage": usage,
                                "finish_reason": "stop",
                            }
                        },
                    )
                emit("done")
            elif action == "invoke_tts":
                emit("chunk", {"result": "6869"})
                emit("done")
            else:
                emit("error", {"code": "UNKNOWN_ACTION", "message": action})
                sys.exit(1)
            """
        ),
        encoding="utf-8",
    )
    binary_path.chmod(0o755)


def _build_runtime(tmp_path: Path) -> SlimRuntime:
    plugin_root = tmp_path / "plugin"
    binary_path = tmp_path / "fake_slim.py"
    _write_fake_plugin(plugin_root)
    _write_fake_slim(binary_path)

    with patch(
        "graphon.model_runtime.slim.runtime.shutil.which",
        return_value=str(binary_path),
    ):
        runtime = SlimRuntime(
            SlimConfig(
                bindings=[
                    SlimProviderBinding(
                        plugin_id="author/fake:0.0.1@test",
                        plugin_root=plugin_root,
                    )
                ],
                local=SlimLocalSettings(folder=tmp_path / "plugins"),
            )
        )
    return runtime


def test_slim_runtime_loads_provider_schema_and_icon(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)

    providers = runtime.fetch_model_providers()

    assert len(providers) == 1
    provider = providers[0]
    assert provider.provider == "fake-provider"
    assert provider.label.en_US == "Fake Provider"
    assert provider.models[0].model == "fake-chat"

    icon_bytes, extension = runtime.get_provider_icon(
        provider="fake-provider",
        icon_type="icon_small",
        lang="en_US",
    )
    assert icon_bytes.startswith(b"<svg")
    assert extension == ".svg"


def test_slim_runtime_invokes_llm_and_counts_tokens(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    prepared = SlimPreparedLLM(
        runtime=runtime,
        provider="fake-provider",
        model_name="fake-chat",
        credentials={"api_key": "secret"},
    )

    result = prepared.invoke_llm(
        prompt_messages=[UserPromptMessage(content="Hello")],
        model_parameters={},
        tools=None,
        stop=None,
        stream=False,
    )

    assert result.message.content == "hello world"
    assert result.usage.total_tokens == 4
    assert prepared.get_llm_num_tokens([UserPromptMessage(content="Hello")]) == 42


def test_slim_runtime_fetches_custom_model_schema_via_action(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)

    schema = runtime.get_model_schema(
        provider="fake-provider",
        model_type=ModelType.LLM,
        model="custom-chat",
        credentials={"api_key": "secret"},
    )

    assert schema is not None
    assert schema.model == "custom-chat"
    assert schema.model_type == ModelType.LLM


def test_slim_package_loader_selects_requested_provider(tmp_path: Path) -> None:
    plugin_root = tmp_path / "plugin"
    binary_path = tmp_path / "fake_slim.py"
    _write_multi_provider_plugin(plugin_root)
    _write_fake_slim(binary_path)

    with patch(
        "graphon.model_runtime.slim.runtime.shutil.which",
        return_value=str(binary_path),
    ):
        runtime = SlimRuntime(
            SlimConfig(
                bindings=[
                    SlimProviderBinding(
                        plugin_id="author/fake:0.0.1@test",
                        provider="other-provider",
                        plugin_root=plugin_root,
                    )
                ],
                local=SlimLocalSettings(folder=tmp_path / "plugins"),
            )
        )

    providers = runtime.fetch_model_providers()

    assert len(providers) == 1
    assert providers[0].provider == "other-provider"
    assert providers[0].models[0].model == "other-chat"


def test_slim_runtime_invokes_tts_without_tenant_context(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)

    result = b"".join(
        runtime.invoke_tts(
            provider="fake-provider",
            model="fake-chat",
            credentials={"api_key": "secret"},
            content_text="Hello",
            voice="nova",
        )
    )

    assert result == b"hi"


def test_slim_runtime_merges_non_stream_tool_call_deltas(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)

    result = runtime.invoke_llm(
        provider="fake-provider",
        model="fake-chat",
        credentials={"api_key": "secret"},
        model_parameters={},
        prompt_messages=[UserPromptMessage(content="Hello")],
        tools=[
            PromptMessageTool(
                name="extract",
                description="Extract fields",
                parameters={},
            )
        ],
        stop=None,
        stream=False,
    )

    assert len(result.message.tool_calls) == 1
    assert result.message.tool_calls[0].function.name == "extract"
    assert result.message.tool_calls[0].function.arguments == '{"query": "Hello"}'


def test_slim_runtime_keeps_blocking_structured_output(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    prepared = SlimPreparedLLM(
        runtime=runtime,
        provider="fake-provider",
        model_name="fake-chat",
        credentials={"api_key": "secret"},
    )

    result = prepared.invoke_llm_with_structured_output(
        prompt_messages=[UserPromptMessage(content="Hello")],
        json_schema={"type": "object"},
        model_parameters={},
        stop=None,
        stream=False,
    )

    assert isinstance(result, LLMResultWithStructuredOutput)
    assert result.structured_output == {"answer": "structured"}
    assert prepared.is_structured_output_parse_error(ValueError("boom")) is False


def test_slim_config_auto_discovers_uv_and_python(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "graphon.model_runtime.slim.config.shutil.which",
        lambda name: "/usr/local/bin/uv" if name == "uv" else None,
    )

    config = SlimConfig(
        bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
        local=SlimLocalSettings(folder=tmp_path / "plugins"),
    )

    assert config.local.python_path == sys.executable
    assert config.local.uv_path == "/usr/local/bin/uv"
    assert config.build_env()["SLIM_MODE"] == "local"


def test_slim_runtime_panics_when_binary_is_missing(
    caplog,
    tmp_path: Path,
) -> None:
    with patch("graphon.model_runtime.slim.runtime.shutil.which", return_value=None):
        with pytest.raises(
            RuntimeError,
            match=(
                r"dify-plugin-daemon-slim is not available in PATH\. "
                r"Set SLIM_BINARY_PATH to override it\."
            ),
        ):
            SlimRuntime(
                SlimConfig(
                    bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
                    local=SlimLocalSettings(folder=tmp_path / "plugins"),
                )
            )

    assert (
        "dify-plugin-daemon-slim is not available in PATH. "
        "Set SLIM_BINARY_PATH to override it."
    ) in caplog.text


def test_slim_runtime_uses_slim_binary_path_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    custom_binary = tmp_path / "custom-slim"
    custom_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    custom_binary.chmod(0o755)

    monkeypatch.setenv("SLIM_BINARY_PATH", str(custom_binary))

    with patch("graphon.model_runtime.slim.runtime.shutil.which", return_value=None):
        runtime = SlimRuntime(
            SlimConfig(
                bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
                local=SlimLocalSettings(folder=tmp_path / "plugins"),
            )
        )

    assert runtime._binary_path == str(custom_binary)  # pyright: ignore[reportPrivateUsage]


def test_slim_runtime_rejects_invalid_slim_binary_path_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    missing_binary = tmp_path / "missing-slim"
    monkeypatch.setenv("SLIM_BINARY_PATH", str(missing_binary))

    with pytest.raises(
        RuntimeError,
        match=rf"SLIM_BINARY_PATH points to a missing file: {missing_binary}",
    ):
        SlimRuntime(
            SlimConfig(
                bindings=[SlimProviderBinding(plugin_id="author/fake:0.0.1@test")],
                local=SlimLocalSettings(folder=tmp_path / "plugins"),
            )
        )
