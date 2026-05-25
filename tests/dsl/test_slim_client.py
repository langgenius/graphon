from __future__ import annotations

from collections.abc import Generator, Mapping
from pathlib import Path
from typing import Any

import pytest

import graphon.dsl.slim.client as slim_client_module
from graphon.dsl.slim.client import (
    SlimChunkEvent,
    SlimClient,
    SlimClientConfig,
    SlimClientError,
    SlimDoneEvent,
    SlimEvent,
    parse_slim_event,
)
from graphon.model_runtime.entities.model_entities import ModelType, ParameterType


class _FakeSlimClient(SlimClient):
    def __init__(
        self,
        *,
        config: SlimClientConfig,
        events: list[SlimEvent],
    ) -> None:
        super().__init__(config=config, binary_path="slim")
        self.events = events

    def invoke_events(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[SlimEvent, None, None]:
        _ = plugin_id, action, data
        yield from self.events


def test_remote_slim_client_unwraps_daemon_success_payload(tmp_path: Path) -> None:
    client = _FakeSlimClient(
        config=SlimClientConfig(
            folder=tmp_path,
            mode="remote",
            daemon_addr="http://daemon.test",
            daemon_key="secret",
        ),
        events=[
            SlimChunkEvent(
                data={
                    "code": 0,
                    "message": "success",
                    "data": {"num_tokens": 42},
                },
            ),
            SlimDoneEvent(),
        ],
    )

    chunks = list(client.invoke_chunks(plugin_id="plugin", action="action", data={}))

    assert chunks == [{"num_tokens": 42}]


def test_remote_slim_client_raises_daemon_error(tmp_path: Path) -> None:
    client = _FakeSlimClient(
        config=SlimClientConfig(
            folder=tmp_path,
            mode="remote",
            daemon_addr="http://daemon.test",
            daemon_key="secret",
        ),
        events=[
            SlimChunkEvent(
                data={
                    "code": 500,
                    "message": "bad credentials",
                    "data": None,
                },
            ),
            SlimDoneEvent(),
        ],
    )

    with pytest.raises(SlimClientError, match="bad credentials"):
        list(client.invoke_chunks(plugin_id="plugin", action="action", data={}))


def test_remote_slim_client_preserves_daemon_error_metadata(tmp_path: Path) -> None:
    client = _FakeSlimClient(
        config=SlimClientConfig(
            folder=tmp_path,
            mode="remote",
            daemon_addr="http://daemon.test",
            daemon_key="secret",
        ),
        events=[
            SlimChunkEvent(
                data={
                    "code": "killed_by_timeout",
                    "message": "killed by timeout",
                    "data": {"stage": "token_count"},
                },
            ),
            SlimDoneEvent(),
        ],
    )

    with pytest.raises(SlimClientError) as exc_info:
        list(client.invoke_chunks(plugin_id="plugin", action="action", data={}))

    assert exc_info.value.code == "killed_by_timeout"
    assert exc_info.value.stage == "token_count"
    assert exc_info.value.data["data"] == {"stage": "token_count"}


def test_remote_slim_client_preserves_top_level_daemon_stage(
    tmp_path: Path,
) -> None:
    client = _FakeSlimClient(
        config=SlimClientConfig(
            folder=tmp_path,
            mode="remote",
            daemon_addr="http://daemon.test",
            daemon_key="secret",
        ),
        events=[
            SlimChunkEvent(
                data={
                    "code": "killed_by_timeout",
                    "stage": "token_count",
                    "message": "killed by timeout",
                    "data": "not-a-mapping",
                },
            ),
            SlimDoneEvent(),
        ],
    )

    with pytest.raises(SlimClientError) as exc_info:
        list(client.invoke_chunks(plugin_id="plugin", action="action", data={}))

    assert exc_info.value.code == "killed_by_timeout"
    assert exc_info.value.stage == "token_count"


def test_slim_error_event_preserves_code_and_stage() -> None:
    with pytest.raises(SlimClientError) as exc_info:
        parse_slim_event(
            '{"event":"error","data":{'
            '"code":"killed_by_timeout",'
            '"stage":"get_llm_num_tokens",'
            '"message":"killed by timeout"'
            "}}",
        )

    assert exc_info.value.code == "killed_by_timeout"
    assert exc_info.value.stage == "get_llm_num_tokens"


def test_slim_error_event_accepts_non_mapping_data() -> None:
    with pytest.raises(SlimClientError) as exc_info:
        parse_slim_event('{"event":"error","data":"plain failure"}')

    assert str(exc_info.value) == "plain failure"
    assert exc_info.value.code is None
    assert exc_info.value.stage is None


def test_slim_extract_preserves_process_error_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def run_stub(*args: Any, **kwargs: Any) -> object:
        _ = args, kwargs

        class ProcessResult:
            returncode = 1
            stdout = ""
            stderr = (
                '{"error_code":"killed_by_timeout",'
                '"stage":"get_llm_num_tokens",'
                '"message":"killed by timeout"}'
            )

        return ProcessResult()

    monkeypatch.setattr(slim_client_module.subprocess, "run", run_stub)
    client = SlimClient(config=SlimClientConfig(folder=tmp_path), binary_path="slim")

    with pytest.raises(SlimClientError) as exc_info:
        client.extract(plugin_id="plugin")

    assert str(exc_info.value) == "killed by timeout"
    assert exc_info.value.code == "killed_by_timeout"
    assert exc_info.value.stage == "get_llm_num_tokens"
    assert exc_info.value.return_code == 1


@pytest.mark.parametrize(
    ("stderr", "expected_message"),
    [
        ("", "Slim process exited with code 1"),
        ("not json", "Slim process exited with code 1: not json"),
        ('["not", "a", "mapping"]', "Slim process exited with code 1: "),
    ],
)
def test_slim_extract_preserves_unstructured_process_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stderr: str,
    expected_message: str,
) -> None:
    def run_stub(*args: Any, **kwargs: Any) -> object:
        _ = args, kwargs

        class ProcessResult:
            returncode = 1
            stdout = ""
            stderr = stderr_value

        return ProcessResult()

    stderr_value = stderr
    monkeypatch.setattr(slim_client_module.subprocess, "run", run_stub)
    client = SlimClient(config=SlimClientConfig(folder=tmp_path), binary_path="slim")

    with pytest.raises(SlimClientError) as exc_info:
        client.extract(plugin_id="plugin")

    assert str(exc_info.value).startswith(expected_message)
    assert exc_info.value.code is None
    assert exc_info.value.stage is None
    assert exc_info.value.return_code == 1


def test_local_slim_client_keeps_raw_payloads(tmp_path: Path) -> None:
    payload = {"code": 0, "message": "raw", "data": {"kept": True}}
    client = _FakeSlimClient(
        config=SlimClientConfig(folder=tmp_path, mode="local"),
        events=[
            SlimChunkEvent(data=payload),
            SlimDoneEvent(),
        ],
    )

    chunks = list(client.invoke_chunks(plugin_id="plugin", action="action", data={}))

    assert chunks == [payload]


def test_slim_client_converts_dify_model_schema_parameter_templates(
    tmp_path: Path,
) -> None:
    client = _FakeSlimClient(
        config=SlimClientConfig(folder=tmp_path),
        events=[
            SlimChunkEvent(
                data={
                    "model_schema": {
                        "model": "fake-chat",
                        "label": {"en_US": "Fake Chat"},
                        "model_type": "llm",
                        "fetch_from": "predefined-model",
                        "model_properties": {
                            "mode": "chat",
                            "context_size": 8192,
                        },
                        "parameter_rules": [
                            {
                                "name": "temperature",
                                "use_template": "temperature",
                            }
                        ],
                    }
                },
            ),
            SlimDoneEvent(),
        ],
    )

    schema = client.get_ai_model_schema(
        plugin_id="author/fake:0.0.1@test",
        provider="fake-provider",
        model_type="llm",
        model="fake-chat",
        credentials={"api_key": "secret"},
    )

    assert schema is not None
    assert schema.model == "fake-chat"
    assert schema.model_type == ModelType.LLM
    assert schema.parameter_rules[0].name == "temperature"
    assert schema.parameter_rules[0].use_template == "temperature"
    assert schema.parameter_rules[0].label.en_us == "temperature"
    assert schema.parameter_rules[0].type == ParameterType.STRING
