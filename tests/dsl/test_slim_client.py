from __future__ import annotations

from collections.abc import Generator, Mapping
from pathlib import Path
from typing import Any

import pytest

from graphon.dsl.slim.client import (
    SlimChunkEvent,
    SlimClient,
    SlimClientConfig,
    SlimClientError,
    SlimDoneEvent,
    SlimEvent,
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
