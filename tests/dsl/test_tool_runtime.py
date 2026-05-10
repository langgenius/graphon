from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from graphon.dsl import tool_runtime
from graphon.dsl.slim.client import SlimClientConfig
from graphon.dsl.tool_runtime import (
    SlimToolAction,
    SlimToolNodeRuntime,
    SlimToolParameterForm,
    SlimToolParameterType,
)
from graphon.enums import BuiltinNodeTypes
from graphon.nodes.tool.entities import ToolInputType, ToolNodeData, ToolProviderType
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage
from tests.helpers.builders import build_variable_pool


def _node_data() -> ToolNodeData:
    return ToolNodeData.model_validate({
        "type": BuiltinNodeTypes.TOOL,
        "title": "Tool",
        "version": "2",
        "provider_id": "langgenius/search/search",
        "provider_type": ToolProviderType.PLUGIN,
        "provider_name": "langgenius/search/search",
        "tool_name": "web_search",
        "tool_label": "Web search",
        "tool_configurations": {
            "region": {"type": ToolInputType.MIXED, "value": "q={{#sys.query#}}"},
            "limit": {"type": ToolInputType.CONSTANT, "value": "3"},
        },
        "tool_parameters": {
            "query": {"type": ToolInputType.VARIABLE, "value": ["sys", "query"]},
        },
        "tool_node_version": "2",
    })


def test_slim_tool_runtime_uses_runtime_parameters_and_dify_merge_order() -> None:
    calls: list[tuple[str, str | SlimToolAction, dict[str, Any]]] = []

    def invoker(
        plugin_id: str,
        action: str | SlimToolAction,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        calls.append((plugin_id, action, dict(data)))
        if action == SlimToolAction.GET_TOOL_RUNTIME_PARAMETERS:
            return [
                {
                    "parameters": [
                        {
                            "name": "query",
                            "form": SlimToolParameterForm.LLM,
                            "required": True,
                        },
                        {
                            "name": "region",
                            "form": SlimToolParameterForm.FORM,
                            "type": SlimToolParameterType.STRING,
                        },
                        {
                            "name": "limit",
                            "form": SlimToolParameterForm.FORM,
                            "type": SlimToolParameterType.NUMBER,
                        },
                    ],
                }
            ]
        return [
            {
                "type": ToolRuntimeMessage.MessageType.JSON,
                "message": {"ok": True},
            }
        ]

    runtime = SlimToolNodeRuntime(
        config=SlimClientConfig(folder=Path(".slim")),
        plugin_id="langgenius/search:0.1.0@abc",
        provider="search",
        provider_id="langgenius/search/search",
        tool_name="web_search",
        credentials={"token": "secret"},
        action_invoker=invoker,
    )
    variable_pool = build_variable_pool(variables=[(["sys", "query"], "Graphon")])
    handle = runtime.get_runtime(
        node_id="tool",
        node_data=_node_data(),
        variable_pool=variable_pool,
    )

    parameters = runtime.get_runtime_parameters(tool_runtime=handle)
    assert [(parameter.name, parameter.required) for parameter in parameters] == [
        ("query", True),
        ("region", False),
        ("limit", False),
    ]

    messages = list(
        runtime.invoke(
            tool_runtime=handle,
            tool_parameters={"query": "override", "region": "explicit"},
            workflow_call_depth=0,
            provider_name="search",
        )
    )

    invoke_data = calls[-1][2]
    assert calls[-1][:2] == (
        "langgenius/search:0.1.0@abc",
        SlimToolAction.INVOKE_TOOL,
    )
    assert invoke_data["provider"] == "search"
    assert invoke_data["tool"] == "web_search"
    assert invoke_data["credentials"] == {"token": "secret"}
    assert invoke_data["tool_parameters"] == {
        "region": "q=Graphon",
        "limit": 3,
        "query": "override",
    }
    assert messages[0].type == ToolRuntimeMessage.MessageType.JSON
    assert isinstance(messages[0].message, ToolRuntimeMessage.JsonMessage)
    assert messages[0].message.json_object == {"ok": True}


def test_tool_declaration_reads_static_parameters_from_slim_extract_payload() -> None:
    declaration = tool_runtime.tool_declaration_from_extract_payload(
        {
            "data": {
                "manifest": {
                    "tool": {
                        "identity": {"name": "search"},
                        "tools": [
                            {
                                "identity": {"name": "web_search"},
                                "parameters": [
                                    {
                                        "name": "query",
                                        "form": SlimToolParameterForm.LLM,
                                        "required": True,
                                    }
                                ],
                                "has_runtime_parameters": False,
                            }
                        ],
                    }
                }
            }
        },
        provider="search",
        tool_name="web_search",
    )

    assert declaration.has_runtime_parameters is False
    assert declaration.parameters[0].name == "query"
    assert declaration.parameters[0].required is True


def test_tool_message_conversion_covers_daemon_wrappers_and_blob_chunks() -> None:
    text = tool_runtime.tool_runtime_message_from_payload({
        "code": 0,
        "data": {
            "type": ToolRuntimeMessage.MessageType.IMAGE_LINK,
            "message": {"text": "https://example.test/a.png"},
        },
    })
    chunk = tool_runtime.tool_runtime_message_from_payload({
        "type": ToolRuntimeMessage.MessageType.BLOB_CHUNK,
        "message": {
            "id": "blob",
            "sequence": 1,
            "total_length": 3,
            "blob": "YWI=",
            "end": False,
        },
    })

    assert text.type == ToolRuntimeMessage.MessageType.IMAGE_LINK
    assert isinstance(text.message, ToolRuntimeMessage.TextMessage)
    assert text.message.text == "https://example.test/a.png"
    assert chunk.type == ToolRuntimeMessage.MessageType.BLOB_CHUNK
    assert isinstance(chunk.message, ToolRuntimeMessage.BlobChunkMessage)
    assert chunk.message.blob == b"ab"


def test_tool_message_conversion_rejects_daemon_error() -> None:
    with pytest.raises(Exception, match="bad credentials"):
        tool_runtime.tool_runtime_message_from_payload({
            "code": 1,
            "message": "bad credentials",
        })
