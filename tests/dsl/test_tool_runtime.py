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
from graphon.file import helpers as file_helpers
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
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
    assert invoke_data["credential_type"] == "api-key"
    assert invoke_data["tool_parameters"] == {
        "region": "explicit",
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


def test_slim_tool_runtime_casts_parameters_and_serializes_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | SlimToolAction, dict[str, Any]]] = []

    def fake_resolve_file_url(_file: File, *, for_external: bool = True) -> str:
        _ = for_external
        return "https://files.example.test/report.pdf"

    monkeypatch.setattr(file_helpers, "resolve_file_url", fake_resolve_file_url)

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
                            "name": "document",
                            "form": SlimToolParameterForm.LLM,
                            "type": "file",
                        },
                        {
                            "name": "tags",
                            "form": SlimToolParameterForm.LLM,
                            "type": SlimToolParameterType.ARRAY,
                        },
                        {
                            "name": "checks",
                            "form": SlimToolParameterForm.FORM,
                            "type": SlimToolParameterType.CHECKBOX,
                        },
                        {
                            "name": "config",
                            "form": SlimToolParameterForm.FORM,
                            "type": SlimToolParameterType.OBJECT,
                        },
                        {
                            "name": "choice",
                            "form": SlimToolParameterForm.FORM,
                            "type": "select",
                        },
                        {
                            "name": "flag",
                            "form": SlimToolParameterForm.FORM,
                            "type": SlimToolParameterType.BOOLEAN,
                        },
                    ],
                }
            ]
        return [{"type": ToolRuntimeMessage.MessageType.TEXT, "message": "ok"}]

    node_data = ToolNodeData.model_validate({
        **_node_data().model_dump(mode="python"),
        "tool_configurations": {
            "config": {"type": ToolInputType.CONSTANT, "value": '{"top_k": 3}'},
            "choice": {"type": ToolInputType.CONSTANT, "value": 7},
            "flag": {"type": ToolInputType.CONSTANT, "value": "false"},
            "checks": {"type": ToolInputType.CONSTANT, "value": ["one", "two"]},
        },
        "tool_parameters": {
            "document": {
                "type": ToolInputType.VARIABLE,
                "value": ["start", "document"],
            },
            "tags": {"type": ToolInputType.CONSTANT, "value": '["alpha", "beta"]'},
        },
    })
    runtime = SlimToolNodeRuntime(
        config=SlimClientConfig(folder=Path(".slim")),
        plugin_id="langgenius/search:0.1.0@abc",
        provider="search",
        provider_id="langgenius/search/search",
        tool_name="web_search",
        credentials={},
        action_invoker=invoker,
    )
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-file-id",
        filename="report.pdf",
        extension=".pdf",
        mime_type="application/pdf",
        size=128,
    )
    variable_pool = build_variable_pool(variables=[(["start", "document"], file)])
    handle = runtime.get_runtime(
        node_id="tool",
        node_data=node_data,
        variable_pool=variable_pool,
    )

    list(
        runtime.invoke(
            tool_runtime=handle,
            tool_parameters={"document": [file], "tags": '["alpha", "beta"]'},
            workflow_call_depth=0,
            provider_name="search",
        )
    )

    invoke_data = calls[-1][2]
    assert invoke_data["tool_parameters"] == {
        "config": {"top_k": 3},
        "choice": "7",
        "flag": False,
        "checks": "['one', 'two']",
        "document": {
            "dify_model_identity": "__dify__file__",
            "mime_type": "application/pdf",
            "filename": "report.pdf",
            "extension": ".pdf",
            "size": 128,
            "type": FileType.DOCUMENT,
            "url": "https://files.example.test/report.pdf",
        },
        "tags": ["alpha", "beta"],
    }


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
    file_message = tool_runtime.tool_runtime_message_from_payload({
        "type": ToolRuntimeMessage.MessageType.FILE,
        "message": {},
        "meta": {
            "file": {
                "tool_file_id": "tool-file-id",
                "type": FileType.DOCUMENT,
                "transfer_method": FileTransferMethod.TOOL_FILE,
                "filename": "report.pdf",
                "mime_type": "application/pdf",
            },
        },
    })

    assert text.type == ToolRuntimeMessage.MessageType.IMAGE_LINK
    assert isinstance(text.message, ToolRuntimeMessage.TextMessage)
    assert text.message.text == "https://example.test/a.png"
    assert chunk.type == ToolRuntimeMessage.MessageType.BLOB_CHUNK
    assert isinstance(chunk.message, ToolRuntimeMessage.BlobChunkMessage)
    assert chunk.message.blob == b"ab"
    assert isinstance(file_message.meta, dict)
    assert isinstance(file_message.meta["file"], File)
    assert file_message.meta["file"].reference == "tool-file-id"


def test_tool_message_conversion_rejects_daemon_error() -> None:
    with pytest.raises(Exception, match="bad credentials"):
        tool_runtime.tool_runtime_message_from_payload({
            "code": 1,
            "message": "bad credentials",
        })


def test_slim_tool_runtime_builds_tool_and_remote_file_references() -> None:
    runtime = SlimToolNodeRuntime(
        config=SlimClientConfig(folder=Path(".slim")),
        plugin_id="langgenius/search:0.1.0@abc",
        provider="search",
        provider_id="langgenius/search/search",
        tool_name="web_search",
        credentials={},
        action_invoker=lambda *_: [],
    )

    tool_file = runtime.build_file_reference(
        mapping={
            "tool_file_id": "tool-file-id",
            "type": FileType.DOCUMENT,
            "transfer_method": FileTransferMethod.TOOL_FILE,
            "filename": "report.pdf",
            "mime_type": "application/pdf",
        },
    )
    remote_file = runtime.build_file_reference(
        mapping={
            "url": "https://example.test/report.pdf",
            "type": FileType.DOCUMENT,
            "transfer_method": FileTransferMethod.REMOTE_URL,
        },
    )

    assert tool_file.reference == "tool-file-id"
    assert tool_file.filename == "report.pdf"
    assert tool_file.mime_type == "application/pdf"
    assert remote_file.remote_url == "https://example.test/report.pdf"
