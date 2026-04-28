from collections.abc import Generator, Mapping
from time import time
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graphon.enums import BuiltinNodeTypes
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.graph_events.node import NodeRunFailedEvent, NodeRunSucceededEvent
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.tool.entities import ToolNodeData, ToolProviderType
from graphon.nodes.tool.exc import ToolNodeError
from graphon.nodes.tool.tool_node import ToolNode
from graphon.nodes.tool_runtime_entities import (
    ToolRuntimeHandle,
    ToolRuntimeMessage,
    ToolRuntimeParameter,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from tests.helpers.builders import build_graph_init_params, build_variable_pool


def _message_stream(
    *messages: ToolRuntimeMessage,
) -> Generator[ToolRuntimeMessage, None, None]:
    yield from messages


class _StubToolRuntime:
    def __init__(self) -> None:
        self.get_usage = MagicMock(return_value=LLMUsage.empty_usage())
        self.build_file_reference = MagicMock()

    def get_runtime(self, **_: object) -> ToolRuntimeHandle:
        msg = "not used in this test"
        raise AssertionError(msg)

    def get_runtime_parameters(self, **_: object) -> list[object]:
        return []

    def invoke(self, **_: object) -> Generator[ToolRuntimeMessage, None, None]:
        msg = "not used in this test"
        raise AssertionError(msg)


class _RunStubToolRuntime:
    def __init__(self) -> None:
        self.runtime_handle = ToolRuntimeHandle(raw=object())
        self.messages = _message_stream()
        self.usage = LLMUsage.empty_usage()
        self.last_get_runtime_kwargs: dict[str, object] | None = None

    def get_runtime(self, **kwargs: object) -> ToolRuntimeHandle:
        self.last_get_runtime_kwargs = kwargs
        return self.runtime_handle

    def get_runtime_parameters(self, **_: object) -> list[ToolRuntimeParameter]:
        return []

    def invoke(self, **_: object) -> Generator[ToolRuntimeMessage, None, None]:
        return self.messages

    def build_file_reference(self, *, mapping: Mapping[str, Any]) -> object:
        _ = mapping
        return object()

    def get_usage(self, *, tool_runtime: ToolRuntimeHandle) -> LLMUsage:
        _ = tool_runtime
        return self.usage


class _RuntimeWithoutExecutionId:
    def __init__(self) -> None:
        self.runtime_handle = ToolRuntimeHandle(raw=object())
        self.messages = _message_stream()
        self.usage = LLMUsage.empty_usage()
        self.last_get_runtime_kwargs: dict[str, object] | None = None

    def get_runtime(
        self,
        *,
        node_id: str,
        node_data: object,
        variable_pool: object,
    ) -> ToolRuntimeHandle:
        self.last_get_runtime_kwargs = {
            "node_id": node_id,
            "node_data": node_data,
            "variable_pool": variable_pool,
        }
        return self.runtime_handle

    def get_runtime_parameters(self, **_: object) -> list[ToolRuntimeParameter]:
        return []

    def invoke(self, **_: object) -> Generator[ToolRuntimeMessage, None, None]:
        return self.messages

    def build_file_reference(self, *, mapping: Mapping[str, Any]) -> object:
        _ = mapping
        return object()

    def get_usage(self, *, tool_runtime: ToolRuntimeHandle) -> LLMUsage:
        _ = tool_runtime
        return self.usage


class _StubToolFileManagerFactory:
    def __init__(self) -> None:
        self.get_file_generator_by_tool_file_id = MagicMock(return_value=(None, None))

    def create_file_by_raw(self, **_: object) -> object:
        msg = "not used in this test"
        raise AssertionError(msg)


def _build_tool_node() -> tuple[
    ToolNode, _StubToolRuntime, _StubToolFileManagerFactory
]:
    node = ToolNode.__new__(ToolNode)
    node.init_node_identity("node-1")
    runtime = _StubToolRuntime()
    tool_file_manager_factory = _StubToolFileManagerFactory()
    node.init_tool_runtime(
        runtime=runtime,
        tool_file_manager_factory=tool_file_manager_factory,
    )
    return node, runtime, tool_file_manager_factory


def _build_run_tool_node(
    *,
    runtime: object,
    tool_node_version: str | None,
) -> tuple[ToolNode, object]:
    variable_pool = build_variable_pool(variables=[(["upstream", "answer"], "42")])
    runtime_state = GraphRuntimeState(
        variable_pool=variable_pool,
        start_at=time(),
        graph_execution=cast(
            Any,
            MagicMock(
                node_executions={
                    "node-1": MagicMock(execution_id="execution-from-runtime-state"),
                },
            ),
        ),
    )
    node = ToolNode(
        node_id="node-1",
        data=ToolNodeData(
            type=BuiltinNodeTypes.TOOL,
            title="Tool node",
            version="1",
            provider_id="provider-id",
            provider_type=ToolProviderType.BUILT_IN,
            provider_name="provider-name",
            tool_name="tool-name",
            tool_label="Tool label",
            tool_configurations={},
            tool_parameters={},
            tool_node_version=tool_node_version,
        ),
        graph_init_params=build_graph_init_params(),
        graph_runtime_state=runtime_state,
        tool_file_manager_factory=cast(Any, _StubToolFileManagerFactory()),
        runtime=cast(Any, runtime),
    )
    return node, variable_pool


def test_transform_message_dispatches_text_variable_and_file_messages() -> None:
    node, _runtime, _tool_file_manager_factory = _build_tool_node()
    file_obj = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="file-ref",
        filename="doc.txt",
    )
    messages = _message_stream(
        ToolRuntimeMessage(
            type=ToolRuntimeMessage.MessageType.TEXT,
            message=ToolRuntimeMessage.TextMessage(text="hello"),
        ),
        ToolRuntimeMessage(
            type=ToolRuntimeMessage.MessageType.VARIABLE,
            message=ToolRuntimeMessage.VariableMessage(
                variable_name="answer",
                variable_value="A",
                stream=True,
            ),
        ),
        ToolRuntimeMessage(
            type=ToolRuntimeMessage.MessageType.FILE,
            message=ToolRuntimeMessage.FileMessage(),
            meta={"file": file_obj},
        ),
    )

    events = list(
        node.transform_message(
            messages=messages,
            tool_info={},
            parameters_for_log={},
            node_id="node-1",
            tool_runtime=ToolRuntimeHandle(raw=object()),
        ),
    )

    assert isinstance(events[0], StreamChunkEvent)
    assert events[0].selector == ["node-1", "text"]
    assert events[0].chunk == "hello"
    assert isinstance(events[1], StreamChunkEvent)
    assert events[1].selector == ["node-1", "answer"]
    assert events[1].chunk == "A"
    assert isinstance(events[2], StreamChunkEvent)
    assert events[2].selector == ["node-1", "text"]
    assert events[2].is_final is True
    assert isinstance(events[3], StreamChunkEvent)
    assert events[3].selector == ["node-1", "answer"]
    assert events[3].is_final is True

    completed_event = events[4]
    assert isinstance(completed_event, StreamCompletedEvent)
    assert completed_event.node_run_result.outputs["text"] == "hello"
    assert completed_event.node_run_result.outputs["answer"] == "A"
    assert completed_event.node_run_result.outputs["files"].value == [file_obj]
    assert completed_event.node_run_result.outputs["json"] == [{"data": []}]


def test_transform_message_dispatches_image_link_with_handler_map() -> None:
    node, runtime, tool_file_manager_factory = _build_tool_node()
    tool_file = File(
        file_type=FileType.IMAGE,
        transfer_method=FileTransferMethod.TOOL_FILE,
        reference="tool-file-1",
        mime_type="image/png",
    )
    built_file = File(
        file_type=FileType.IMAGE,
        transfer_method=FileTransferMethod.TOOL_FILE,
        reference="tool-file-1",
    )
    tool_file_manager_factory.get_file_generator_by_tool_file_id.return_value = (
        None,
        tool_file,
    )
    runtime.build_file_reference.return_value = built_file

    events = list(
        node.transform_message(
            messages=_message_stream(
                ToolRuntimeMessage(
                    type=ToolRuntimeMessage.MessageType.IMAGE_LINK,
                    message=ToolRuntimeMessage.TextMessage(
                        text="https://example.com/image.png",
                    ),
                    meta={"tool_file_id": "tool-file-1"},
                ),
            ),
            tool_info={},
            parameters_for_log={},
            node_id="node-1",
            tool_runtime=ToolRuntimeHandle(raw=object()),
        ),
    )

    completed_event = events[-1]
    assert isinstance(completed_event, StreamCompletedEvent)
    assert completed_event.node_run_result.outputs["files"].value == [built_file]
    runtime.build_file_reference.assert_called_once_with(
        mapping={
            "tool_file_id": "tool-file-1",
            "type": FileType.IMAGE,
            "transfer_method": FileTransferMethod.TOOL_FILE,
            "url": "https://example.com/image.png",
        },
    )


def test_transform_message_rejects_non_file_payload_in_file_message() -> None:
    node, _runtime, _tool_file_manager_factory = _build_tool_node()

    with pytest.raises(ToolNodeError, match="Expected File object"):
        list(
            node.transform_message(
                messages=_message_stream(
                    ToolRuntimeMessage(
                        type=ToolRuntimeMessage.MessageType.FILE,
                        message=ToolRuntimeMessage.FileMessage(),
                        meta={"file": "not-a-file"},
                    ),
                ),
                tool_info={},
                parameters_for_log={},
                node_id="node-1",
                tool_runtime=ToolRuntimeHandle(raw=object()),
            ),
        )


@pytest.mark.parametrize(
    ("tool_node_version", "expects_variable_pool"),
    [
        (None, False),
        ("2", True),
    ],
)
def test_run_passes_variable_pool_and_restored_execution_id_to_runtime(
    *,
    tool_node_version: str | None,
    expects_variable_pool: bool,
) -> None:
    runtime = _RunStubToolRuntime()
    node, variable_pool = _build_run_tool_node(
        runtime=runtime,
        tool_node_version=tool_node_version,
    )

    events = list(node.run())

    assert runtime.last_get_runtime_kwargs == {
        "node_id": "node-1",
        "node_data": node.node_data,
        "variable_pool": variable_pool if expects_variable_pool else None,
        "node_execution_id": "execution-from-runtime-state",
    }
    assert node.execution_id == "execution-from-runtime-state"
    assert events[0].id == "execution-from-runtime-state"
    assert isinstance(events[-1], NodeRunSucceededEvent)


def test_run_requires_runtime_adapter_to_accept_execution_id() -> None:
    runtime = _RuntimeWithoutExecutionId()
    node, _variable_pool = _build_run_tool_node(
        runtime=runtime,
        tool_node_version=None,
    )

    events = list(node.run())

    assert runtime.last_get_runtime_kwargs is None
    assert isinstance(events[-1], NodeRunFailedEvent)
    assert "node_execution_id" in events[-1].error
