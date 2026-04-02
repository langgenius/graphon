from unittest.mock import MagicMock

import pytest

from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.node import StreamChunkEvent, StreamCompletedEvent
from graphon.nodes.tool.exc import ToolNodeError
from graphon.nodes.tool.tool_node import ToolNode
from graphon.nodes.tool_runtime_entities import ToolRuntimeHandle, ToolRuntimeMessage


def _build_tool_node() -> ToolNode:
    node = ToolNode.__new__(ToolNode)
    node._node_id = "node-1"
    node._runtime = MagicMock()
    node._runtime.get_usage.return_value = LLMUsage.empty_usage()
    node._tool_file_manager_factory = MagicMock()
    return node


def test_transform_message_dispatches_text_variable_and_file_messages() -> None:
    node = _build_tool_node()
    file_obj = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="file-ref",
        filename="doc.txt",
    )
    messages = iter([
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
    ])

    events = list(
        node._transform_message(
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
    node = _build_tool_node()
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
    node._tool_file_manager_factory.get_file_generator_by_tool_file_id.return_value = (
        None,
        tool_file,
    )
    node._runtime.build_file_reference.return_value = built_file

    events = list(
        node._transform_message(
            messages=iter([
                ToolRuntimeMessage(
                    type=ToolRuntimeMessage.MessageType.IMAGE_LINK,
                    message=ToolRuntimeMessage.TextMessage(
                        text="https://example.com/image.png",
                    ),
                    meta={"tool_file_id": "tool-file-1"},
                ),
            ]),
            tool_info={},
            parameters_for_log={},
            node_id="node-1",
            tool_runtime=ToolRuntimeHandle(raw=object()),
        ),
    )

    completed_event = events[-1]
    assert isinstance(completed_event, StreamCompletedEvent)
    assert completed_event.node_run_result.outputs["files"].value == [built_file]
    node._runtime.build_file_reference.assert_called_once_with(
        mapping={
            "tool_file_id": "tool-file-1",
            "type": FileType.IMAGE,
            "transfer_method": FileTransferMethod.TOOL_FILE,
            "url": "https://example.com/image.png",
        },
    )


def test_transform_message_rejects_non_file_payload_in_file_message() -> None:
    node = _build_tool_node()

    with pytest.raises(ToolNodeError, match="Expected File object"):
        list(
            node._transform_message(
                messages=iter([
                    ToolRuntimeMessage(
                        type=ToolRuntimeMessage.MessageType.FILE,
                        message=ToolRuntimeMessage.FileMessage(),
                        meta={"file": "not-a-file"},
                    ),
                ]),
                tool_info={},
                parameters_for_log={},
                node_id="node-1",
                tool_runtime=ToolRuntimeHandle(raw=object()),
            ),
        )
