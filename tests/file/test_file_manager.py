import base64
from unittest.mock import MagicMock

import pytest

import graphon.file.runtime as runtime_module
from graphon.file.enums import (
    FileTransferMethod,
    FileType,
)
from graphon.file.file_manager import download, to_prompt_message_content
from graphon.file.models import File
from graphon.file.runtime import (
    WorkflowFileRuntimeRegistry,
    set_workflow_file_runtime,
)
from graphon.model_runtime.entities.message_entities import (
    DocumentPromptMessageContent,
    ImagePromptMessageContent,
    TextPromptMessageContent,
)

from ..helpers import build_file_reference


def _build_file(
    *,
    transfer_method: FileTransferMethod,
    file_type: FileType = FileType.IMAGE,
    reference: str | None = None,
    remote_url: str | None = None,
    filename: str = "image.png",
    extension: str = ".png",
    mime_type: str = "image/png",
) -> File:
    return File(
        file_id="file-id",
        file_type=file_type,
        transfer_method=transfer_method,
        reference=reference,
        remote_url=remote_url,
        filename=filename,
        extension=extension,
        mime_type=mime_type,
        size=128,
    )


@pytest.fixture
def workflow_file_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> MagicMock:
    runtime = MagicMock()
    runtime_registry = WorkflowFileRuntimeRegistry()
    runtime_registry.set(runtime)
    monkeypatch.setattr(
        runtime_module,
        "_workflow_file_runtime_registry",
        runtime_registry,
    )
    set_workflow_file_runtime(runtime)
    return runtime


@pytest.mark.parametrize(
    "transfer_method",
    [
        FileTransferMethod.LOCAL_FILE,
        FileTransferMethod.TOOL_FILE,
        FileTransferMethod.DATASOURCE_FILE,
    ],
)
def test_download_delegates_storage_backed_files_to_runtime_loader(
    workflow_file_runtime: MagicMock,
    transfer_method: FileTransferMethod,
) -> None:
    workflow_file_runtime.load_file_bytes.return_value = b"payload"
    file = _build_file(
        transfer_method=transfer_method,
        reference=build_file_reference(
            record_id="file-id",
            storage_key="files/payload.bin",
        ),
    )

    assert download(file) == b"payload"
    workflow_file_runtime.load_file_bytes.assert_called_once_with(file=file)


def test_download_remote_url_uses_runtime_http_get(
    workflow_file_runtime: MagicMock,
) -> None:
    response = MagicMock()
    response.content = b"remote-payload"
    workflow_file_runtime.http_get.return_value = response
    file = _build_file(
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.com/image.png",
    )

    assert download(file) == b"remote-payload"
    workflow_file_runtime.http_get.assert_called_once_with(
        "https://example.com/image.png",
        follow_redirects=True,
    )
    response.raise_for_status.assert_called_once_with()


def test_to_prompt_message_content_uses_runtime_url_resolution_for_images(
    workflow_file_runtime: MagicMock,
) -> None:
    workflow_file_runtime.multimodal_send_format = "url"
    workflow_file_runtime.resolve_file_url.return_value = (
        "https://cdn.example.com/image.png"
    )
    file = _build_file(
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference=build_file_reference(
            record_id="upload-file-id",
            storage_key="files/image.png",
        ),
    )

    content = to_prompt_message_content(
        file,
        image_detail_config=ImagePromptMessageContent.DETAIL.HIGH,
    )

    assert isinstance(content, ImagePromptMessageContent)
    assert content.url == "https://cdn.example.com/image.png"
    assert not content.base64_data
    assert content.detail == ImagePromptMessageContent.DETAIL.HIGH


def test_to_prompt_message_content_uses_runtime_file_loader_for_base64_documents(
    workflow_file_runtime: MagicMock,
) -> None:
    workflow_file_runtime.multimodal_send_format = "base64"
    workflow_file_runtime.load_file_bytes.return_value = b"document-bytes"
    file = _build_file(
        transfer_method=FileTransferMethod.TOOL_FILE,
        file_type=FileType.DOCUMENT,
        reference=build_file_reference(
            record_id="tool-file-id",
            storage_key="docs/report.pdf",
        ),
        filename="report.pdf",
        extension=".pdf",
        mime_type="application/pdf",
    )

    content = to_prompt_message_content(file)

    assert isinstance(content, DocumentPromptMessageContent)
    assert content.base64_data == base64.b64encode(b"document-bytes").decode("utf-8")
    assert not content.url
    workflow_file_runtime.load_file_bytes.assert_called_once_with(file=file)


def test_to_prompt_message_content_returns_text_placeholder_for_custom_files() -> None:
    file = _build_file(
        transfer_method=FileTransferMethod.REMOTE_URL,
        file_type=FileType.CUSTOM,
        remote_url="https://example.com/archive.bin",
        filename="archive.bin",
        extension=".bin",
        mime_type="application/octet-stream",
    )

    content = to_prompt_message_content(file)

    assert isinstance(content, TextPromptMessageContent)
    assert content.data == "[Unsupported file type: archive.bin (custom)]"
