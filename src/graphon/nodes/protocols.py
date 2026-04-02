from collections.abc import Generator, Mapping
from typing import Any, Protocol

from graphon.file.models import File
from graphon.http.protocols import HttpClientProtocol


class FileManagerProtocol(Protocol):
    def download(self, f: File, /) -> bytes: ...


class ToolFileManagerProtocol(Protocol):
    def create_file_by_raw(
        self,
        *,
        file_binary: bytes,
        mimetype: str,
        filename: str | None = None,
    ) -> Any: ...

    def get_file_generator_by_tool_file_id(
        self,
        tool_file_id: str,
    ) -> tuple[Generator | None, File | None]: ...


class FileReferenceFactoryProtocol(Protocol):
    def build_from_mapping(self, *, mapping: Mapping[str, Any]) -> File: ...


__all__ = [
    "FileManagerProtocol",
    "FileReferenceFactoryProtocol",
    "HttpClientProtocol",
    "ToolFileManagerProtocol",
]
