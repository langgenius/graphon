from __future__ import annotations

from collections.abc import Generator, Mapping
from typing import Any, cast

import pytest

from graphon.file.models import File
from graphon.http import HttpResponse, HttpxHttpClient, get_http_client
from graphon.nodes.llm.file_saver import FileSaverImpl


class _ToolFileManager:
    def create_file_by_raw(
        self,
        *,
        file_binary: bytes,
        mimetype: str,
        filename: str | None = None,
    ) -> object:
        _ = file_binary, mimetype, filename
        raise NotImplementedError

    def get_file_generator_by_tool_file_id(
        self,
        tool_file_id: str,
    ) -> tuple[Generator[bytes, None, None] | None, File | None]:
        _ = tool_file_id
        raise NotImplementedError


class _FileReferenceFactory:
    def build_from_mapping(
        self,
        *,
        mapping: Mapping[str, Any],
    ) -> File:
        return File.model_validate(mapping)


class _FalseyHttpClient:
    @property
    def max_retries_exceeded_error(self) -> type[Exception]:
        return RuntimeError

    @property
    def request_error(self) -> type[Exception]:
        return RuntimeError

    def __bool__(self) -> bool:
        return False

    def get(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError

    def head(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError

    def post(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError

    def put(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError

    def delete(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError

    def patch(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        _ = url, max_retries, kwargs
        raise NotImplementedError


def test_file_saver_impl_with_runtime_accepts_explicit_http_client() -> None:
    http_client = HttpxHttpClient()

    file_saver = FileSaverImpl.with_runtime(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
        http_client=http_client,
    )

    assert file_saver.http_client is http_client


def test_file_saver_impl_with_runtime_uses_default_http_client() -> None:
    file_saver = FileSaverImpl.with_runtime(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )

    assert file_saver.http_client is get_http_client()


def test_file_saver_impl_with_runtime_preserves_falsey_http_client() -> None:
    http_client = _FalseyHttpClient()

    file_saver = FileSaverImpl.with_runtime(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
        http_client=http_client,
    )

    assert file_saver.http_client is http_client


def test_file_saver_impl_constructor_still_accepts_legacy_arguments() -> None:
    http_client = HttpxHttpClient()

    file_saver = FileSaverImpl(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
        http_client=http_client,
    )

    assert file_saver.http_client is http_client


def test_file_saver_impl_constructor_requires_runtime_collaborators() -> None:
    constructor = cast(Any, FileSaverImpl)

    with pytest.raises(
        TypeError,
        match="missing 1 required keyword-only argument",
    ):
        constructor(
            file_reference_factory=_FileReferenceFactory(),
        )
