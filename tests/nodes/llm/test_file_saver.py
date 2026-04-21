from __future__ import annotations

from collections.abc import Generator, Mapping
from typing import Any, cast

import pytest

from graphon.file.models import File
from graphon.http import HttpResponse, HttpxHttpClient, get_http_client
from graphon.nodes.llm.file_saver import FileSaverDependencies, FileSaverImpl


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


def test_file_saver_impl_accepts_dependency_bundle() -> None:
    http_client = HttpxHttpClient()

    file_saver = FileSaverImpl(
        dependencies=FileSaverDependencies(
            tool_file_manager=_ToolFileManager(),
            file_reference_factory=_FileReferenceFactory(),
            http_client=http_client,
        ),
    )

    assert file_saver.http_client is http_client


def test_file_saver_impl_uses_default_http_client_for_dependency_bundle() -> None:
    file_saver = FileSaverImpl(
        dependencies=FileSaverDependencies(
            tool_file_manager=_ToolFileManager(),
            file_reference_factory=_FileReferenceFactory(),
        ),
    )

    assert file_saver.http_client is get_http_client()


def test_file_saver_impl_preserves_explicit_falsey_http_client() -> None:
    http_client = _FalseyHttpClient()

    file_saver = FileSaverImpl(
        dependencies=FileSaverDependencies(
            tool_file_manager=_ToolFileManager(),
            file_reference_factory=_FileReferenceFactory(),
            http_client=http_client,
        ),
    )

    assert file_saver.http_client is http_client


def test_file_saver_impl_rejects_mixed_dependency_styles() -> None:
    dependencies = FileSaverDependencies(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )
    constructor = cast(Any, FileSaverImpl)

    with pytest.raises(
        TypeError,
        match="Use either 'dependencies' or the legacy keyword arguments",
    ):
        constructor(
            dependencies=dependencies,
            tool_file_manager=_ToolFileManager(),
        )


def test_file_saver_impl_requires_complete_legacy_arguments() -> None:
    constructor = cast(Any, FileSaverImpl)

    with pytest.raises(
        TypeError,
        match="requires either 'dependencies=FileSaverDependencies",
    ):
        constructor(
            file_reference_factory=_FileReferenceFactory(),
        )
