import inspect
import time
from collections.abc import Callable, Generator, Mapping
from http import HTTPStatus
from typing import Any

import httpx
import pytest
from pytest_mock import MockerFixture

from graphon.file.models import File
from graphon.http import (
    HttpClientMaxRetriesExceededError,
    HttpClientProtocol,
    HttpResponse,
    HttpStatusError,
    HttpxHttpClient,
    get_default_http_client,
    get_http_client,
    set_http_client,
)
from graphon.nodes.document_extractor.entities import DocumentExtractorNodeData
from graphon.nodes.document_extractor.node import DocumentExtractorNode
from graphon.nodes.http_request import (
    HttpRequestNode,
    HttpRequestNodeData,
    HttpRequestNodeDependencies,
    build_http_request_config,
)
from graphon.nodes.http_request.entities import (
    HttpRequestNodeAuthorization,
    HttpRequestNodeBody,
)
from graphon.nodes.llm.file_saver import FileSaverImpl
from graphon.nodes.llm.node import LLMNode
from graphon.nodes.question_classifier.question_classifier_node import (
    QuestionClassifierNode,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ..helpers import build_graph_init_params, build_variable_pool


class _ToolFileManager:
    def create_file_by_raw(
        self,
        *,
        file_binary: bytes,
        mimetype: str,
        filename: str | None = None,
    ) -> object:
        raise NotImplementedError

    def get_file_generator_by_tool_file_id(
        self,
        tool_file_id: str,
    ) -> tuple[Generator[bytes, None, None] | None, File | None]:
        raise NotImplementedError


class _FileManager:
    def download(self, f: File, /) -> bytes:
        raise NotImplementedError


class _FileReferenceFactory:
    def build_from_mapping(
        self,
        *,
        mapping: Mapping[str, Any],
    ) -> File:
        return File.model_validate(mapping)


class _StubHttpClient:
    def __init__(self, name: str) -> None:
        self.name = name

    @property
    def max_retries_exceeded_error(self) -> type[Exception]:
        return RuntimeError

    @property
    def request_error(self) -> type[Exception]:
        return RuntimeError

    def get(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("GET", url, max_retries=max_retries, **kwargs)

    def head(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("HEAD", url, max_retries=max_retries, **kwargs)

    def post(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("POST", url, max_retries=max_retries, **kwargs)

    def put(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("PUT", url, max_retries=max_retries, **kwargs)

    def delete(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("DELETE", url, max_retries=max_retries, **kwargs)

    def patch(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self._raise("PATCH", url, max_retries=max_retries, **kwargs)

    def _raise(self, method: str, url: str, **kwargs: Any) -> HttpResponse:
        msg = (
            f"unexpected {method} request in test stub {self.name}: {url!r}, {kwargs!r}"
        )
        raise AssertionError(msg)


def _build_runtime_state() -> GraphRuntimeState:
    return GraphRuntimeState(
        variable_pool=build_variable_pool(),
        start_at=time.perf_counter(),
    )


@pytest.fixture(autouse=True)
def _restore_default_http_client() -> Generator[None, None, None]:
    default_http_client = get_default_http_client()
    yield
    set_http_client(default_http_client)


def _build_dependencies(
    *,
    http_client: HttpClientProtocol | None = None,
) -> HttpRequestNodeDependencies:
    return HttpRequestNodeDependencies(
        tool_file_manager_factory=_ToolFileManager,
        file_manager=_FileManager(),
        file_reference_factory=_FileReferenceFactory(),
        http_client=http_client,
    )


def test_httpx_http_client_normalizes_request_kwargs(
    mocker: MockerFixture,
) -> None:
    request = httpx.Request("POST", "https://example.com")
    response = httpx.Response(HTTPStatus.OK, request=request)
    request_mock = mocker.patch(
        "graphon.http.client.httpx.request",
        return_value=response,
    )

    client = HttpxHttpClient()
    returned_response = client.post(
        "https://example.com",
        json={"ok": True},
        ssl_verify=False,
        timeout=(1, 2, 3),
    )

    assert isinstance(returned_response, HttpResponse)
    assert returned_response.status_code == HTTPStatus.OK
    assert returned_response.content == response.content
    request_mock.assert_called_once()
    _, _, kwargs = request_mock.mock_calls[0]
    assert kwargs["verify"] is False
    assert isinstance(kwargs["timeout"], httpx.Timeout)
    assert kwargs["timeout"].connect == 1
    assert kwargs["timeout"].read == 2
    assert kwargs["timeout"].write == 3


def test_httpx_http_client_retries_until_success(mocker: MockerFixture) -> None:
    request = httpx.Request("GET", "https://example.com")
    responses = [
        httpx.ConnectError("boom-1", request=request),
        httpx.ConnectError("boom-2", request=request),
        httpx.Response(HTTPStatus.OK, request=request),
    ]
    request_mock = mocker.patch(
        "graphon.http.client.httpx.request",
        side_effect=responses,
    )

    response = HttpxHttpClient().get("https://example.com", max_retries=2)

    assert response.status_code == HTTPStatus.OK
    assert request_mock.call_count == len(responses)


def test_http_response_raise_for_status_uses_library_error() -> None:
    response = HttpResponse(status_code=404, url="https://example.com/missing")

    with pytest.raises(HttpStatusError):
        response.raise_for_status()


def test_http_response_text_prefers_charset_from_content_type(
    mocker: MockerFixture,
) -> None:
    detected_mock = mocker.patch("graphon.http.response.charset_normalizer.from_bytes")
    response = HttpResponse(
        status_code=HTTPStatus.OK,
        headers={"Content-Type": "application/json; charset=utf-8"},
        content=b"\xe4\xb8\xad\xe6\x96\x87",
    )

    assert response.text == "\u4e2d\u6587"
    detected_mock.assert_not_called()


def test_http_response_text_prefers_fallback_text_over_detection(
    mocker: MockerFixture,
) -> None:
    class _DetectedEncoding:
        encoding = "latin-1"

    class _DetectedMatches:
        def best(self) -> _DetectedEncoding:
            return _DetectedEncoding()

    detected_mock = mocker.patch(
        "graphon.http.response.charset_normalizer.from_bytes",
        return_value=_DetectedMatches(),
    )
    response = HttpResponse(
        status_code=HTTPStatus.OK,
        content=b"\xe4\xb8\xad\xe6\x96\x87",
        fallback_text="\u4e2d\u6587",
    )

    assert response.text == "\u4e2d\u6587"
    detected_mock.assert_not_called()


def test_http_response_from_httpx_preserves_httpx_text_without_charset_header(
    mocker: MockerFixture,
) -> None:
    detected_mock = mocker.patch("graphon.http.response.charset_normalizer.from_bytes")
    request = httpx.Request("GET", "https://example.com")
    raw_response = httpx.Response(
        HTTPStatus.OK,
        request=request,
        content="日本語の本文です".encode("shift_jis"),
    )

    response = HttpResponse.from_httpx(raw_response)

    assert response.text == raw_response.text
    detected_mock.assert_not_called()


def test_http_response_from_httpx_preserves_custom_default_encoding(
    mocker: MockerFixture,
) -> None:
    detected_mock = mocker.patch("graphon.http.response.charset_normalizer.from_bytes")
    request = httpx.Request("GET", "https://example.com")
    raw_response = httpx.Response(
        HTTPStatus.OK,
        request=request,
        content="Привет".encode("cp1251"),
        default_encoding="cp1251",
    )

    response = HttpResponse.from_httpx(raw_response)

    assert response.text == raw_response.text
    detected_mock.assert_not_called()


def test_http_response_text_ignores_charset_fragments_inside_quoted_parameters(
    mocker: MockerFixture,
) -> None:
    detected_mock = mocker.patch("graphon.http.response.charset_normalizer.from_bytes")
    response = HttpResponse(
        status_code=HTTPStatus.OK,
        headers={"Content-Type": 'text/plain; foo="a; charset=latin-1"; charset=utf-8'},
        content=b"\xe4\xb8\xad\xe6\x96\x87",
    )

    assert response.text == "\u4e2d\u6587"
    detected_mock.assert_not_called()


def test_http_response_text_uses_declared_charset_with_replacement(
    mocker: MockerFixture,
) -> None:
    detected_mock = mocker.patch("graphon.http.response.charset_normalizer.from_bytes")
    response = HttpResponse(
        status_code=HTTPStatus.OK,
        headers={"Content-Type": "text/plain; charset=utf-8"},
        content=b"\xffabc",
    )

    assert response.text == "\ufffdabc"
    detected_mock.assert_not_called()


def test_httpx_http_client_raises_max_retries_exceeded_after_last_retry(
    mocker: MockerFixture,
) -> None:
    request = httpx.Request("GET", "https://example.com")
    failures = [
        httpx.ConnectError("boom-1", request=request),
        httpx.ConnectError("boom-2", request=request),
    ]
    request_mock = mocker.patch(
        "graphon.http.client.httpx.request",
        side_effect=failures,
    )

    with pytest.raises(HttpClientMaxRetriesExceededError):
        HttpxHttpClient().get("https://example.com", max_retries=1)

    assert request_mock.call_count == len(failures)


def test_httpx_http_client_raises_request_error_without_retry_wrapping(
    mocker: MockerFixture,
) -> None:
    request = httpx.Request("GET", "https://example.com")
    mocker.patch(
        "graphon.http.client.httpx.request",
        side_effect=httpx.ConnectError("boom", request=request),
    )

    with pytest.raises(httpx.RequestError):
        HttpxHttpClient().get("https://example.com")


def test_set_http_client_updates_process_default() -> None:
    default_http_client = _StubHttpClient("default")

    set_http_client(default_http_client)

    assert get_default_http_client() is default_http_client
    assert get_http_client() is default_http_client


def test_http_request_node_accepts_public_dependency_bundle() -> None:
    node = HttpRequestNode(
        node_id="http",
        data=HttpRequestNodeData(
            title="HTTP Request",
            method="get",
            url="https://example.com",
            authorization=HttpRequestNodeAuthorization(type="no-auth"),
            headers="",
            params="",
            body=HttpRequestNodeBody(type="none", data=[]),
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=_build_runtime_state(),
        http_request_config=build_http_request_config(),
        dependencies=_build_dependencies(),
    )

    assert node.http_client is get_http_client()


def test_http_request_node_rejects_mixed_dependency_inputs() -> None:
    with pytest.raises(
        TypeError,
        match=r"accepts either dependencies=\.\.\. or legacy dependency keywords",
    ):
        HttpRequestNode(
            node_id="http",
            data=HttpRequestNodeData(
                title="HTTP Request",
                method="get",
                url="https://example.com",
                authorization=HttpRequestNodeAuthorization(type="no-auth"),
                headers="",
                params="",
                body=HttpRequestNodeBody(type="none", data=[]),
            ),
            graph_init_params=build_graph_init_params(
                graph_config={"nodes": [], "edges": []},
            ),
            graph_runtime_state=_build_runtime_state(),
            http_request_config=build_http_request_config(),
            dependencies=_build_dependencies(),
            file_manager=_FileManager(),
        )


def test_http_request_node_uses_configured_default_http_client() -> None:
    default_http_client = _StubHttpClient("http-request")
    set_http_client(default_http_client)

    node = HttpRequestNode(
        node_id="http",
        data=HttpRequestNodeData(
            title="HTTP Request",
            method="get",
            url="https://example.com",
            authorization=HttpRequestNodeAuthorization(type="no-auth"),
            headers="",
            params="",
            body=HttpRequestNodeBody(type="none", data=[]),
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=_build_runtime_state(),
        http_request_config=build_http_request_config(),
        tool_file_manager_factory=_ToolFileManager,
        file_manager=_FileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )

    assert node.http_client is default_http_client


def test_document_extractor_node_uses_default_http_client_when_not_injected() -> None:
    node = DocumentExtractorNode(
        node_id="extractor",
        data=DocumentExtractorNodeData(
            title="Document Extractor",
            variable_selector=["inputs", "file"],
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=_build_runtime_state(),
    )

    assert node.http_client is get_http_client()


def test_document_extractor_node_uses_configured_default_http_client() -> None:
    default_http_client = _StubHttpClient("document-extractor")
    set_http_client(default_http_client)

    node = DocumentExtractorNode(
        node_id="extractor",
        data=DocumentExtractorNodeData(
            title="Document Extractor",
            variable_selector=["inputs", "file"],
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=_build_runtime_state(),
    )

    assert node.http_client is default_http_client


def test_file_saver_impl_uses_default_http_client_when_not_injected() -> None:
    file_saver = FileSaverImpl.with_runtime(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )

    assert file_saver.http_client is get_http_client()


def test_file_saver_impl_uses_configured_default_http_client() -> None:
    default_http_client = _StubHttpClient("file-saver")
    set_http_client(default_http_client)

    file_saver = FileSaverImpl(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )

    assert file_saver.http_client is default_http_client


@pytest.mark.parametrize(
    ("callable_obj", "parameter_name"),
    [
        (HttpRequestNode.__init__, "http_client"),
        (DocumentExtractorNode.__init__, "http_client"),
        (FileSaverImpl.__init__, "http_client"),
        (LLMNode.__init__, "http_client"),
        (QuestionClassifierNode.__init__, "http_client"),
    ],
)
def test_http_client_injection_is_optional(
    callable_obj: Callable[..., object],
    parameter_name: str,
) -> None:
    parameter = inspect.signature(callable_obj).parameters[parameter_name]

    assert parameter.default is None


def test_http_request_node_signature_exposes_public_dependencies() -> None:
    parameters = inspect.signature(HttpRequestNode.__init__).parameters

    assert "dependencies" in parameters
    assert parameters["dependencies"].default is None
