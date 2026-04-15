import inspect
import time
from collections.abc import Callable, Generator, Mapping
from http import HTTPStatus

import httpx
import pytest
from pytest_mock import MockerFixture

from graphon.file.models import File
from graphon.http import (
    HttpClientMaxRetriesExceededError,
    HttpResponse,
    HttpStatusError,
    HttpxHttpClient,
    get_http_client,
)
from graphon.nodes.document_extractor.entities import DocumentExtractorNodeData
from graphon.nodes.document_extractor.node import DocumentExtractorNode
from graphon.nodes.http_request import (
    HttpRequestNode,
    HttpRequestNodeData,
    build_http_request_config,
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
        mapping: Mapping[str, object],
    ) -> Mapping[str, object]:
        return mapping


def _build_runtime_state() -> GraphRuntimeState:
    return GraphRuntimeState(
        variable_pool=build_variable_pool(),
        start_at=time.perf_counter(),
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


def test_http_request_node_uses_default_http_client_when_not_injected() -> None:
    node = HttpRequestNode(
        node_id="http",
        config=HttpRequestNodeData(
            title="HTTP Request",
            method="get",
            url="https://example.com",
            authorization={"type": "no-auth"},
            headers="",
            params="",
            body={"type": "none", "data": []},
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

    assert node.http_client is get_http_client()


def test_document_extractor_node_uses_default_http_client_when_not_injected() -> None:
    node = DocumentExtractorNode(
        node_id="extractor",
        config=DocumentExtractorNodeData(
            title="Document Extractor",
            variable_selector=["inputs", "file"],
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=_build_runtime_state(),
    )

    assert node.http_client is get_http_client()


def test_file_saver_impl_uses_default_http_client_when_not_injected() -> None:
    file_saver = FileSaverImpl(
        tool_file_manager=_ToolFileManager(),
        file_reference_factory=_FileReferenceFactory(),
    )

    assert file_saver.http_client is get_http_client()


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
