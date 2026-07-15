"""Tests that secret variable values are masked in the HTTP executor's to_log() output.

The real request fields (params, json, data, content, headers) retain the raw secret
so the actual HTTP call is unaffected.
"""

from __future__ import annotations

from typing import Any, Literal

import pytest

from graphon.nodes.http_request.config import build_http_request_config
from graphon.nodes.http_request.entities import (
    BodyData,
    HttpRequestNodeAuthorization,
    HttpRequestNodeBody,
    HttpRequestNodeData,
    HttpRequestNodeTimeout,
)
from graphon.nodes.http_request.executor import Executor
from graphon.runtime.variable_pool import VariablePool
from graphon.variables import SecretVariable

SECRET = "sk-supersecret-123456"  # noqa: S105
# The obfuscated form per _obfuscated_token(): first 6 chars + 12 stars + last 2 chars.
# len(SECRET) = 22 > 8, so: "sk-sup" + "************" + "56"
OBFUSCATED = "sk-sup" + "*" * 12 + "56"
# When the obfuscated token is percent-encoded (e.g. inside urlencode for params/body)
# the '*' character becomes '%2A'.
OBFUSCATED_URL = "sk-sup" + "%2A" * 12 + "56"


class _FileManager:
    def download(self, f: Any, /) -> bytes:
        raise NotImplementedError


class _StubHttpClient:
    @property
    def max_retries_exceeded_error(self) -> type[Exception]:
        return RuntimeError

    @property
    def request_error(self) -> type[Exception]:
        return RuntimeError

    def get(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("GET", url, max_retries=max_retries, **kwargs)

    def head(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("HEAD", url, max_retries=max_retries, **kwargs)

    def post(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("POST", url, max_retries=max_retries, **kwargs)

    def put(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("PUT", url, max_retries=max_retries, **kwargs)

    def delete(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("DELETE", url, max_retries=max_retries, **kwargs)

    def patch(self, url: str, max_retries: int = 0, **kwargs: Any) -> Any:
        return self._raise("PATCH", url, max_retries=max_retries, **kwargs)

    def _raise(self, method: str, url: str, **kwargs: Any) -> Any:
        msg = f"unexpected {method} request in test stub: {url!r}, {kwargs!r}"
        raise AssertionError(msg)


def _build_pool() -> VariablePool:
    """Build a VariablePool with a SecretVariable env variable API_TOKEN."""
    return VariablePool.from_bootstrap(
        environment_variables=[
            SecretVariable(name="API_TOKEN", value=SECRET),
        ],
    )


_Method = Literal[
    "get", "post", "put", "patch", "delete", "head", "options",
    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS",
]
_BodyType = Literal[
    "none", "form-data", "x-www-form-urlencoded", "raw-text", "json", "binary",
]


def _build_executor(
    *,
    method: _Method = "get",
    url: str = "https://example.com",
    params: str = "",
    headers: str = "",
    body_type: _BodyType = "none",
    body_data: list[BodyData] | None = None,
    body_str: str | None = None,
) -> Executor:
    """Construct an Executor for testing; injects a stub HTTP client."""
    pool = _build_pool()
    if body_data is None and body_str is not None:
        body_data = [BodyData(key="", type="text", value=body_str)]
    elif body_data is None:
        body_data = []
    return Executor(
        node_data=HttpRequestNodeData(
            title="Test HTTP",
            method=method,
            url=url,
            authorization=HttpRequestNodeAuthorization(type="no-auth"),
            headers=headers,
            params=params,
            body=HttpRequestNodeBody(type=body_type, data=body_data),
        ),
        timeout=HttpRequestNodeTimeout(connect=10, read=60, write=60),
        variable_pool=pool,
        http_request_config=build_http_request_config(),
        http_client=_StubHttpClient(),
        file_manager=_FileManager(),
    )


# ---------------------------------------------------------------------------
# Tests: secret in query params
# ---------------------------------------------------------------------------


def test_secret_in_query_param_is_masked_in_log() -> None:
    executor = _build_executor(params="token: {{#env.API_TOKEN#}}")
    log = executor.to_log()
    # Real request field keeps the raw secret
    assert executor.params == [("token", SECRET)]
    # The log must NOT contain the raw secret
    assert SECRET not in log
    # The log contains the obfuscated token (percent-encoded inside a URL)
    assert OBFUSCATED_URL in log


def test_non_secret_query_param_log_is_unchanged() -> None:
    executor = _build_executor(params="page: 1")
    log = executor.to_log()
    assert executor.params == [("page", "1")]
    assert "page=1" in log


# ---------------------------------------------------------------------------
# Tests: secret in headers
# ---------------------------------------------------------------------------


def test_secret_in_non_auth_header_is_masked_in_log() -> None:
    executor = _build_executor(headers="X-API-Key: {{#env.API_TOKEN#}}")
    log = executor.to_log()
    # Real request field keeps the raw secret
    assert executor.headers.get("X-API-Key") == SECRET
    # The log must NOT contain the raw secret
    assert SECRET not in log
    assert OBFUSCATED in log


# ---------------------------------------------------------------------------
# Tests: secret in body - raw-text
# ---------------------------------------------------------------------------


def test_secret_in_raw_text_body_is_masked_in_log() -> None:
    executor = _build_executor(
        method="post",
        body_type="raw-text",
        body_str="api_key={{#env.API_TOKEN#}}",
    )
    log = executor.to_log()
    # Real request field keeps the raw secret
    assert executor.content == f"api_key={SECRET}"
    # The log must NOT contain the raw secret
    assert SECRET not in log
    assert OBFUSCATED in log


# ---------------------------------------------------------------------------
# Tests: secret in body - JSON
# ---------------------------------------------------------------------------


def test_secret_in_json_body_is_masked_in_log() -> None:
    executor = _build_executor(
        method="post",
        body_type="json",
        body_str='{"token": "{{#env.API_TOKEN#}}"}',
    )
    log = executor.to_log()
    # Real request keeps the raw secret (parsed JSON)
    assert executor.json == {"token": SECRET}
    # The log must NOT contain the raw secret
    assert SECRET not in log
    assert OBFUSCATED in log


# ---------------------------------------------------------------------------
# Tests: secret in body - x-www-form-urlencoded
# ---------------------------------------------------------------------------


def test_secret_in_urlencoded_body_is_masked_in_log() -> None:
    executor = _build_executor(
        method="post",
        body_type="x-www-form-urlencoded",
        body_data=[BodyData(key="token", type="text", value="{{#env.API_TOKEN#}}")],
    )
    log = executor.to_log()
    # Real request keeps the raw secret
    assert executor.data == {"token": SECRET}
    # The log must NOT contain the raw secret
    assert SECRET not in log
    # urlencode percent-encodes '*' as '%2A'
    assert OBFUSCATED_URL in log


# ---------------------------------------------------------------------------
# Tests: secret in body - form-data
# ---------------------------------------------------------------------------


def test_secret_in_form_data_body_is_masked_in_log() -> None:
    executor = _build_executor(
        method="post",
        body_type="form-data",
        body_data=[BodyData(key="token", type="text", value="{{#env.API_TOKEN#}}")],
    )
    log = executor.to_log()
    # Real request keeps the raw secret
    assert executor.data == {"token": SECRET}
    # The log must NOT contain the raw secret
    assert SECRET not in log
    assert OBFUSCATED in log


# ---------------------------------------------------------------------------
# Regression: non-secret request log is produced faithfully
# ---------------------------------------------------------------------------


def test_non_secret_json_body_log_is_unchanged() -> None:
    executor = _build_executor(
        method="post",
        body_type="json",
        body_str='{"page": 1}',
    )
    log = executor.to_log()
    assert executor.json == {"page": 1}
    # The literal value should be visible in the log
    assert "1" in log


def test_non_secret_urlencoded_body_log_is_unchanged() -> None:
    executor = _build_executor(
        method="post",
        body_type="x-www-form-urlencoded",
        body_data=[BodyData(key="name", type="text", value="Alice")],
    )
    log = executor.to_log()
    assert executor.data == {"name": "Alice"}
    assert "name=Alice" in log


@pytest.mark.parametrize(
    "body_type",
    ["none", "raw-text", "json", "x-www-form-urlencoded", "form-data"],
)
def test_no_secret_never_leaks_across_body_types(body_type: str) -> None:
    """Parametric smoke test: secret must never appear in log for any body type."""
    if body_type == "none":
        executor = _build_executor(params="token: {{#env.API_TOKEN#}}")
    elif body_type == "raw-text":
        executor = _build_executor(
            method="post",
            body_type="raw-text",
            body_str="{{#env.API_TOKEN#}}",
        )
    elif body_type == "json":
        executor = _build_executor(
            method="post",
            body_type="json",
            body_str='{"key": "{{#env.API_TOKEN#}}"}',
        )
    elif body_type == "x-www-form-urlencoded":
        executor = _build_executor(
            method="post",
            body_type="x-www-form-urlencoded",
            body_data=[BodyData(key="key", type="text", value="{{#env.API_TOKEN#}}")],
        )
    else:  # form-data
        executor = _build_executor(
            method="post",
            body_type="form-data",
            body_data=[BodyData(key="key", type="text", value="{{#env.API_TOKEN#}}")],
        )
    log = executor.to_log()
    assert SECRET not in log, f"Secret leaked in to_log() for body_type={body_type!r}"
