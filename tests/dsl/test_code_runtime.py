from __future__ import annotations

from http import HTTPStatus
from typing import Any

import pytest

from graphon.dsl.code_runtime import SandboxCodeExecutionError, SandboxCodeExecutor
from graphon.dsl.entities import DslCodeSettings
from graphon.nodes.code.entities import CodeLanguage


class _Response:
    def __init__(self, *, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


def test_sandbox_code_executor_posts_dify_sandbox_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> _Response:
        captured["url"] = url
        captured.update(kwargs)
        return _Response(
            status_code=HTTPStatus.OK,
            payload={
                "code": 0,
                "data": {
                    "stdout": 'noise <<RESULT>>{"answer": "1e+3"}<<RESULT>>',
                    "error": "",
                },
            },
        )

    monkeypatch.setattr("graphon.dsl.code_runtime.httpx.post", fake_post)

    executor = SandboxCodeExecutor(
        DslCodeSettings(
            execution_endpoint="http://sandbox:8194/",
            execution_api_key="secret",
            ssl_verify=False,
        )
    )
    result = executor.execute(
        language=CodeLanguage.PYTHON3,
        code="def main(query):\n    return {'answer': query}",
        inputs={"query": "Graphon"},
    )

    assert result == {"answer": 1000.0}
    assert captured["url"] == "http://sandbox:8194/v1/sandbox/run"
    assert captured["headers"] == {"X-Api-Key": "secret"}
    assert captured["verify"] is False
    assert captured["json"]["language"] == "python3"
    assert "main(**inputs_obj)" in captured["json"]["code"]


def test_sandbox_code_executor_reports_sandbox_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_post(_url: str, **_: Any) -> _Response:
        return _Response(
            status_code=HTTPStatus.OK,
            payload={"code": 0, "data": {"stdout": "", "error": "boom"}},
        )

    monkeypatch.setattr("graphon.dsl.code_runtime.httpx.post", fake_post)

    executor = SandboxCodeExecutor(DslCodeSettings(execution_endpoint="http://sandbox"))
    with pytest.raises(SandboxCodeExecutionError, match="boom"):
        executor.execute(
            language=CodeLanguage.JAVASCRIPT, code="function main() {}", inputs={}
        )
