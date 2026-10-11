import json
from http import HTTPStatus
from typing import Any

import httpx
import pytest
import yaml

from graphon.dsl import loads
from graphon.engine_events.graph import (
    GraphRunPartialSucceededEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.node import NodeRunExceptionEvent


@pytest.mark.parametrize(
    ("flag", "flags"),
    [(False, []), (True, [False, True])],
)
@pytest.mark.parametrize("code_fails", [False, True])
def test_code_boolean_defaults_reach_downstream_outputs(
    monkeypatch: pytest.MonkeyPatch,
    flag: bool,
    flags: list[bool],
    code_fails: bool,
) -> None:
    code_outputs = {"flag": not flag, "flags": [not flag]}

    def fake_post(_url: str, **_kwargs: Any) -> httpx.Response:
        return httpx.Response(
            HTTPStatus.OK,
            json={
                "code": 0,
                "data": {
                    "error": "code failed" if code_fails else "",
                    "stdout": f"<<RESULT>>{json.dumps(code_outputs)}<<RESULT>>",
                },
            },
        )

    monkeypatch.setattr("graphon.dsl.code_runtime.httpx.post", fake_post)
    dsl = yaml.safe_dump({
        "kind": "graph",
        "graph": {
            "nodes": [
                {"id": "start", "data": {"type": "start", "variables": []}},
                {
                    "id": "code",
                    "data": {
                        "type": "code",
                        "variables": [],
                        "code_language": "python3",
                        "code": "def main():\n    raise RuntimeError('code failed')"
                        if code_fails
                        else f"def main():\n    return {code_outputs!r}",
                        "outputs": {
                            "flag": {"type": "boolean"},
                            "flags": {"type": "array[boolean]"},
                        },
                        "error_strategy": "default-value",
                        "default_value": [
                            {"key": "flag", "type": "boolean", "value": flag},
                            {
                                "key": "flags",
                                "type": "array[boolean]",
                                "value": json.dumps(flags),
                            },
                        ],
                    },
                },
                {
                    "id": "end",
                    "data": {
                        "type": "end",
                        "outputs": [
                            {"variable": name, "value_selector": ["code", name]}
                            for name in ("flag", "flags")
                        ],
                    },
                },
            ],
            "edges": [
                {"source": "start", "target": "code"},
                {"source": "code", "target": "end"},
            ],
        },
    })

    engine = loads(dsl, workers=1)
    events = list(engine.run())
    completed = events[-1]

    if code_fails:
        assert isinstance(completed, GraphRunPartialSucceededEvent)
        expected_outputs = {"flag": flag, "flags": flags}
        exceptions = [
            event for event in events if isinstance(event, NodeRunExceptionEvent)
        ]
        assert len(exceptions) == 1
        assert exceptions[0].node_id == "code"
    else:
        assert isinstance(completed, GraphRunSucceededEvent)
        expected_outputs = code_outputs
        assert not any(isinstance(event, NodeRunExceptionEvent) for event in events)

    assert completed.outputs == expected_outputs
    assert isinstance(completed.outputs["flag"], bool)
    result_flags = completed.outputs["flags"]
    assert isinstance(result_flags, list)
    assert all(isinstance(item, bool) for item in result_flags)
