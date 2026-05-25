from __future__ import annotations

import pytest
import yaml

from graphon.dsl import DslError, inspect, loads


def _app_dsl_with_runtime_variables(
    *,
    environment_variables: object = None,
    conversation_variables: object = None,
) -> str:
    workflow: dict[str, object] = {
        "graph": {
            "nodes": [
                {"id": "start", "data": {"type": "start", "variables": []}},
            ],
            "edges": [],
        },
    }
    if environment_variables is not None:
        workflow["environment_variables"] = environment_variables
    if conversation_variables is not None:
        workflow["conversation_variables"] = conversation_variables
    return yaml.safe_dump({
        "kind": "app",
        "app": {"mode": "workflow"},
        "workflow": workflow,
    })


def test_app_dsl_loads_environment_and_conversation_variables() -> None:
    dsl = _app_dsl_with_runtime_variables(
        environment_variables=[
            {"name": "var1", "value": "env-value"},
        ],
        conversation_variables=[
            {"name": "topic", "value": "conversation-value"},
        ],
    )

    engine = loads(dsl)
    variable_pool = engine.graph_runtime_state.variable_pool
    env_var = variable_pool.get(["env", "var1"])
    conversation_var = variable_pool.get(["conversation", "topic"])

    assert env_var is not None
    assert conversation_var is not None
    assert env_var.to_object() == "env-value"
    assert conversation_var.to_object() == "conversation-value"


def test_app_dsl_keeps_runtime_variables_out_of_graph_config() -> None:
    dsl = yaml.safe_dump({
        "kind": "app",
        "app": {"mode": "workflow"},
        "workflow": {
            "environment_variables": [
                {"name": "config", "value": {"base_url": "https://example.com"}},
            ],
            "conversation_variables": [
                {"name": "recent_topics", "value": ["billing", "refund"]},
            ],
            "graph": {
                "nodes": [
                    {"id": "start", "data": {"type": "start", "variables": []}},
                ],
                "edges": [],
            },
        },
    })

    plan = inspect(dsl)
    assert plan.document.graph_config is not None
    assert "__graphon_bootstrap" not in plan.document.graph_config
    assert plan.document.runtime_variables.environment_variables[0].name == "config"
    assert plan.document.runtime_variables.environment_variables[0].source == {
        "name": "config",
        "value": {"base_url": "https://example.com"},
    }
    assert (
        plan.document.runtime_variables.environment_variables[0].value["base_url"]
        == "https://example.com"
    )

    engine = loads(dsl)
    variable_pool = engine.graph_runtime_state.variable_pool
    config_var = variable_pool.get(["env", "config"])
    topics_var = variable_pool.get(["conversation", "recent_topics"])

    assert config_var is not None
    assert topics_var is not None
    assert config_var.to_object() == {"base_url": "https://example.com"}
    assert topics_var.to_object() == ["billing", "refund"]


def test_app_dsl_rejects_invalid_runtime_variables() -> None:
    dsl = _app_dsl_with_runtime_variables(
        environment_variables=[{"value": "missing-name"}],
    )

    with pytest.raises(DslError) as exc_info:
        inspect(dsl)

    assert exc_info.value.code == "runtime_variables.invalid_name"
    assert exc_info.value.path == "/workflow/environment_variables/0/name"


@pytest.mark.parametrize(
    ("field_name", "value", "expected_code", "expected_path"),
    [
        (
            "environment_variables",
            {"name": "not-a-list"},
            "runtime_variables.invalid",
            "/workflow/environment_variables",
        ),
        (
            "environment_variables",
            ["not-a-mapping"],
            "runtime_variables.invalid_item",
            "/workflow/environment_variables/0",
        ),
        (
            "conversation_variables",
            [{"name": 123, "value": "bad-name"}],
            "runtime_variables.invalid_name",
            "/workflow/conversation_variables/0/name",
        ),
    ],
)
def test_app_dsl_rejects_invalid_runtime_variable_shapes(
    field_name: str,
    value: object,
    expected_code: str,
    expected_path: str,
) -> None:
    kwargs = {field_name: value}
    dsl = _app_dsl_with_runtime_variables(**kwargs)

    with pytest.raises(DslError) as exc_info:
        inspect(dsl)

    assert exc_info.value.code == expected_code
    assert exc_info.value.path == expected_path
