from __future__ import annotations

from typing import Any

import pytest
import yaml

from graphon.dsl import inspect as inspect_dsl
from graphon.dsl.entities import LoadStatus

_OPENAI_PLUGIN_ID = "langgenius/openai:0.3.8@test"


def _graph_dsl_for_node(node_data: dict[str, Any]) -> str:
    return _graph_dsl_for_nodes(
        nodes=[{"id": "node", "data": node_data}],
        edges=[{"source": "start", "target": "node"}],
    )


def _graph_dsl_for_nodes(
    *,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> str:
    return yaml.safe_dump({
        "kind": "graph",
        "dependencies": [
            {
                "type": "marketplace",
                "value": {
                    "marketplace_plugin_unique_identifier": _OPENAI_PLUGIN_ID,
                },
            },
        ],
        "graph": {
            "nodes": [
                {"id": "start", "data": {"type": "start", "variables": []}},
                *nodes,
            ],
            "edges": edges,
        },
    })


def _http_request_data() -> dict[str, Any]:
    return {
        "type": "http-request",
        "method": "get",
        "url": "https://example.com/api",
        "authorization": {"type": "no-auth"},
        "headers": "",
        "params": "",
        "body": {"type": "none"},
    }


def _variable_aggregator_data() -> dict[str, Any]:
    return {
        "type": "variable-aggregator",
        "output_type": "string",
        "variables": [["missing", "value"], ["start", "candidate"]],
    }


def _assigner_data() -> dict[str, Any]:
    return {
        "type": "assigner",
        "version": "2",
        "items": [
            {
                "variable_selector": ["conversation", "topic"],
                "input_type": "variable",
                "operation": "over-write",
                "value": ["start", "value"],
            },
        ],
    }


def _list_operator_data() -> dict[str, Any]:
    return {
        "type": "list-operator",
        "variable": ["start", "items"],
        "filter_by": {"enabled": False, "conditions": []},
        "order_by": {"enabled": False},
        "limit": {"enabled": False},
    }


def _model(name: str) -> dict[str, Any]:
    return {
        "provider": "langgenius/openai/openai",
        "name": name,
        "mode": "chat",
        "completion_params": {"temperature": 0.1},
    }


def _question_classifier_data() -> dict[str, Any]:
    return {
        "type": "question-classifier",
        "query_variable_selector": ["start", "query"],
        "model": _model("classifier-model"),
        "classes": [
            {"id": "billing", "name": "Billing questions", "label": "Billing"},
            {"id": "refund", "name": "Refund requests", "label": "Refunds"},
        ],
        "instruction": "Choose the best category.",
    }


def _parameter_extractor_data() -> dict[str, Any]:
    return {
        "type": "parameter-extractor",
        "query": ["start", "query"],
        "model": _model("extractor-model"),
        "parameters": [
            {
                "name": "location",
                "type": "string",
                "description": "The requested location",
                "required": True,
            },
        ],
        "instruction": "Extract the requested location.",
        "reasoning_mode": "prompt",
    }


@pytest.mark.parametrize(
    "node_data",
    [
        _http_request_data(),
        _variable_aggregator_data(),
        _assigner_data(),
        _list_operator_data(),
        _question_classifier_data(),
        _parameter_extractor_data(),
    ],
)
def test_default_factory_node_type_is_loadable(node_data: dict[str, Any]) -> None:
    plan = inspect_dsl(_graph_dsl_for_node(node_data))

    assert plan.load_status == LoadStatus.LOADABLE
    assert plan.load_reason is None


@pytest.mark.parametrize(
    "body",
    [
        {
            "type": "binary",
            "data": [{"type": "file", "file": ["start", "file"]}],
        },
        {
            "type": "form-data",
            "data": [
                {"key": "file", "type": "file", "file": ["start", "file"]},
            ],
        },
    ],
)
def test_http_request_file_request_bodies_are_not_loadable(
    body: dict[str, Any],
) -> None:
    node_data = _http_request_data()
    node_data["body"] = body

    plan = inspect_dsl(_graph_dsl_for_node(node_data))

    assert plan.load_status == LoadStatus.UNSUPPORTED
    assert "HTTP request node 'node' is unsupported" in str(plan.load_reason)


def test_http_request_form_data_text_fields_remain_loadable() -> None:
    node_data = _http_request_data()
    node_data["body"] = {
        "type": "form-data",
        "data": [
            {"key": "query", "type": "text", "value": "{{#start.query#}}"},
        ],
    }

    plan = inspect_dsl(_graph_dsl_for_node(node_data))

    assert plan.load_status == LoadStatus.LOADABLE
    assert plan.load_reason is None


def test_unsupported_node_type_and_http_file_body_reasons_are_joined() -> None:
    http_data = _http_request_data()
    http_data["body"] = {
        "type": "binary",
        "data": [{"type": "file", "file": ["start", "file"]}],
    }
    dsl = _graph_dsl_for_nodes(
        nodes=[
            {"id": "unknown", "data": {"type": "not-supported"}},
            {"id": "http", "data": http_data},
        ],
        edges=[
            {"source": "start", "target": "unknown"},
            {"source": "start", "target": "http"},
        ],
    )

    plan = inspect_dsl(dsl)

    assert plan.load_status == LoadStatus.UNSUPPORTED
    assert plan.load_reason == (
        "Unsupported node types: not-supported; "
        "HTTP request node 'http' is unsupported: "
        "binary request bodies require file download support"
    )


@pytest.mark.parametrize(
    "node_data",
    [
        _question_classifier_data(),
        _parameter_extractor_data(),
    ],
)
def test_model_node_provider_is_normalized_to_plugin_vendor(
    node_data: dict[str, Any],
) -> None:
    plan = inspect_dsl(_graph_dsl_for_node(node_data))

    assert plan.document.graph_config is not None
    loaded_node = plan.document.graph_config["nodes"][1]
    assert loaded_node["data"]["model"]["provider"] == "openai"


def test_legacy_variable_assigner_remains_unsupported_by_default() -> None:
    plan = inspect_dsl(_graph_dsl_for_node({"type": "variable-assigner"}))

    assert plan.load_status == LoadStatus.UNSUPPORTED
    assert plan.load_reason == "Unsupported node types: variable-assigner"


@pytest.mark.parametrize(
    "node_type",
    [
        "iteration",
        "iteration-start",
        "loop",
        "loop-start",
        "loop-end",
    ],
)
def test_container_nodes_are_loadable_by_default(node_type: str) -> None:
    plan = inspect_dsl(_graph_dsl_for_node({"type": node_type}))

    assert plan.load_status == LoadStatus.LOADABLE
    assert plan.load_reason is None
