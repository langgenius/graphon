from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.node_events.node import ModelInvokeCompletedEvent
from graphon.nodes.question_classifier import (
    QuestionClassifierNode,
    QuestionClassifierNodeData,
)
from graphon.nodes.question_classifier.question_classifier_node import llm_utils
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from ...helpers import build_graph_init_params


def test_question_classifier_node_data_accepts_optional_label() -> None:
    node_data = QuestionClassifierNodeData.model_validate({
        "title": "Classifier",
        "query_variable_selector": ["start", "sys.query"],
        "model": {
            "provider": "openai",
            "name": "gpt-4o",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [
            {
                "id": "billing",
                "name": "Questions about invoices and charges",
                "label": "Billing",
            }
        ],
        "instruction": "Classify the query",
    })

    assert node_data.classes[0].id == "billing"
    assert node_data.classes[0].name == "Questions about invoices and charges"
    assert node_data.classes[0].label == "Billing"


def test_question_classifier_node_data_defaults_label_to_empty_string() -> None:
    node_data = QuestionClassifierNodeData.model_validate({
        "title": "Classifier",
        "query_variable_selector": ["start", "sys.query"],
        "model": {
            "provider": "openai",
            "name": "gpt-4o",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [{"id": "billing", "name": "Questions about invoices and charges"}],
        "instruction": "Classify the query",
    })

    assert not node_data.classes[0].label


def _build_question_classifier_node(
    node_data: QuestionClassifierNodeData,
    *,
    variable_pool: MagicMock,
    template_renderer: MagicMock,
) -> QuestionClassifierNode:
    return QuestionClassifierNode(
        node_id="classifier",
        config=node_data,
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=0.0,
        ),
        model_instance=MagicMock(
            provider="openai",
            model_name="gpt-4o",
            stop=(),
            parameters={},
        ),
        template_renderer=template_renderer,
        llm_file_saver=MagicMock(),
    )


def test_question_classifier_run_returns_custom_class_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_data = QuestionClassifierNodeData.model_validate({
        "title": "Classifier",
        "query_variable_selector": ["start", "sys.query"],
        "model": {
            "provider": "openai",
            "name": "gpt-4o",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [
            {
                "id": "billing",
                "name": "Questions about invoices and charges",
                "label": "Billing",
            },
            {
                "id": "refund",
                "name": "Questions about refunds",
                "label": "Refund desk",
            },
        ],
        "instruction": "Classify the query",
    })
    variable_pool = MagicMock()
    variable_pool.get.return_value = SimpleNamespace(value="Where is my refund?")
    variable_pool.convert_template.side_effect = lambda value: SimpleNamespace(
        text=value
    )
    template_renderer = MagicMock()
    node = _build_question_classifier_node(
        node_data,
        variable_pool=variable_pool,
        template_renderer=template_renderer,
    )

    monkeypatch.setattr(
        llm_utils,
        "resolve_completion_params_variables",
        lambda parameters, _: parameters,
    )
    monkeypatch.setattr(
        llm_utils,
        "fetch_prompt_messages",
        MagicMock(return_value=([], None)),
    )
    monkeypatch.setattr(node, "_calculate_rest_token", MagicMock(return_value=1024))
    monkeypatch.setattr(node, "_get_prompt_template", MagicMock(return_value=[]))
    monkeypatch.setattr(
        "graphon.nodes.question_classifier.question_classifier_node.LLMNode.invoke_llm",
        lambda **_: iter([
            ModelInvokeCompletedEvent(
                text=(
                    '{"category_id": "refund", '
                    '"category_name": "Questions about refunds"}'
                ),
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ]),
    )

    result = node._run()  # noqa: SLF001

    assert result.outputs["class_name"] == "Questions about refunds"
    assert result.outputs["class_label"] == "Refund desk"
    assert result.outputs["class_id"] == "refund"
    assert result.inputs["model_provider"] == "openai"
    assert result.inputs["model_name"] == "gpt-4o"


def test_question_classifier_run_falls_back_to_canonical_class_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_data = QuestionClassifierNodeData.model_validate({
        "title": "Classifier",
        "query_variable_selector": ["start", "sys.query"],
        "model": {
            "provider": "openai",
            "name": "gpt-4o",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [
            {
                "id": "billing",
                "name": "Questions about invoices and charges",
                "label": "Billing",
            },
            {
                "id": "refund",
                "name": "Questions about refunds",
            },
        ],
        "instruction": "Classify the query",
    })
    variable_pool = MagicMock()
    variable_pool.get.return_value = SimpleNamespace(value="Where is my refund?")
    variable_pool.convert_template.side_effect = lambda value: SimpleNamespace(
        text=value
    )
    template_renderer = MagicMock()
    node = _build_question_classifier_node(
        node_data,
        variable_pool=variable_pool,
        template_renderer=template_renderer,
    )

    monkeypatch.setattr(
        llm_utils,
        "resolve_completion_params_variables",
        lambda parameters, _: parameters,
    )
    monkeypatch.setattr(
        llm_utils,
        "fetch_prompt_messages",
        MagicMock(return_value=([], None)),
    )
    monkeypatch.setattr(node, "_calculate_rest_token", MagicMock(return_value=1024))
    monkeypatch.setattr(node, "_get_prompt_template", MagicMock(return_value=[]))
    monkeypatch.setattr(
        "graphon.nodes.question_classifier.question_classifier_node.LLMNode.invoke_llm",
        lambda **_: iter([
            ModelInvokeCompletedEvent(
                text=(
                    '{"category_id": "refund", '
                    '"category_name": "Questions about refunds"}'
                ),
                usage=LLMUsage.empty_usage(),
                finish_reason="stop",
            ),
        ]),
    )

    result = node._run()  # noqa: SLF001

    assert result.outputs["class_name"] == "Questions about refunds"
    assert result.outputs["class_label"] == "CLASS 2"
    assert result.outputs["class_id"] == "refund"
