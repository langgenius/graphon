from graphon.nodes.question_classifier import QuestionClassifierNodeData


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


def test_question_classifier_node_data_defaults_label_to_none() -> None:
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

    assert node_data.classes[0].label is None
