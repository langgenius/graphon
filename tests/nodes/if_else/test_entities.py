from graphon.nodes.if_else.entities import IfElseNodeData
from graphon.nodes.if_else.if_else_node import IfElseNode
from graphon.utils.condition.entities import Condition


def test_iter_cases_normalizes_legacy_conditions() -> None:
    condition = Condition(
        variable_selector=["start", "flag"],
        comparison_operator="is",
        value="yes",
    )
    node_data = IfElseNodeData(
        logical_operator="or",
        conditions=[condition],
    )

    cases = node_data.iter_cases()

    assert len(cases) == 1
    assert cases[0].case_id == "true"
    assert cases[0].logical_operator == "or"
    assert cases[0].conditions == [condition]


def test_extract_variable_mapping_includes_legacy_conditions() -> None:
    node_data = IfElseNodeData(
        conditions=[
            Condition(
                variable_selector=["start", "flag"],
                comparison_operator="is",
                value="yes",
            ),
        ],
    )

    mapping = IfElseNode.extract_variable_selector_to_variable_mapping(
        graph_config={},
        config={
            "id": "if-node",
            "data": node_data.model_dump(mode="json"),
        },
    )

    assert mapping == {
        "if-node.#start.flag#": ["start", "flag"],
    }
