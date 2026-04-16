from graphon.nodes.if_else.entities import IfElseNodeData
from graphon.nodes.if_else.if_else_node import IfElseNode
from graphon.utils.condition.entities import Condition


def _condition(selector: list[str], value: str) -> Condition:
    return Condition(
        variable_selector=selector,
        comparison_operator="is",
        value=value,
    )


def _extract_variable_mapping(node_data: IfElseNodeData) -> dict[str, list[str]]:
    return dict(
        IfElseNode.extract_variable_selector_to_variable_mapping(
            graph_config={},
            config={
                "id": "if-node",
                "data": node_data.model_dump(mode="json"),
            },
        )
    )


class TestIfElseNodeDataIterCases:
    def test_iter_cases_normalizes_legacy_conditions(self) -> None:
        condition = _condition(["start", "flag"], "yes")
        node_data = IfElseNodeData(
            logical_operator="or",
            conditions=[condition],
        )

        cases = node_data.iter_cases()

        assert len(cases) == 1
        assert cases[0].case_id == "true"
        assert cases[0].logical_operator == "or"
        assert cases[0].conditions == [condition]

    def test_iter_cases_preserves_explicit_cases(self) -> None:
        legacy_condition = _condition(["legacy", "flag"], "legacy")
        case_condition = _condition(["cases", "flag"], "case")
        explicit_case = IfElseNodeData.Case(
            case_id="explicit",
            logical_operator="and",
            conditions=[case_condition],
        )
        node_data = IfElseNodeData(
            logical_operator="or",
            conditions=[legacy_condition],
            cases=[explicit_case],
        )

        assert node_data.iter_cases() == [explicit_case]

    def test_iter_cases_preserves_explicit_empty_cases(self) -> None:
        node_data = IfElseNodeData(
            conditions=[_condition(["legacy", "flag"], "legacy")],
            cases=[],
        )

        assert node_data.iter_cases() == []


class TestIfElseNodeVariableMapping:
    def test_extract_variable_mapping_includes_legacy_conditions(self) -> None:
        node_data = IfElseNodeData(
            conditions=[_condition(["start", "flag"], "yes")],
        )

        assert _extract_variable_mapping(node_data) == {
            "if-node.#start.flag#": ["start", "flag"],
        }

    def test_extract_variable_mapping_uses_cases_when_present(self) -> None:
        node_data = IfElseNodeData(
            conditions=[_condition(["legacy", "flag"], "legacy")],
            cases=[
                IfElseNodeData.Case(
                    case_id="explicit",
                    logical_operator="and",
                    conditions=[_condition(["cases", "flag"], "case")],
                )
            ],
        )

        assert _extract_variable_mapping(node_data) == {
            "if-node.#cases.flag#": ["cases", "flag"],
        }

    def test_extract_variable_mapping_ignores_legacy_conditions_for_empty_cases(
        self,
    ) -> None:
        node_data = IfElseNodeData(
            conditions=[_condition(["legacy", "flag"], "legacy")],
            cases=[],
        )

        assert _extract_variable_mapping(node_data) == {}
