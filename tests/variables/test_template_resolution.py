from collections.abc import Sequence

from graphon.runtime.read_only_wrappers import ReadOnlyVariablePoolWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import Segment
from graphon.variables.template_resolution import convert_template


class _LookupOnlyPool:
    def get(self, selector: Sequence[str], /) -> Segment | None:
        if list(selector) == ["start", "name"]:
            pool = VariablePool.empty()
            pool.add(("start", "name"), "Joe")
            return pool.get(("start", "name"))
        return None


class TestConvertTemplate:
    def test_resolves_variables_from_read_only_pool(self) -> None:
        pool = VariablePool.empty()
        pool.add(("start", "name"), "Joe")

        rendered = convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            "The start.name is {{#start.name#}}",
        )

        assert rendered.text == "The start.name is Joe"
        assert [segment.value for segment in rendered.value] == [
            "The start.name is ",
            "Joe",
        ]

    def test_accepts_minimal_lookup_protocol(self) -> None:
        rendered = convert_template(
            _LookupOnlyPool(),
            "The start.name is {{#start.name#}}",
        )

        assert rendered.text == "The start.name is Joe"

    def test_does_not_mutate_variable_dictionary(self) -> None:
        pool = VariablePool.empty()
        pool.add(("start", "name"), 0)

        convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            "The start.name is {{#start.name#}}",
        )

        assert "The start" not in pool.variable_dictionary
