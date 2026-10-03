from graphon.runtime.read_only_wrappers import ReadOnlyVariablePoolWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import ArrayObjectSegment
from graphon.variables.template_resolution import convert_template


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

    def test_does_not_mutate_variable_dictionary(self) -> None:
        pool = VariablePool.empty()
        pool.add(("start", "name"), 0)

        convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            "The start.name is {{#start.name#}}",
        )

        assert "The start" not in pool.variable_dictionary

    def test_inserts_array_object_as_json(self) -> None:
        pool = VariablePool.empty()
        pool.add(
            ("code", "items"),
            [
                {"sku": "A1", "gift": True, "note": None},
                {"sku": "B2", "gift": False, "note": "Tom's"},
            ],
        )

        rendered = convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            '{"items": {{#code.items#}}}',
        )

        assert rendered.text == (
            '{"items": [{"sku": "A1", "gift": true, "note": null}, '
            '{"sku": "B2", "gift": false, "note": "Tom\'s"}]}'
        )

        pool.add(("code", "nullable"), [{"a": None}])
        nullable = convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            "{{#code.nullable#}}",
        )
        assert nullable.text == '[{"a": null}]'
        assert ArrayObjectSegment(value=[]).text == ""
