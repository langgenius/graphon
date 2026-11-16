import json

import pytest

from graphon.file import File, FileTransferMethod, FileType
from graphon.runtime.read_only_wrappers import ReadOnlyVariablePoolWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import ArrayObjectSegment, StringSegment
from graphon.variables.template_resolution import convert_template


class TestConvertTemplate:
    @pytest.mark.parametrize(
        "node_id",
        ["input-check", "550e8400-e29b-41d4-a716-446655440000", "-input-"],
    )
    def test_resolves_hyphenated_node_ids(self, node_id: str) -> None:
        pool = VariablePool.empty()
        pool.add((node_id, "result"), {"word": "red"})
        pool.add(("input_check", "result"), {"word": "blue"})
        template = "{{#" + node_id + ".result.word#}} / {{#input_check.result.word#}}"

        rendered = convert_template(ReadOnlyVariablePoolWrapper(pool), template)

        assert rendered.text == "red / blue"

    @pytest.mark.parametrize(
        "selector",
        ["input check.word", "input-check.bad-name", "a" * 51 + ".word"],
    )
    def test_preserves_unsupported_references(self, selector: str) -> None:
        pool = VariablePool.empty()
        pool.add(selector.split("."), "must not be substituted")
        template = "{{#" + selector + "#}}"

        assert convert_template(pool, template).text == template

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

    def test_inserts_object_arrays_with_files_and_segments_as_json(self) -> None:
        file = File(
            file_type=FileType.IMAGE,
            transfer_method=FileTransferMethod.REMOTE_URL,
            remote_url="https://example.com/image.png",
            filename="image.png",
        )
        pool = VariablePool.empty()
        pool.add(
            ("iteration", "output"),
            [{"file": file, "group": {"output": StringSegment(value="hello")}}],
        )

        rendered = convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            '{"items": {{#iteration.output#}}}',
        )
        item = json.loads(rendered.text)["items"][0]

        assert item["file"]["remote_url"] == "https://example.com/image.png"
        assert item["file"]["filename"] == "image.png"
        assert item["group"]["output"] == {"value_type": "string", "value": "hello"}
