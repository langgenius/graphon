import ast
import inspect
from collections.abc import Sequence
from pathlib import Path

from graphon.runtime.read_only_wrappers import ReadOnlyVariablePoolWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables import template_resolution
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

    def test_uses_local_minimal_lookup_protocol(self) -> None:
        module_path = Path(template_resolution.__file__)
        parsed = ast.parse(module_path.read_text())

        imports_read_only_pool = any(
            isinstance(node, ast.ImportFrom)
            and node.module == "graphon.runtime.graph_runtime_state_protocol"
            and any(alias.name == "ReadOnlyVariablePool" for alias in node.names)
            for node in parsed.body
        )
        assert not imports_read_only_pool

        local_protocols = [
            cls
            for cls in vars(template_resolution).values()
            if inspect.isclass(cls)
            and cls.__module__ == template_resolution.__name__
            and getattr(cls, "_is_protocol", False)
        ]
        assert len(local_protocols) == 1

        protocol_cls = local_protocols[0]
        assert protocol_cls.__name__.startswith("_")

        protocol_members = [
            name
            for name, value in protocol_cls.__dict__.items()
            if inspect.isfunction(value) and not name.startswith("__")
        ]
        assert protocol_members == ["get"]
        assert getattr(protocol_cls.__dict__["get"], "__isabstractmethod__", False)

    def test_does_not_mutate_variable_dictionary(self) -> None:
        pool = VariablePool.empty()
        pool.add(("start", "name"), 0)

        convert_template(
            ReadOnlyVariablePoolWrapper(pool),
            "The start.name is {{#start.name#}}",
        )

        assert "The start" not in pool.variable_dictionary
