from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import (
    BooleanSegment,
    IntegerSegment,
    NoneSegment,
    ObjectSegment,
    StringSegment,
)
from graphon.variables.variables import (
    RAGPipelineVariable,
    RAGPipelineVariableInput,
    StringVariable,
)


class TestVariablePoolConstruction:
    def test_default_constructor_starts_empty(self) -> None:
        assert VariablePool().flatten() == {}

    def test_from_bootstrap_loads_legacy_inputs(self) -> None:
        system_variable = StringVariable(
            name="system_name",
            value="sys-value",
            selector=["wrong", "system_name"],
        )
        conversation_variable = StringVariable(
            name="session_name",
            value="before",
        )
        rag_variable = RAGPipelineVariableInput(
            variable=RAGPipelineVariable(
                belong_to_node_id="retriever",
                type="text-input",
                label="Question",
                variable="query",
            ),
            value="answer",
        )

        pool = VariablePool.from_bootstrap(
            system_variables=[system_variable],
            conversation_variables=[conversation_variable],
            rag_pipeline_variables=[rag_variable],
            user_inputs={"query": "ignored"},
        )

        normalized_system_variable = pool.get_variable(("sys", "system_name"))
        assert normalized_system_variable is not None
        assert tuple(normalized_system_variable.selector) == ("sys", "system_name")

        conversation_segment = pool.get(("conversation", "session_name"))
        assert conversation_segment is not None
        assert conversation_segment.value == "before"

        rag_segment = pool.get(("rag", "retriever"))
        assert rag_segment is not None
        assert rag_segment.value == {"query": "answer"}


class TestVariablePoolGetAndNestedAttribute:
    def test__get_nested_attribute_existing_key(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"a": 123})
        segment = pool.get(("node1", "obj", "a"))
        assert segment is not None
        assert segment.value == 123

    def test__get_nested_attribute_missing_key(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"a": 123})
        segment = pool.get(("node1", "obj", "b"))
        assert segment is None

    def test__get_nested_attribute_non_dict(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), ["not", "a", "dict"])
        segment = pool.get(("node1", "obj", "a"))
        assert segment is None

    def test__get_nested_attribute_with_none_value(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"a": None})
        segment = pool.get(("node1", "obj", "a"))
        assert segment is not None
        assert isinstance(segment, NoneSegment)

    def test__get_nested_attribute_with_empty_string(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"a": ""})
        segment = pool.get(("node1", "obj", "a"))
        assert segment is not None
        assert isinstance(segment, StringSegment)
        assert not segment.value

    def test_get_simple_variable(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "var1"), "value1")
        segment = pool.get(("node1", "var1"))
        assert segment is not None
        assert segment.value == "value1"

    def test_get_missing_variable(self) -> None:
        pool = VariablePool.empty()
        result = pool.get(("node1", "unknown"))
        assert result is None

    def test_get_with_too_short_selector(self) -> None:
        pool = VariablePool.empty()
        result = pool.get(("only_node",))
        assert result is None

    def test_get_nested_object_attribute(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"inner": "hello"})

        segment = pool.get(("node1", "obj", "inner"))
        assert segment is not None
        assert segment.value == "hello"

    def test_get_nested_object_missing_attribute(self) -> None:
        pool = VariablePool.empty()
        pool.add(("node1", "obj"), {"inner": "hello"})

        result = pool.get(("node1", "obj", "not_exist"))
        assert result is None

    def test_get_nested_object_attribute_with_falsy_values(self) -> None:
        pool = VariablePool.empty()
        pool.add(
            ("node1", "obj"),
            {
                "inner_none": None,
                "inner_empty": "",
                "inner_zero": 0,
                "inner_false": False,
            },
        )

        segment_none = pool.get(("node1", "obj", "inner_none"))
        assert segment_none is not None
        assert isinstance(segment_none, NoneSegment)

        segment_empty = pool.get(("node1", "obj", "inner_empty"))
        assert segment_empty is not None
        assert isinstance(segment_empty, StringSegment)
        assert not segment_empty.value

        segment_zero = pool.get(("node1", "obj", "inner_zero"))
        assert segment_zero is not None
        assert isinstance(segment_zero, IntegerSegment)
        assert segment_zero.value == 0

        segment_false = pool.get(("node1", "obj", "inner_false"))
        assert segment_false is not None
        assert isinstance(segment_false, BooleanSegment)
        assert segment_false.value is False

    def test_add_keeps_variable_instances_and_supports_segments(self) -> None:
        pool = VariablePool.empty()
        variable = StringVariable(name="name", selector=["node1", "name"], value="Joe")
        pool.add(("node1", "name"), variable)
        pool.add(("node1", "profile"), ObjectSegment(value={"city": "Paris"}))

        assert pool.variable_dictionary["node1"]["name"] is variable
        segment = pool.get(("node1", "profile", "city"))
        assert segment is not None
        assert segment.value == "Paris"


class TestVariablePoolGetNotModifyVariableDictionary:
    _NODE_ID = "start"
    _VAR_NAME = "name"

    def test_convert_to_template_should_not_introduce_extra_keys(self) -> None:
        pool = VariablePool.empty()
        pool.add([self._NODE_ID, self._VAR_NAME], 0)
        pool.convert_template("The start.name is {{#start.name#}}")
        assert "The start" not in pool.variable_dictionary

    def test_get_should_not_modify_variable_dictionary(self) -> None:
        pool = VariablePool.empty()
        pool.get([self._NODE_ID, self._VAR_NAME])
        assert len(pool.variable_dictionary) == 0
        assert "start" not in pool.variable_dictionary

        pool = VariablePool.empty()
        pool.add([self._NODE_ID, self._VAR_NAME], "Joe")
        pool.get([self._NODE_ID, "count"])
        start_subdict = pool.variable_dictionary[self._NODE_ID]
        assert "count" not in start_subdict
