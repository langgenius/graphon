import pytest
from pydantic import ValidationError

from graphon.variables.input_entities import VariableEntity, VariableEntityType

_VALID_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0},
    },
    "required": ["name"],
}


def _make_payload(json_schema):
    return {
        "variable": "profile",
        "label": "profile",
        "type": VariableEntityType.JSON_OBJECT,
        "json_schema": json_schema,
    }


class TestValidateJsonSchema:
    def test_accepts_dict_input(self):
        entity = VariableEntity.model_validate(_make_payload(_VALID_SCHEMA))
        assert entity.json_schema == _VALID_SCHEMA

    def test_accepts_json_string_input(self):
        # The frontend's code editor persists the schema as a raw JSON string.
        raw = (
            "{\n"
            '  "type": "object",\n'
            '  "properties": {\n'
            '    "name": {"type": "string"},\n'
            '    "age": {"type": "integer", "minimum": 0}\n'
            "  },\n"
            '  "required": ["name"]\n'
            "}"
        )
        entity = VariableEntity.model_validate(_make_payload(raw))
        assert entity.json_schema == _VALID_SCHEMA

    def test_treats_none_as_none(self):
        entity = VariableEntity.model_validate(_make_payload(None))
        assert entity.json_schema is None

    def test_treats_empty_string_as_none(self):
        # Frontend "clear schema" sends "" rather than removing the field;
        # treating it as None matches the user's intent of "no constraint".
        entity = VariableEntity.model_validate(_make_payload(""))
        assert entity.json_schema is None

    def test_rejects_malformed_json_string(self):
        with pytest.raises(ValidationError) as exc_info:
            VariableEntity.model_validate(_make_payload('{"type": "object"'))
        assert "not valid JSON" in str(exc_info.value)

    def test_rejects_non_object_non_string_input(self):
        with pytest.raises(ValidationError) as exc_info:
            VariableEntity.model_validate(_make_payload(123))
        assert "must be a JSON object" in str(exc_info.value)

    def test_rejects_semantically_invalid_schema(self):
        bad_schema = {"type": "not_a_real_type"}
        with pytest.raises(ValidationError) as exc_info:
            VariableEntity.model_validate(_make_payload(bad_schema))
        assert "Invalid JSON schema" in str(exc_info.value)

    def test_rejects_semantically_invalid_schema_from_string(self):
        bad_schema_str = '{"type": "not_a_real_type"}'
        with pytest.raises(ValidationError) as exc_info:
            VariableEntity.model_validate(_make_payload(bad_schema_str))
        assert "Invalid JSON schema" in str(exc_info.value)
