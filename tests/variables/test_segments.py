import pytest
from pydantic import TypeAdapter

from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.variables.factory import segment_to_variable
from graphon.variables.segments import SerializableSegment
from graphon.variables.variables import (
    ArrayAnyVariable,
    ArrayBooleanVariable,
    ArrayFileVariable,
    ArrayNumberVariable,
    ArrayObjectVariable,
    ArrayStringVariable,
    BooleanVariable,
    FileVariable,
    FloatVariable,
    IntegerVariable,
    NoneVariable,
    ObjectVariable,
    SecretVariable,
    StringVariable,
    VariableBase,
)


@pytest.mark.parametrize(
    "variable",
    [
        NoneVariable(name="source"),
        StringVariable(name="source", value="text"),
        IntegerVariable(name="source", value=1),
        FloatVariable(name="source", value=0.5),
        BooleanVariable(name="source", value=True),
        ObjectVariable(name="source", value={"key": "text"}),
        FileVariable(
            name="source",
            value=File(
                file_type=FileType.DOCUMENT,
                transfer_method=FileTransferMethod.REMOTE_URL,
                remote_url="https://example.com/file.txt",
            ),
        ),
        ArrayAnyVariable(name="source", value=["text", 1]),
        ArrayStringVariable(name="source", value=[]),
        ArrayNumberVariable(name="source", value=[]),
        ArrayBooleanVariable(name="source", value=[]),
        ArrayObjectVariable(name="source", value=[]),
        ArrayFileVariable(name="source", value=[]),
        SecretVariable(name="source", value="example-value"),
    ],
    ids=[
        "none",
        "string",
        "integer",
        "float",
        "boolean",
        "object",
        "file",
        "any",
        "strings",
        "numbers",
        "booleans",
        "objects",
        "files",
        "secret",
    ],
)
def test_variable_to_segment_preserves_type_and_value_without_identity(
    variable: VariableBase,
) -> None:
    segment = variable.to_segment()

    assert not isinstance(segment, VariableBase)
    assert segment.value_type == variable.value_type
    assert segment.value == variable.value
    rebound = segment_to_variable(segment=segment, selector=["target", "output"])
    assert rebound.id != variable.id
    assert rebound.name == "output"
    assert rebound.selector == ["target", "output"]
    assert rebound.value_type == variable.value_type
    assert rebound.value == variable.value


def test_serializable_segment_restores_secret_type() -> None:
    adapter = TypeAdapter(SerializableSegment)

    segment = adapter.validate_json('{"value_type":"secret","value":"example-value"}')

    assert segment.value_type == "secret"
    assert segment.value == "example-value"
    assert segment.log == "exampl" + "*" * 12 + "ue"
