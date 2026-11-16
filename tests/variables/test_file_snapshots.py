import json
from collections import deque
from dataclasses import dataclass
from typing import Literal, cast

import pytest
from jsonschema import Draft202012Validator
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    SerializationInfo,
    TypeAdapter,
    model_serializer,
)
from pydantic_core import PydanticSerializationError

from graphon.file import File, FileTransferMethod, FileType
from graphon.variables.segments import (
    ArrayAnySegment,
    FileSegment,
    ObjectSegment,
    SerializableSegment,
    StringSegment,
)
from graphon.variables.variables import StringVariable, Variable


@pytest.mark.parametrize("mode", ["python", "json"])
def test_snapshots_preserve_files_inside_serializable_models(
    mode: Literal["python", "json"],
) -> None:
    class Attachment(BaseModel):
        visible: File = Field(serialization_alias="file")
        hidden: File = Field(exclude=True)

    @dataclass
    class Document:
        file: File

    class CustomAttachment(BaseModel):
        file: File

        @model_serializer
        def serialize_attachment(self, info: SerializationInfo) -> dict[str, File]:
            return {cast("dict[str, str]", info.context)["file_field"]: self.file}

    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    file_data = file.model_dump(mode="json")
    segment = ArrayAnySegment(
        value=[
            FileSegment(value=file),
            ObjectSegment(value={"file": file, "json": file_data}),
            ArrayAnySegment(value=[file, file_data]),
            Attachment(visible=file, hidden=file),
            Document(file),
            RootModel[File](file),
            CustomAttachment(file=file),
            deque([file, file_data]),
        ]
    )
    for _ in range(2):
        snapshot = segment.model_dump(
            mode=mode, by_alias=True, context={"file_field": "attachment"}
        )
        segment = ArrayAnySegment.model_validate(snapshot)

    value = segment.value
    assert value[0] == {"value_type": "file", "value": file}
    assert value[1] == {
        "value_type": "object",
        "value": {"file": file, "json": file_data},
    }
    assert value[2]["value_type"] == "array[any]"
    assert value[2]["value"] == [file, file_data]
    assert value[3] == {"file": file}
    assert value[4] == {"file": file}
    assert value[5] == file
    assert value[6] == {"attachment": file}
    assert list(value[7]) == [file, file_data]


def test_snapshots_keep_the_declared_file_fields() -> None:
    class InternalFile(File):
        credential: str = "private-credential"

    class Attachment(BaseModel):
        file: File

    file = InternalFile(
        type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    attachment = Attachment(file=file)

    snapshot = ArrayAnySegment(value=[attachment]).model_dump(mode="json")

    assert snapshot["value"] == [attachment.model_dump(mode="json")]
    restored = ArrayAnySegment.model_validate(snapshot)
    assert isinstance(restored.value[0]["file"], File)
    assert restored.value[0]["file"].reference == "upload-id"


def test_snapshots_keep_the_declared_segment_fields() -> None:
    class InternalArray(ArrayAnySegment):
        credential: str = "private-credential"

    class Result(BaseModel):
        output: ArrayAnySegment

    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    result = Result(output=InternalArray(value=[file]))

    snapshot = result.model_dump(mode="json")

    assert "credential" not in snapshot["output"]
    restored = Result.model_validate(snapshot)
    assert isinstance(restored.output.value[0], File)
    assert restored.output.value[0].reference == "upload-id"


def test_failed_snapshots_leave_file_serialization_unchanged() -> None:
    class BrokenAttachment(BaseModel):
        file: File

        @model_serializer
        def serialize_attachment(self) -> dict[str, File]:
            msg = "Cannot serialize attachment"
            raise ValueError(msg)

    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    original = file.model_dump()

    with pytest.raises(PydanticSerializationError, match="Cannot serialize attachment"):
        ArrayAnySegment(value=[file, BrokenAttachment(file=file)]).model_dump_json()

    assert file.model_dump() == original


@pytest.mark.parametrize("mode", ["python", "json"])
def test_filtered_exports_allow_plain_json_object_keys(
    mode: Literal["python", "json"],
) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    segment = ArrayAnySegment(value=[{7: file}, None])

    data = segment.model_dump(mode=mode, exclude={"value": {1}})

    key = 7 if mode == "python" else "7"
    assert data["value"] == [{key: file.model_dump(mode=mode)}]
    assert "file_paths" not in data


@pytest.mark.parametrize("container", [set, frozenset])
def test_snapshots_preserve_files_in_sets(
    container: type[set] | type[frozenset],
) -> None:
    class FrozenFile(File):
        model_config = ConfigDict(frozen=True)

    file = FrozenFile(
        type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    file_data = file.model_dump(mode="json")
    segment = ArrayAnySegment(value=[container([file]), file_data])

    restored = ArrayAnySegment.model_validate_json(segment.model_dump_json())

    assert len(restored.value[0]) == 1
    assert isinstance(restored.value[0][0], File)
    assert restored.value[0][0].reference == "upload-id"
    assert restored.value[1] == file_data


@pytest.mark.parametrize("json_mode", [False, True], ids=["python", "json"])
def test_mixed_array_snapshots_distinguish_files_from_plain_json(
    json_mode: bool,
) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    file_data = file.model_dump(mode="json")
    value = [
        file,
        file_data,
        {"files": [file, file_data], "file_paths": [[0]]},
        {"dify_model_identity": "__dify__file__"},
    ]
    segment = ArrayAnySegment(value=value)

    for _ in range(2):
        segment = (
            ArrayAnySegment.model_validate_json(segment.model_dump_json())
            if json_mode
            else ArrayAnySegment.model_validate(segment.model_dump())
        )

    assert segment.to_object() == value


@pytest.mark.parametrize("json_mode", [False, True], ids=["python", "json"])
@pytest.mark.parametrize("include", [False, True], ids=["exclude", "include"])
@pytest.mark.parametrize("keep_index", [0, 1], ids=["file", "json"])
@pytest.mark.parametrize("selector", ["value", "__all__"])
def test_partial_array_dumps_do_not_turn_remaining_json_into_files(
    json_mode: bool, include: bool, keep_index: int, selector: str
) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    file_data = file.model_dump(mode="json")
    segment = ArrayAnySegment(value=[file, file_data])
    mode = "json" if json_mode else "python"
    data = (
        segment.model_dump(mode=mode, include={selector: {keep_index}})
        if include
        else segment.model_dump(mode=mode, exclude={selector: {1 - keep_index}})
    )

    restored = ArrayAnySegment.model_validate(data)

    assert restored.to_object() == [file_data]


@pytest.mark.parametrize(("file_key", "plain_key"), [(1, "1"), ("1", 1), ("file", 2)])
def test_file_snapshots_require_string_keys(
    file_key: str | int, plain_key: str | int
) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload-id",
    )
    value = [{file_key: file, plain_key: file.model_dump(mode="json")}]

    with pytest.raises(PydanticSerializationError, match="string"):
        ArrayAnySegment(value=value).model_dump_json()


def test_plain_mapping_with_integer_keys_remains_json() -> None:
    segment = ArrayAnySegment(value=[{1: "plain"}])

    assert json.loads(segment.model_dump_json())["value"] == [{"1": "plain"}]


@pytest.mark.parametrize("use_variable", [False, True], ids=["segment", "variable"])
def test_serialized_union_values_match_their_json_schema(use_variable: bool) -> None:
    adapter = TypeAdapter(Variable if use_variable else SerializableSegment)
    value = (
        StringVariable(name="text", value="hello")
        if use_variable
        else StringSegment(value="hello")
    )

    Draft202012Validator(adapter.json_schema(mode="serialization")).validate(
        adapter.dump_python(value, mode="json")
    )
