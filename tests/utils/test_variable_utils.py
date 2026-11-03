import json
from unittest.mock import patch

import pytest
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter
from pydantic_core import PydanticSerializationError

from graphon.file import File, FileTransferMethod, FileType
from graphon.variables.factory import build_segment
from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import (
    ArrayAnySegment,
    SerializableSegment,
    StringSegment,
)
from graphon.variables.utils import dumps_with_segments, segment_orjson_default
from graphon.variables.variables import StringVariable, Variable


def test_segment_orjson_default_uses_match_dispatch() -> None:
    segment = StringSegment(value="value")
    group = SegmentGroup(value=[segment])

    assert segment_orjson_default(segment) == "value"
    assert segment_orjson_default(group) == ["value"]


def test_mixed_array_preserves_plain_file_json() -> None:
    file = File(
        file_type=FileType.IMAGE,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.com/image.png",
        related_id="upload-id",
        filename="image.png",
        mime_type="image/png",
        size=42,
    )
    file_data = file.model_dump(mode="json")
    value = ["metadata", file_data, {"files": [file_data]}]
    segment = ArrayAnySegment(value=value)

    serialized = dumps_with_segments(segment)

    assert json.loads(serialized) == value
    restored = ArrayAnySegment(value=json.loads(serialized))
    assert segment.to_object() == value
    assert restored.to_object() == value


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


@pytest.mark.parametrize("use_plugin_parameter", [False, True])
def test_mixed_array_preserves_public_file_json(use_plugin_parameter: bool) -> None:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.com/report.pdf",
        related_id="upload-id",
        filename="report.pdf",
    )
    with patch.object(File, "generate_url", return_value=file.remote_url):
        file_data = (
            file.to_plugin_parameter() if use_plugin_parameter else file.to_dict()
        )
    value = [None, file_data, {"files": [file_data]}]

    with patch.object(
        File,
        "generate_url",
        side_effect=AssertionError("Must not resolve public file JSON"),
    ):
        segment = build_segment(value)
        serialized = dumps_with_segments(segment)
        restored = ArrayAnySegment(value=json.loads(serialized))

    assert segment.to_object() == value
    assert json.loads(serialized) == value
    assert restored.to_object() == value


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
