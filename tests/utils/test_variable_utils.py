import json

from graphon.file import File, FileTransferMethod, FileType
from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import ArrayAnySegment, StringSegment
from graphon.variables.utils import dumps_with_segments, segment_orjson_default


def test_segment_orjson_default_uses_match_dispatch() -> None:
    segment = StringSegment(value="value")
    group = SegmentGroup(value=[segment])

    assert segment_orjson_default(segment) == "value"
    assert segment_orjson_default(group) == ["value"]


def test_mixed_array_file_values_survive_json_round_trip() -> None:
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
    assert restored.value[1] == file
    assert restored.value[2]["files"][0] == file
