import json
from unittest.mock import patch

import pytest

from graphon.file import File, FileTransferMethod, FileType
from graphon.variables.factory import build_segment
from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import (
    ArrayAnySegment,
    StringSegment,
)
from graphon.variables.utils import dumps_with_segments, segment_orjson_default


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
