from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import StringSegment
from graphon.variables.utils import segment_orjson_default


def test_segment_orjson_default_uses_match_dispatch() -> None:
    segment = StringSegment(value="value")
    group = SegmentGroup(value=[segment])

    assert segment_orjson_default(segment) == "value"
    assert segment_orjson_default(group) == ["value"]
