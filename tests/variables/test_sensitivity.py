from graphon.variables import SecretVariable, SegmentGroup, StringSegment
from graphon.variables.sensitivity import is_sensitive

SECRET = "sk-1234567890abcdef"


def test_secret_variable_is_sensitive():
    assert is_sensitive(SecretVariable(value=SECRET, name="api_key")) is True


def test_plain_string_segment_is_not_sensitive():
    assert is_sensitive(StringSegment(value="hello")) is False


def test_segment_group_with_secret_is_sensitive():
    group = SegmentGroup(value=[StringSegment(value="Bearer "), SecretVariable(value=SECRET, name="k")])
    assert is_sensitive(group) is True


def test_segment_group_without_secret_is_not_sensitive():
    group = SegmentGroup(value=[StringSegment(value="a"), StringSegment(value="b")])
    assert is_sensitive(group) is False


def test_nested_segment_group_with_secret_is_sensitive():
    inner = SegmentGroup(value=[SecretVariable(value=SECRET, name="k")])
    outer = SegmentGroup(value=[StringSegment(value="x"), inner])
    assert is_sensitive(outer) is True
