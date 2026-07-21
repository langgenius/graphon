from graphon.variables import (
    SecretVariable,
    SegmentGroup,
    StringSegment,
)
from graphon.variables import (
    is_sensitive as is_sensitive_pkg,
)
from graphon.variables.sensitivity import is_sensitive

SECRET = "sk-1234567890abcdef"  # noqa: S105


def test_secret_variable_is_sensitive() -> None:
    assert is_sensitive(SecretVariable(value=SECRET, name="api_key")) is True


def test_plain_string_segment_is_not_sensitive() -> None:
    assert is_sensitive(StringSegment(value="hello")) is False


def test_segment_group_with_secret_is_sensitive() -> None:
    group = SegmentGroup(
        value=[StringSegment(value="Bearer "), SecretVariable(value=SECRET, name="k")]
    )
    assert is_sensitive(group) is True


def test_segment_group_without_secret_is_not_sensitive() -> None:
    group = SegmentGroup(value=[StringSegment(value="a"), StringSegment(value="b")])
    assert is_sensitive(group) is False


def test_nested_segment_group_with_secret_is_sensitive() -> None:
    inner = SegmentGroup(value=[SecretVariable(value=SECRET, name="k")])
    outer = SegmentGroup(value=[StringSegment(value="x"), inner])
    assert is_sensitive(outer) is True


def test_package_level_export_is_same_function() -> None:
    secret = SecretVariable(value=SECRET, name="api_key")
    assert is_sensitive_pkg(secret) is True
    assert is_sensitive_pkg is is_sensitive
