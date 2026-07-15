from graphon.variables import SecretVariable, SegmentGroup, StringSegment
from graphon.workflow_type_encoder import WorkflowRuntimeTypeConverter

SECRET = "sk-1234567890abcdef"  # noqa: S105
MASKED = "sk-123" + "*" * 12 + "ef"  # _obfuscated_token(SECRET)


def test_secret_variable_is_masked() -> None:
    result = WorkflowRuntimeTypeConverter().to_json_encodable({
        "key": SecretVariable(value=SECRET, name="api_key")
    })
    assert result == {"key": MASKED}


def test_segment_group_masks_only_the_secret_part() -> None:
    group = SegmentGroup(
        value=[StringSegment(value="Bearer "), SecretVariable(value=SECRET, name="k")]
    )
    result = WorkflowRuntimeTypeConverter().to_json_encodable({"auth": group})
    assert result == {"auth": "Bearer " + MASKED}


def test_secret_nested_in_list_is_masked() -> None:
    result = WorkflowRuntimeTypeConverter().to_json_encodable({
        "items": [SecretVariable(value=SECRET, name="k")]
    })
    assert result == {"items": [MASKED]}


def test_non_secret_values_are_unchanged() -> None:
    result = WorkflowRuntimeTypeConverter().to_json_encodable({
        "a": StringSegment(value="hello"),
        "b": [1, 2, "x"],
        "c": {"n": 3},
    })
    assert result == {"a": "hello", "b": [1, 2, "x"], "c": {"n": 3}}


def test_non_sensitive_segment_group_serializes_as_list() -> None:
    # A non-sensitive SegmentGroup recurses into .value (list[Segment]),
    # so each StringSegment further recurses into its .value (str).
    # Result is a list of strings, NOT a joined string.
    group = SegmentGroup(value=[StringSegment(value="a"), StringSegment(value="b")])
    result = WorkflowRuntimeTypeConverter().to_json_encodable({"data": group})
    assert result == {"data": ["a", "b"]}
