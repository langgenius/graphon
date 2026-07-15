from graphon.variables import SecretVariable, SegmentGroup, StringSegment
from graphon.workflow_type_encoder import WorkflowRuntimeTypeConverter

SECRET = "sk-1234567890abcdef"
MASKED = "sk-123" + "*" * 12 + "ef"  # _obfuscated_token(SECRET)


def test_secret_variable_is_masked():
    result = WorkflowRuntimeTypeConverter().to_json_encodable({"key": SecretVariable(value=SECRET, name="api_key")})
    assert result == {"key": MASKED}


def test_segment_group_masks_only_the_secret_part():
    group = SegmentGroup(value=[StringSegment(value="Bearer "), SecretVariable(value=SECRET, name="k")])
    result = WorkflowRuntimeTypeConverter().to_json_encodable({"auth": group})
    assert result == {"auth": "Bearer " + MASKED}


def test_secret_nested_in_list_is_masked():
    result = WorkflowRuntimeTypeConverter().to_json_encodable({"items": [SecretVariable(value=SECRET, name="k")]})
    assert result == {"items": [MASKED]}


def test_non_secret_values_are_unchanged():
    result = WorkflowRuntimeTypeConverter().to_json_encodable(
        {"a": StringSegment(value="hello"), "b": [1, 2, "x"], "c": {"n": 3}}
    )
    assert result == {"a": "hello", "b": [1, 2, "x"], "c": {"n": 3}}
