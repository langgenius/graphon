from math import isfinite

type JsonValue = (
    bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None
)
type JsonObject = dict[str, JsonValue]


def copy_json(value: JsonValue) -> JsonValue:
    """Detach a JSON value without coercing scalar types or sharing containers."""
    return _copy_json(value, set())


def _copy_json(value: JsonValue, ancestor_ids: set[int]) -> JsonValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and isfinite(value):
        return value
    if isinstance(value, (list, dict)) and id(value) not in ancestor_ids:
        ancestor_ids.add(id(value))
        try:
            if isinstance(value, list):
                return [_copy_json(item, ancestor_ids) for item in value]
            if all(isinstance(key, str) for key in value):
                return {
                    key: _copy_json(item, ancestor_ids) for key, item in value.items()
                }
        finally:
            ancestor_ids.remove(id(value))
    message = "Expected finite, acyclic JSON with string object keys"
    raise ValueError(message)


def copy_json_object(value: JsonObject) -> JsonObject:
    copied_value = copy_json(value)
    if not isinstance(copied_value, dict):
        message = "Expected a JSON object"
        raise TypeError(message)
    return copied_value


def copy_usage(usage: JsonValue) -> JsonValue:
    return 0 if usage is None else copy_json(usage)
