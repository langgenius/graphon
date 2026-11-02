from __future__ import annotations

import json


class OutputParserError(ValueError):
    """Raised when a markdown-wrapped JSON payload cannot be parsed or validated."""


_JSON_START_CHARS = frozenset(("{", "["))
_FENCE_MARKERS = ("```json", "```")


def _payload_start(text: str) -> int:
    """Offset of the payload's first JSON start character, or -1 when there is none.

    Braces are checked before array brackets, and both before backticks, so an inline
    code span elsewhere in the text cannot hide the payload: the earlier `find` on a
    backtick used to win over a `{` that comes first. Fenced payloads keep priority so
    the fenced block still wins over prose braces that appear before it.
    """
    for marker in (*_FENCE_MARKERS, "{", "["):
        index = text.find(marker)
        if index == -1:
            continue
        if marker in _JSON_START_CHARS:
            return index
        follow = index + len(marker)
        while follow < len(text) and text[follow].isspace():
            follow += 1
        if follow < len(text) and text[follow] in _JSON_START_CHARS:
            return follow
    return -1


def parse_json_markdown(json_string: str) -> dict | list:
    """Extract and parse the first JSON object or array embedded in markdown text."""
    text = json_string.strip()
    start_index = _payload_start(text)
    if start_index == -1:
        msg = "could not find json block in the output."
        raise ValueError(msg)

    # The decoder parses exactly one value and stops there, so trailing prose, a second
    # payload, or a brace inside a string can no longer extend the slice past the value.
    value, _end = json.JSONDecoder().raw_decode(text, start_index)
    return value


def parse_and_check_json_markdown(text: str, expected_keys: list[str]) -> dict:
    try:
        json_obj = parse_json_markdown(text)
    except json.JSONDecodeError as exc:
        msg = f"got invalid json object. error: {exc}"
        raise OutputParserError(msg) from exc

    if isinstance(json_obj, list):
        if len(json_obj) == 1 and isinstance(json_obj[0], dict):
            json_obj = json_obj[0]
        else:
            msg = f"got invalid return object. obj:{json_obj}"
            raise OutputParserError(msg)

    for key in expected_keys:
        if key not in json_obj:
            msg = (
                f"got invalid return object. expected key `{key}` to be present, "
                f"but got {json_obj}"
            )
            raise OutputParserError(msg)

    return json_obj
