"""Unit tests for graphon.nodes.llm.reasoning (pure, node-independent)."""

from graphon.nodes.llm.reasoning import ThinkStreamFilter, split_reasoning


def _feed_all(parts: list[str]) -> str:
    flt = ThinkStreamFilter()
    out = "".join(flt.feed(part) for part in parts)
    return out + flt.finalize()


def test_filter_strips_single_block() -> None:
    flt = ThinkStreamFilter()

    assert flt.feed("<think>x</think>y") == "y"
    assert flt.finalize() == ""


def test_filter_passes_through_non_think_angle_brackets() -> None:
    assert _feed_all(["<div>ok"]) == "<div>ok"


def test_filter_passes_through_tags_with_think_prefix() -> None:
    assert (
        _feed_all(["before<think", "ing>idea</thinking>after"])
        == "before<thinking>idea</thinking>after"
    )


def test_filter_strips_tag_split_across_chunks() -> None:
    assert _feed_all(["<thi", "nk>plan</thi", "nk>ans", "wer"]) == "answer"


def test_filter_handles_tag_attributes() -> None:
    assert _feed_all(['<think foo="x">p</think>hi']) == "hi"


def test_filter_handles_multiple_blocks() -> None:
    assert _feed_all(["<think>a</think>X<think>b</think>Y"]) == "XY"


def test_filter_drops_unclosed_trailing_think() -> None:
    assert _feed_all(["hi<think>tail"]) == "hi"


def test_filter_strips_leading_whitespace_after_reasoning() -> None:
    assert _feed_all(["<think>r</think>", "\n", "answer"]) == "answer"


def test_split_reasoning_strips_closed_block() -> None:
    clean, reasoning = split_reasoning("<think>a</think>hello", "separated")

    assert clean == "hello"
    assert reasoning == "a"


def test_split_reasoning_joins_multiple_blocks() -> None:
    clean, reasoning = split_reasoning(
        "<think>a</think>X<think>b</think>Y", "separated"
    )

    assert clean == "XY"
    assert reasoning == "a\nb"


def test_split_reasoning_strips_unclosed_trailing_block() -> None:
    clean, reasoning = split_reasoning("hello<think>oops", "separated")

    assert clean == "hello"
    assert reasoning == "oops"


def test_split_reasoning_without_think_is_unchanged() -> None:
    clean, reasoning = split_reasoning("plain text", "separated")

    assert clean == "plain text"
    assert reasoning == ""


def test_split_reasoning_tagged_is_noop() -> None:
    clean, reasoning = split_reasoning("<think>a</think>hi", "tagged")

    assert clean == "<think>a</think>hi"
    assert reasoning == ""


def test_filter_is_case_insensitive() -> None:
    assert _feed_all(["<THINK>x</THINK>y"]) == "y"


def test_filter_keeps_discarding_on_false_partial_close() -> None:
    # "</thi" looks like a closing tag start but turns into "</this", which is
    # still reasoning and must stay discarded.
    assert _feed_all(["<think>a</thi", "s b</think>c"]) == "c"


def test_filter_releases_dangling_open_bracket_as_literal() -> None:
    # A "<" that never grows into "<think" is literal text, not a held tag.
    assert _feed_all(["a<", "b"]) == "a<b"


def test_filter_keeps_malformed_open_tag_with_nested_bracket() -> None:
    assert _feed_all(["x<think <y", ">secret</think>z"]) == (
        "x<think <y>secret</think>z"
    )


def test_filter_releases_overlong_partial_open_tag_as_literal() -> None:
    partial = "<think " + ("x" * 600)

    assert _feed_all(["a", partial, " end"]) == f"a{partial} end"


def test_filter_handles_empty_input() -> None:
    flt = ThinkStreamFilter()

    assert flt.feed("") == ""
    assert flt.finalize() == ""


def test_filter_emits_nothing_for_reasoning_only_output() -> None:
    assert _feed_all(["<think>just reasoning</think>"]) == ""


def test_filter_strips_block_streamed_character_by_character() -> None:
    assert _feed_all(list("<think>plan</think>answer")) == "answer"


def test_split_reasoning_is_case_insensitive() -> None:
    clean, reasoning = split_reasoning("<THINK>a</THINK>hi", "separated")

    assert clean == "hi"
    assert reasoning == "a"


def test_split_reasoning_keeps_tags_with_think_prefix() -> None:
    text = "before<thinking>idea</thinking>after"

    clean, reasoning = split_reasoning(text, "separated")

    assert clean == text
    assert reasoning == ""


def test_split_reasoning_keeps_malformed_open_tag_with_nested_bracket() -> None:
    text = "x<think <y>secret</think>z"

    clean, reasoning = split_reasoning(text, "separated")

    assert clean == text
    assert reasoning == ""
