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
