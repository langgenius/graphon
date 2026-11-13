"""Unit tests for graphon.nodes.llm.reasoning (pure, node-independent)."""

from collections.abc import Sequence

from graphon.nodes.llm.reasoning import FilterPiece, ThinkStreamFilter, split_reasoning


def _feed_pieces(parts: Sequence[str]) -> list[FilterPiece]:
    flt = ThinkStreamFilter()
    pieces: list[FilterPiece] = []
    for part in parts:
        pieces.extend(flt.feed(part))
    pieces.extend(flt.finalize())
    return pieces


def _feed_all(parts: Sequence[str]) -> str:
    return "".join(piece.chunk for piece in _feed_pieces(parts) if piece.kind == "text")


def _feed_all_reasoning(parts: Sequence[str]) -> str:
    return "".join(
        piece.chunk for piece in _feed_pieces(parts) if piece.kind == "reasoning"
    )


def test_filter_strips_single_block() -> None:
    flt = ThinkStreamFilter()

    pieces = flt.feed("<think>x</think>y")
    assert pieces == [
        FilterPiece(kind="reasoning", chunk="x"),
        FilterPiece(kind="text", chunk="y"),
    ]
    assert flt.finalize() == []


def test_filter_preserves_piece_order_with_interleaved_text() -> None:
    flt = ThinkStreamFilter()

    pieces = flt.feed("a<think>b</think>c")

    assert pieces == [
        FilterPiece(kind="text", chunk="a"),
        FilterPiece(kind="reasoning", chunk="b"),
        FilterPiece(kind="text", chunk="c"),
    ]


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

    assert flt.feed("") == []
    assert flt.finalize() == []


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


def test_filter_streams_reasoning_of_single_block() -> None:
    assert _feed_all_reasoning(["<think>plan</think>answer"]) == "plan"


def test_filter_streams_reasoning_split_across_chunks() -> None:
    assert _feed_all_reasoning(["<thi", "nk>plan</thi", "nk>ans", "wer"]) == "plan"


def test_filter_streams_reasoning_with_tag_attributes() -> None:
    assert _feed_all_reasoning(['<think foo="x">p</think>hi']) == "p"


def test_filter_streams_reasoning_of_multiple_blocks_concatenated() -> None:
    # Live stream concatenates without the "\n" join split_reasoning applies.
    assert _feed_all_reasoning(["<think>a</think>X<think>b</think>Y"]) == "ab"


def test_filter_streams_unclosed_trailing_reasoning_on_finalize() -> None:
    # Behavior change vs #171: truncated reasoning is handed out, not dropped.
    flt = ThinkStreamFilter()

    pieces = flt.feed("hi<think>tail")
    assert pieces == [
        FilterPiece(kind="text", chunk="hi"),
        FilterPiece(kind="reasoning", chunk="tail"),
    ]
    assert flt.finalize() == []


def test_filter_streams_reasoning_held_partial_close_on_finalize() -> None:
    # A partial "</thi" held when the stream ends is truncated reasoning.
    flt = ThinkStreamFilter()

    assert flt.feed("<think>ab</thi") == [FilterPiece(kind="reasoning", chunk="ab")]
    assert flt.finalize() == [FilterPiece(kind="reasoning", chunk="</thi")]


def test_filter_streams_reasoning_across_false_partial_close() -> None:
    # "</thi" that grows into "</this" stays reasoning, content after too.
    assert _feed_all_reasoning(["<think>a</thi", "s b</think>c"]) == "a</this b"
    assert _feed_all(["<think>a</thi", "s b</think>c"]) == "c"


def test_filter_streams_no_reasoning_without_think() -> None:
    assert _feed_all_reasoning(["plain ", "text"]) == ""


def test_filter_streams_reasoning_only_output() -> None:
    assert _feed_all_reasoning(["<think>just reasoning</think>"]) == "just reasoning"
    assert _feed_all(["<think>just reasoning</think>"]) == ""


# A literal closing tag inside an inline code span, as reported in #357.
_LITERAL_CLOSE_STREAM = (
    "<think>I shouldn't wrap it in a `</think>` tag or something. "
    "Let me just output the HTML answer directly.</think><html>answer</html>"
)
_LITERAL_CLOSE_REASONING = (
    "I shouldn't wrap it in a `</think>` tag or something. "
    "Let me just output the HTML answer directly."
)
_LITERAL_CLOSE_TEXT = "<html>answer</html>"


def test_split_reasoning_keeps_literal_close_inside_backticks() -> None:
    clean, reasoning = split_reasoning(_LITERAL_CLOSE_STREAM, "separated")

    assert clean == _LITERAL_CLOSE_TEXT
    assert reasoning == _LITERAL_CLOSE_REASONING


def test_filter_keeps_literal_close_inside_backticks() -> None:
    assert _feed_all([_LITERAL_CLOSE_STREAM]) == _LITERAL_CLOSE_TEXT
    assert _feed_all_reasoning([_LITERAL_CLOSE_STREAM]) == _LITERAL_CLOSE_REASONING


def test_filter_keeps_literal_close_across_chunk_boundaries() -> None:
    # The reported stream used 7-character chunks. Also split the backtick run
    # and the closing tag themselves.
    chunked = [
        _LITERAL_CLOSE_STREAM[index : index + 7]
        for index in range(0, len(_LITERAL_CLOSE_STREAM), 7)
    ]
    split_tag = [
        "<think>I shouldn't wrap it in a ",
        "`</thi",
        "nk>` tag or something. ",
        "Let me just output the HTML answer directly.</thi",
        "nk><html>answer</html>",
    ]

    assert _feed_all(chunked) == _LITERAL_CLOSE_TEXT
    assert _feed_all_reasoning(chunked) == _LITERAL_CLOSE_REASONING
    assert _feed_all(split_tag) == _LITERAL_CLOSE_TEXT
    assert _feed_all_reasoning(split_tag) == _LITERAL_CLOSE_REASONING
    assert _feed_all(list(_LITERAL_CLOSE_STREAM)) == _LITERAL_CLOSE_TEXT
    assert _feed_all_reasoning(list(_LITERAL_CLOSE_STREAM)) == _LITERAL_CLOSE_REASONING


def test_literal_close_inside_longer_span_preserves_later_blocks() -> None:
    text = "<think>keep `a literal </think> tag` inside</think>X<think>b</think>Y"

    clean, reasoning = split_reasoning(text, "separated")

    assert clean == "XY"
    assert reasoning == "keep `a literal </think> tag` inside\nb"
    assert _feed_all([text]) == "XY"
    assert _feed_all_reasoning([text]) == "keep `a literal </think> tag` insideb"


def test_closed_code_span_does_not_hide_real_think_close() -> None:
    text = "<think>use `json`\nthen finish</think>answer `code`"
    unmatched = "<think>the ` char\n</think>answer `code`"

    clean, reasoning = split_reasoning(text, "separated")
    unmatched_clean, unmatched_reasoning = split_reasoning(unmatched, "separated")

    assert clean == "answer `code`"
    assert reasoning == "use `json`\nthen finish"
    assert unmatched_clean == "answer `code`"
    assert unmatched_reasoning == "the ` char"
    assert _feed_all([text]) == "answer `code`"
    assert _feed_all_reasoning([text]) == "use `json`\nthen finish"
    assert _feed_all([unmatched]) == "answer `code`"
    # The stream keeps the newline before the real tag; split_reasoning strips it.
    assert _feed_all_reasoning([unmatched]) == "the ` char\n"


def _seven_char_chunks(text: str) -> list[str]:
    return [text[index : index + 7] for index in range(0, len(text), 7)]


def test_escaped_backtick_does_not_open_code_span() -> None:
    text = r"<think>A backtick looks like \`.</think>Use \` in text."
    # Split between the backslash and the backtick it escapes.
    split_escape = ["<think>A backtick looks like \\", "`.</think>Use \\` in text."]

    clean, reasoning = split_reasoning(text, "separated")

    assert clean == r"Use \` in text."
    assert reasoning == r"A backtick looks like \`."
    for parts in ([text], _seven_char_chunks(text), split_escape):
        assert _feed_all(parts) == r"Use \` in text."
        assert _feed_all_reasoning(parts) == r"A backtick looks like \`."

    # Any odd number of backslashes escapes the backtick.
    three = r"<think>three \\\`.</think>Use \` in text."
    assert split_reasoning(three, "separated") == (r"Use \` in text.", r"three \\\`.")
    assert _feed_all(_seven_char_chunks(three)) == r"Use \` in text."
    assert _feed_all_reasoning(_seven_char_chunks(three)) == r"three \\\`."


def test_escaped_backtick_leaves_rest_of_run_as_opener() -> None:
    text = r"<think>a \``</think>` b</think>answer"
    split_escape = ["<think>a \\", "``</think>` b</think>answer"]

    assert split_reasoning(text, "separated") == ("answer", r"a \``</think>` b")
    for parts in ([text], _seven_char_chunks(text), split_escape):
        assert _feed_all(parts) == "answer"
        assert _feed_all_reasoning(parts) == r"a \``</think>` b"


def test_escaped_backslash_before_backtick_still_opens_code_span() -> None:
    text = r"<think>a path C:\\`</think>` stays</think>answer"
    split_backslashes = ["<think>a path C:\\", "\\`</think>` stays</think>answer"]
    split_before_backtick = ["<think>a path C:\\\\", "`</think>` stays</think>answer"]

    clean, reasoning = split_reasoning(text, "separated")

    assert clean == "answer"
    assert reasoning == r"a path C:\\`</think>` stays"
    for parts in (
        [text],
        _seven_char_chunks(text),
        split_backslashes,
        split_before_backtick,
    ):
        assert _feed_all(parts) == "answer"
        assert _feed_all_reasoning(parts) == r"a path C:\\`</think>` stays"


def test_filter_streams_reasoning_inside_open_code_span() -> None:
    flt = ThinkStreamFilter()
    code = "x" * 2000

    streamed = [
        piece.chunk
        for part in _seven_char_chunks("<think>see `" + code)
        for piece in flt.feed(part)
        if piece.kind == "reasoning"
    ]

    # Reasoning before any literal </think> is not held back waiting for the
    # span to close.
    assert "".join(streamed) == "see `" + code

    rest = [*flt.feed("</thi"), *flt.feed("nk>` done</think>answer"), *flt.finalize()]

    assert "".join(p.chunk for p in rest if p.kind == "reasoning") == "</think>` done"
    assert "".join(p.chunk for p in rest if p.kind == "text") == "answer"


def test_unclosed_code_span_does_not_hide_later_think_close() -> None:
    same_line = "<think>see `x" + "y" * 40 + "</think>answer"
    next_line = "<think>see `x" + "y" * 40 + "\nthen</think>answer"

    for text in (same_line, next_line):
        clean, reasoning = split_reasoning(text, "separated")

        assert clean == "answer"
        assert _feed_all(_seven_char_chunks(text)) == "answer"
        assert _feed_all_reasoning(_seven_char_chunks(text)) == reasoning
