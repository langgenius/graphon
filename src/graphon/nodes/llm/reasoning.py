"""Handling of inline ``<think>...</think>`` reasoning in LLM output.

Reasoning models emit their chain-of-thought wrapped in ``<think>`` tags inside
the normal text stream. This module owns everything that strips or separates
that reasoning so the two code paths stay consistent:

- :class:`ThinkStreamFilter` strips it incrementally while the node streams
  tokens (``separated`` mode), handling tags split across chunk boundaries.
- :func:`split_reasoning` / :func:`extract_stream_reasoning` strip it in one
  pass from a fully buffered string (blocking results and the streaming
  completion event).

A ``</think>`` inside a same-line backtick span stays in the reasoning text. A
backslash-escaped backtick does not open a span. Streaming and blocking share
:class:`_ThinkBodyScanner`, so a literal tag does not end the block in either.
"""

from __future__ import annotations

import bisect
import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class FilterPiece:
    """One ordered piece split by :class:`ThinkStreamFilter`."""

    kind: Literal["text", "reasoning"]
    chunk: str


# Complete <think>...</think> blocks (attrs, case-insensitive, multiline).
_THINK_OPEN_ATTR_MAX_CHARS = 512
_THINK_OPEN_PREFIX = "<think"
_THINK_OPEN_TAG = rf"<think(?:\s[^<>]{{0,{_THINK_OPEN_ATTR_MAX_CHARS}}})?>"
_THINK_OPEN_RE = re.compile(_THINK_OPEN_TAG, re.IGNORECASE)
_THINK_CLOSE_TAG = "</think>"
_THINK_CLOSE_LEN = len(_THINK_CLOSE_TAG)
_THINK_OPEN_PREFIX_MAX_LEN = len(_THINK_OPEN_PREFIX) + 1 + _THINK_OPEN_ATTR_MAX_CHARS
# Characters that can change the scanner state inside a think body.
_THINK_BODY_SPECIAL_CHAR_RE = re.compile(r"[`<\\\n]")


@dataclass(frozen=True)
class _BacktickRun:
    """A complete run of backticks on the current line."""

    start: int
    length: int
    escaped: bool


def _first_close_outside_spans(
    runs: list[_BacktickRun], closes: list[int]
) -> int | None:
    """Find the first ``</think>`` outside code spans on one complete line.

    An opening run pairs with the next run of the same length. If the first
    backtick of a run is escaped, the rest of the run is the opener.

    Returns:
        The position of that ``</think>``, or ``None`` when every one is inside
        a span.

    """
    run_indexes: dict[int, list[int]] = {}
    for index, run in enumerate(runs):
        run_indexes.setdefault(run.length, []).append(index)

    outside_from = 0
    close_index = 0
    run_index = 0
    while run_index < len(runs):
        run = runs[run_index]
        while close_index < len(closes) and closes[close_index] < outside_from:
            close_index += 1
        if close_index < len(closes) and closes[close_index] < run.start:
            return closes[close_index]
        opener_length = run.length - run.escaped
        same_length = run_indexes.get(opener_length, [])
        closer = bisect.bisect_right(same_length, run_index)
        if opener_length and closer < len(same_length):
            closer_run = runs[same_length[closer]]
            outside_from = closer_run.start + closer_run.length
            run_index = same_length[closer] + 1
        else:
            outside_from = run.start + run.length
            run_index += 1
    while close_index < len(closes) and closes[close_index] < outside_from:
        close_index += 1
    return closes[close_index] if close_index < len(closes) else None


class _ThinkBodyScanner:
    """Find the real end of one think body, chunk by chunk, without rescanning.

    Text is scanned once. Scan state (backtick runs, escapes, a partial
    ``</think>``, an open code span) carries over between chunks. Reasoning is
    released as soon as it cannot be part of the block end; only text from a
    ``</think>`` inside a still-open span is held until the span closes or the
    line ends.
    """

    def __init__(self) -> None:
        self._pending: list[str] = []
        self._pending_start = 0
        self._scanned = 0
        self._close_prefix_len = 0
        self._backslashes = 0
        self._run_start = 0
        self._run_length = 0
        self._run_escaped = False
        self._span_length = 0
        self._line_runs: list[_BacktickRun] = []
        self._line_closes: list[int] = []

    def feed(self, text: str, *, final: bool) -> tuple[str, str | None]:
        """Scan the next chunk of the think body.

        Returns:
            ``(reasoning, text_after_close)``. ``text_after_close`` is ``None``
            while the block is still open. With ``final``, everything not after
            a real close is reasoning.

        """
        chunk_start = self._scanned
        if text:
            self._pending.append(text)
        close = self._scan(text, chunk_start)
        if close is None and final:
            close = self._end_line()
        if close is not None:
            split = close - self._pending_start
            body = self._take_pending()
            return body[:split], body[split + _THINK_CLOSE_LEN :]
        if final:
            return self._take_pending(), None
        return self._release_until(self._hold_start()), None

    def _scan(self, text: str, chunk_start: int) -> int | None:
        index = 0
        while index < len(text):
            if not self._close_prefix_len and not self._run_length:
                special_char = _THINK_BODY_SPECIAL_CHAR_RE.search(text, index)
                next_index = len(text) if special_char is None else special_char.start()
                if next_index > index:
                    self._backslashes = 0
                    index = next_index
                    if special_char is None:
                        break
            char = text[index]
            position = chunk_start + index
            index += 1
            self._scanned = position + 1
            close = self._scan_char(char, position)
            if close is not None:
                return close
        self._scanned = chunk_start + len(text)
        return None

    def _scan_char(self, char: str, position: int) -> int | None:
        if char == "`":
            if not self._run_length:
                self._run_start = position
                self._run_escaped = self._backslashes % 2 == 1
            self._run_length += 1
        elif self._run_length:
            self._end_run()
        self._backslashes = self._backslashes + 1 if char == "\\" else 0
        if char == "\n":
            self._close_prefix_len = 0
            return self._end_line()
        return self._match_close(char, position)

    def _match_close(self, char: str, position: int) -> int | None:
        """Track a ``</think>``; return its start when it ends the block."""
        if char.lower() == _THINK_CLOSE_TAG[self._close_prefix_len]:
            self._close_prefix_len += 1
        else:
            self._close_prefix_len = 1 if char == "<" else 0
        if self._close_prefix_len < _THINK_CLOSE_LEN:
            return None
        self._close_prefix_len = 0
        close = position + 1 - _THINK_CLOSE_LEN
        if not self._span_length:
            return close
        self._line_closes.append(close)
        return None

    def _end_run(self) -> None:
        length = self._run_length
        escaped = self._run_escaped
        self._run_length = 0
        if not self._span_length:
            self._span_length = length - escaped
            return
        if length == self._span_length:
            self._clear_line()
            return
        self._line_runs.append(_BacktickRun(self._run_start, length, escaped))

    def _end_line(self) -> int | None:
        """Settle the line: an unclosed span's opener was literal after all."""
        if self._run_length:
            self._end_run()
        close = None
        if self._span_length:
            close = _first_close_outside_spans(self._line_runs, self._line_closes)
        self._clear_line()
        return close

    def _clear_line(self) -> None:
        self._span_length = 0
        self._line_runs.clear()
        self._line_closes.clear()

    def _hold_start(self) -> int:
        if self._line_closes:
            return self._line_closes[0]
        return self._scanned - self._close_prefix_len

    def _release_until(self, end: int) -> str:
        if end <= self._pending_start:
            return ""
        text = "".join(self._pending)
        split = end - self._pending_start
        self._pending = [text[split:]] if split < len(text) else []
        self._pending_start = end
        return text[:split]

    def _take_pending(self) -> str:
        text = "".join(self._pending)
        self._pending = []
        self._pending_start = self._scanned
        return text


class ThinkStreamFilter:
    """Stateful, chunk-boundary-safe stripper for ``<think>...</think>`` blocks.

    Used only in ``separated`` mode, where the LLM node must not stream
    reasoning to the ``text`` selector. Reasoning arrives inline as
    ``<think>...</think>`` and is streamed token by token, so a single tag can
    be split across chunks (``"<thi" + "nk>"``). This filter buffers only the
    trailing bytes that could still grow into a tag, or the reasoning after a
    literal ``</think>`` inside a still-open backtick span, and emits
    everything else as soon as it arrives.

    ``tagged`` mode does not use this filter at all (it streams raw tokens), so
    the class is unconditional: it always strips.

    Matches :func:`split_reasoning` only up to whitespace: that also collapses
    blank-line runs and strips; this only lstrips the leading text.
    """

    def __init__(self) -> None:
        self._body_scanner: _ThinkBodyScanner | None = None
        self._hold = ""
        self._seen_clean = False

    def feed(self, text_part: str) -> list[FilterPiece]:
        """Return ordered clean-text and reasoning pieces from this chunk."""
        return self._drain(text_part, final=False)

    def finalize(self) -> list[FilterPiece]:
        """Flush whatever is safe to emit once the stream ends."""
        return self._drain("", final=True)

    def _drain(self, text_part: str, *, final: bool) -> list[FilterPiece]:
        pieces: list[FilterPiece] = []
        work = self._hold + text_part
        self._hold = ""
        while work or (final and self._body_scanner is not None):
            if self._body_scanner is None:
                match = _THINK_OPEN_RE.search(work)
                if match:
                    self._append_text_piece(pieces, work[: match.start()])
                    self._body_scanner = _ThinkBodyScanner()
                    work = work[match.end() :]
                    continue
                if not final:
                    keep = self._open_suffix_len(work)
                    if keep:
                        self._append_text_piece(pieces, work[:-keep])
                        self._hold = work[-keep:]
                        work = ""
                        continue
                self._append_text_piece(pieces, work)
                work = ""
                continue
            # A partial "</thi" stays in the scanner until the next chunk, and
            # becomes truncated reasoning when the stream ends.
            reasoning, after_close = self._body_scanner.feed(work, final=final)
            self._append_reasoning_piece(pieces, reasoning)
            if after_close is None:
                if final:
                    self._body_scanner = None
                work = ""
                continue
            self._body_scanner = None
            work = after_close
        return pieces

    def _append_text_piece(
        self,
        pieces: list[FilterPiece],
        clean: str,
    ) -> None:
        clean = self._strip_leading(clean)
        if clean:
            pieces.append(FilterPiece(kind="text", chunk=clean))

    @staticmethod
    def _append_reasoning_piece(
        pieces: list[FilterPiece],
        reasoning: str,
    ) -> None:
        if reasoning:
            pieces.append(FilterPiece(kind="reasoning", chunk=reasoning))

    def _strip_leading(self, clean: str) -> str:
        # Mirror split_reasoning()'s leading strip (reasoning-first models).
        if self._seen_clean or not clean:
            return clean
        stripped = clean.lstrip()
        if stripped:
            self._seen_clean = True
        return stripped

    @staticmethod
    def _open_suffix_len(work: str) -> int:
        """Length of the trailing suffix that could still become ``<think...>``."""
        keep = 0
        lt = work.rfind("<")
        if lt != -1:
            tail = work[lt:]
            tail_lower = tail.lower()
            if ">" in tail:
                keep = 0
            elif len(tail) <= len(_THINK_OPEN_PREFIX):
                keep = len(tail) if _THINK_OPEN_PREFIX.startswith(tail_lower) else 0
            elif (
                tail_lower.startswith(_THINK_OPEN_PREFIX)
                and tail[len(_THINK_OPEN_PREFIX)].isspace()
                and len(tail) <= _THINK_OPEN_PREFIX_MAX_LEN
            ):
                keep = len(tail)
        return keep


def split_reasoning(
    text: str,
    reasoning_format: Literal["separated", "tagged"] = "tagged",
) -> tuple[str, str]:
    """Split reasoning content from text based on ``reasoning_format``.

    - ``separated``: remove ``<think>`` blocks and return clean text plus the
      extracted reasoning content.
    - ``tagged``: keep ``<think>`` tags in text and return empty reasoning.

    Returns:
        A tuple of ``(clean_text, reasoning_content)``.

    """
    if reasoning_format == "tagged":
        return text, ""

    reasoning_parts: list[str] = []
    clean_parts: list[str] = []
    rest = text
    while rest:
        opened = _THINK_OPEN_RE.search(rest)
        if opened is None:
            clean_parts.append(rest)
            break
        clean_parts.append(rest[: opened.start()])
        body = rest[opened.end() :]
        reasoning, after_close = _ThinkBodyScanner().feed(body, final=True)
        # A trailing unclosed <think> is reasoning, not text.
        part = reasoning.strip()
        if part:
            reasoning_parts.append(part)
        if after_close is None:
            break
        rest = after_close

    clean_text = re.sub(r"\n\s*\n", "\n\n", "".join(clean_parts)).strip()
    return clean_text, "\n".join(reasoning_parts)


def extract_stream_reasoning(
    *,
    full_text: str,
    reasoning_format: Literal["separated", "tagged"],
) -> tuple[str, str]:
    """Like :func:`split_reasoning` but a no-op in ``tagged`` mode."""
    if reasoning_format == "tagged":
        return full_text, ""
    return split_reasoning(full_text, reasoning_format)
