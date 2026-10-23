"""Handling of inline ``<think>...</think>`` reasoning in LLM output.

Reasoning models emit their chain-of-thought wrapped in ``<think>`` tags inside
the normal text stream. This module owns everything that strips or separates
that reasoning so the two code paths stay consistent:

- :class:`ThinkStreamFilter` strips it incrementally while the node streams
  tokens (``separated`` mode), handling tags split across chunk boundaries.
- :func:`split_reasoning` / :func:`extract_stream_reasoning` strip it in one
  pass from a fully buffered string (blocking results and the streaming
  completion event).

A ``</think>`` inside a same-line backtick span stays in the reasoning text.
Streaming and blocking use that rule, so a literal tag does not end the block.
"""

from __future__ import annotations

import enum
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


class _BoundaryKind(enum.Enum):
    """How much of a think body can be resolved from the current buffer."""

    CLOSE = enum.auto()
    HOLD = enum.auto()
    UNCLOSED = enum.auto()


class _BacktickStep(enum.Enum):
    """What to do with a backtick run while scanning a think body."""

    SKIP = enum.auto()
    LITERAL = enum.auto()
    HOLD = enum.auto()


def _is_think_close_at(text: str, index: int) -> bool:
    end = index + _THINK_CLOSE_LEN
    return end <= len(text) and text[index:end].lower() == _THINK_CLOSE_TAG


def _backtick_run_length(text: str, index: int) -> int:
    length = 0
    while index + length < len(text) and text[index + length] == "`":
        length += 1
    return length


def _same_line_backtick_closer(
    text: str, start: int, run: int, limit: int
) -> int | None:
    """Index of the next backtick run of length ``run`` before ``limit``."""
    index = start
    while index < limit:
        if text[index] != "`":
            index += 1
            continue
        length = _backtick_run_length(text, index)
        if length == run and index + length <= limit:
            return index
        index += length
    return None


def _advance_backtick_run(
    text: str,
    opener: int,
    *,
    final: bool,
) -> tuple[_BacktickStep, int]:
    """Move past one backtick run, or hold when the span is still incomplete."""
    # A span is the opener and the next equal run before the newline. A run that
    # touches the end of an incomplete chunk is held, because the next chunk may
    # extend it. </think> between the runs is literal reasoning text.
    run = _backtick_run_length(text, opener)
    run_end = opener + run
    if not final and run_end == len(text):
        return _BacktickStep.HOLD, opener

    newline = text.find("\n", run_end)
    line_complete = final or newline != -1
    search_end = newline if newline != -1 else len(text)
    closer = _same_line_backtick_closer(text, run_end, run, search_end)
    if closer is None:
        if not line_complete:
            return _BacktickStep.HOLD, opener
        return _BacktickStep.LITERAL, run_end

    span_end = closer + run
    if not final and span_end == len(text):
        return _BacktickStep.HOLD, opener
    return _BacktickStep.SKIP, span_end


def _think_body_boundary(text: str, *, final: bool) -> tuple[_BoundaryKind, int]:
    """Locate the real think-block end, skipping closes inside backtick spans."""
    index = 0
    while index < len(text):
        if text[index] == "`":
            step, index = _advance_backtick_run(text, index, final=final)
            if step == _BacktickStep.HOLD:
                return _BoundaryKind.HOLD, index
            continue
        if _is_think_close_at(text, index):
            return _BoundaryKind.CLOSE, index
        index += 1
    return _BoundaryKind.UNCLOSED, len(text)


class ThinkStreamFilter:
    """Stateful, chunk-boundary-safe stripper for ``<think>...</think>`` blocks.

    Used only in ``separated`` mode, where the LLM node must not stream
    reasoning to the ``text`` selector. Reasoning arrives inline as
    ``<think>...</think>`` and is streamed token by token, so a single tag can
    be split across chunks (``"<thi" + "nk>"``). This filter buffers only the
    trailing bytes that could still grow into a tag, or into a backtick span
    that might contain one, and emits everything else as clean text.

    ``tagged`` mode does not use this filter at all (it streams raw tokens), so
    the class is unconditional: it always strips.

    Matches :func:`split_reasoning` only up to whitespace: that also collapses
    blank-line runs and strips; this only lstrips the leading text.
    """

    def __init__(self) -> None:
        self._inside_think = False
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
        while work:
            if not self._inside_think:
                match = _THINK_OPEN_RE.search(work)
                if match:
                    self._append_text_piece(pieces, work[: match.start()])
                    self._inside_think = True
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
            work = self._consume_think_body(work, pieces, final=final)
        return pieces

    def _consume_think_body(
        self,
        work: str,
        pieces: list[FilterPiece],
        *,
        final: bool,
    ) -> str:
        """Consume reasoning. Return text that follows a real closing tag."""
        kind, index = _think_body_boundary(work, final=final)
        if kind == _BoundaryKind.CLOSE:
            self._append_reasoning_piece(pieces, work[:index])
            self._inside_think = False
            return work[index + _THINK_CLOSE_LEN :]
        if kind == _BoundaryKind.HOLD:
            self._append_reasoning_piece(pieces, work[:index])
            self._hold = work[index:]
            return ""
        # Unclosed block. A partial "</thi" is held until the next chunk, and
        # becomes truncated reasoning when the stream ends.
        keep = 0 if final else self._close_suffix_len(work)
        self._append_reasoning_piece(pieces, work[: len(work) - keep])
        self._hold = work[len(work) - keep :] if keep else ""
        return ""

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

    @staticmethod
    def _close_suffix_len(work: str) -> int:
        """Length of the trailing suffix that could still become ``</think>``."""
        max_k = min(len(work), len(_THINK_CLOSE_TAG) - 1)
        for k in range(max_k, 0, -1):
            if _THINK_CLOSE_TAG.startswith(work[-k:].lower()):
                return k
        return 0


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
    cursor = 0
    while cursor < len(text):
        opened = _THINK_OPEN_RE.search(text, cursor)
        if opened is None:
            clean_parts.append(text[cursor:])
            break
        clean_parts.append(text[cursor : opened.start()])
        body = text[opened.end() :]
        kind, index = _think_body_boundary(body, final=True)
        if kind != _BoundaryKind.CLOSE:
            # Trailing unclosed <think> is reasoning, not text.
            trailing = body.strip()
            if trailing:
                reasoning_parts.append(trailing)
            break
        part = body[:index].strip()
        if part:
            reasoning_parts.append(part)
        cursor = opened.end() + index + _THINK_CLOSE_LEN

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
