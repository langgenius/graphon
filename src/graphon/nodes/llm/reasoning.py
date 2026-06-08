"""Handling of inline ``<think>...</think>`` reasoning in LLM output.

Reasoning models emit their chain-of-thought wrapped in ``<think>`` tags inside
the normal text stream. This module owns everything that strips or separates
that reasoning so the two code paths stay consistent:

- :class:`ThinkStreamFilter` strips it incrementally while the node streams
  tokens (``separated`` mode), handling tags split across chunk boundaries.
- :func:`split_reasoning` / :func:`extract_stream_reasoning` strip it in one
  pass from a fully buffered string (blocking results and the streaming
  completion event).
"""

from __future__ import annotations

import re
from typing import Literal

# Content of complete <think>...</think> blocks (optional attributes,
# case-insensitive, across newlines).
_THINK_PATTERN = re.compile(r"<think[^>]*>(.*?)</think>", re.IGNORECASE | re.DOTALL)
# Open/close matchers used by the streaming filter; mirror _THINK_PATTERN.
_THINK_OPEN_RE = re.compile(r"<think[^>]*>", re.IGNORECASE)
_THINK_CLOSE_RE = re.compile(r"</think>", re.IGNORECASE)
# A trailing, unclosed <think> with no matching </think> (e.g. a truncated
# generation). Used to keep reasoning out of the final text on both paths.
_THINK_OPEN_TRAILING_RE = re.compile(
    r"<think[^>]*>(?P<reasoning>(?:(?!</think>)[\s\S])*)\Z",
    re.IGNORECASE,
)
_THINK_OPEN_KEYWORD = "<think"
_THINK_CLOSE_TAG = "</think>"


class ThinkStreamFilter:
    """Stateful, chunk-boundary-safe stripper for ``<think>...</think>`` blocks.

    Used only in ``separated`` mode, where the LLM node must not stream
    reasoning to the ``text`` selector. Reasoning arrives inline as
    ``<think>...</think>`` and is streamed token by token, so a single tag can
    be split across chunks (``"<thi" + "nk>"``). This filter buffers only the
    minimal trailing bytes that could still grow into a tag and emits everything
    else as clean text.

    ``tagged`` mode does not use this filter at all (it streams raw tokens), so
    the class is unconditional: it always strips.
    """

    def __init__(self) -> None:
        self._inside_think = False
        self._hold = ""
        self._seen_clean = False

    def feed(self, text_part: str) -> str:
        """Return the clean text that is safe to emit for this chunk."""
        out_parts: list[str] = []
        work = self._hold + text_part
        self._hold = ""
        while work:
            if not self._inside_think:
                match = _THINK_OPEN_RE.search(work)
                if match:
                    out_parts.append(work[: match.start()])
                    self._inside_think = True
                    work = work[match.end() :]
                    continue
                keep = self._open_suffix_len(work)
                if keep:
                    out_parts.append(work[:-keep])
                    self._hold = work[-keep:]
                else:
                    out_parts.append(work)
                work = ""
            else:
                match = _THINK_CLOSE_RE.search(work)
                if match:
                    self._inside_think = False
                    work = work[match.end() :]
                    continue
                keep = self._close_suffix_len(work)
                self._hold = work[-keep:] if keep else ""
                work = ""
        return self._strip_leading("".join(out_parts))

    def finalize(self) -> str:
        """Flush whatever is safe to emit once the stream ends."""
        if self._inside_think:
            # Unclosed trailing <think>: drop the truncated reasoning, never leak.
            self._hold = ""
            return ""
        remainder = self._hold
        self._hold = ""
        return self._strip_leading(remainder)

    def _strip_leading(self, clean: str) -> str:
        # Mirror split_reasoning()'s leading .strip() for the first visible text
        # (covers reasoning-first models: "<think>...</think>\nanswer").
        if self._seen_clean or not clean:
            return clean
        stripped = clean.lstrip()
        if stripped:
            self._seen_clean = True
        return stripped

    @staticmethod
    def _open_suffix_len(work: str) -> int:
        """Length of the trailing suffix that could still become ``<think...>``."""
        lt = work.rfind("<")
        if lt == -1:
            return 0
        tail = work[lt:]
        if ">" in tail:
            return 0
        keyword = _THINK_OPEN_KEYWORD
        if len(tail) <= len(keyword):
            return len(tail) if keyword.startswith(tail.lower()) else 0
        if tail[: len(keyword)].lower() == keyword:
            return len(tail)
        return 0

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

    # Closed <think>...</think> blocks (case-insensitive).
    matches = _THINK_PATTERN.findall(text)
    reasoning_parts = [match.strip() for match in matches if match.strip()]
    clean_text = _THINK_PATTERN.sub("", text)

    # Also drop a trailing, unclosed <think> (e.g. truncated generation) so
    # reasoning never leaks into the final text; keep it as reasoning.
    trailing = _THINK_OPEN_TRAILING_RE.search(clean_text)
    if trailing:
        trailing_reasoning = trailing.group("reasoning").strip()
        if trailing_reasoning:
            reasoning_parts.append(trailing_reasoning)
        clean_text = clean_text[: trailing.start()]

    clean_text = re.sub(r"\n\s*\n", "\n\n", clean_text).strip()
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
