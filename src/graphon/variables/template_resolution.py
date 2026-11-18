from __future__ import annotations

import re
from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import Protocol

from graphon.variables.factory import build_segment
from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import Segment

VARIABLE_PATTERN = re.compile(
    r"\{\{#([a-zA-Z0-9_-]{1,50}(?:\.[a-zA-Z_][a-zA-Z0-9_]{0,29}){1,10})#\}\}",
)


class _TemplateLookupPool(Protocol):
    @abstractmethod
    def get(self, selector: Sequence[str], /) -> Segment | None: ...


def convert_template(
    pool: _TemplateLookupPool,
    template: str,
    /,
    *,
    on_reference: Callable[[int, Sequence[str], Segment | None], None] | None = None,
) -> SegmentGroup:
    """Resolve the template and report explicit lookups at their segment positions."""
    segments: list[Segment] = []
    for index, part in enumerate(VARIABLE_PATTERN.split(template)):
        if not part:
            continue
        variable = None
        if "." in part:
            selector = part.split(".")
            variable = pool.get(selector)
            if on_reference is not None and index % 2:
                on_reference(len(segments), selector, variable)
        segments.append(variable if variable is not None else build_segment(part))
    return SegmentGroup(value=segments)
