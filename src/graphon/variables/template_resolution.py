from __future__ import annotations

import re

from graphon.runtime.graph_runtime_state_protocol import ReadOnlyVariablePool
from graphon.variables.factory import build_segment
from graphon.variables.segment_group import SegmentGroup
from graphon.variables.segments import Segment

VARIABLE_PATTERN = re.compile(
    r"\{\{#([a-zA-Z0-9_]{1,50}(?:\.[a-zA-Z_][a-zA-Z0-9_]{0,29}){1,10})#\}\}",
)


def convert_template(
    pool: ReadOnlyVariablePool,
    template: str,
    /,
) -> SegmentGroup:
    segments: list[Segment] = []
    for part in filter(None, VARIABLE_PATTERN.split(template)):
        if "." in part:
            variable = pool.get(part.split("."))
            if variable is not None:
                segments.append(variable)
                continue
        segments.append(build_segment(part))
    return SegmentGroup(value=segments)
