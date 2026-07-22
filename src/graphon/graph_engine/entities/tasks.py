from __future__ import annotations

from dataclasses import dataclass

from graphon.graph_events.base import GraphNodeEventBase


@dataclass(frozen=True, slots=True)
class TaskEvent:
    frame_id: str
    event: GraphNodeEventBase
