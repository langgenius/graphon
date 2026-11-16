from __future__ import annotations

from typing import Literal

from pydantic import Field

from graphon.runtime.runtime_state import GraphProtocol

from .snapshot import ResponseStreamFilterSnapshot


class Snapshot(ResponseStreamFilterSnapshot[Literal["3.0"]]):
    """ResponseStreamFilter snapshot using reachability gating state."""

    version: Literal["3.0"] = "3.0"
    skipped_edges: list[str] | None = None
    taken_edges: list[str] = Field(default_factory=list)


def loads(data: str) -> Snapshot:
    """Validate JSON against the exact version 3 schema."""
    return Snapshot.model_validate_json(data)


def dumps(snapshot: ResponseStreamFilterSnapshot) -> str:
    """Serialize native version 3 state."""
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    exclude_fields: set[str] = {"paths_map"}
    if snapshot.skipped_edges is None:
        exclude_fields.add("skipped_edges")
    return snapshot.model_dump_json(exclude=exclude_fields)


def for_graph(
    snapshot: ResponseStreamFilterSnapshot,
    graph: GraphProtocol,
) -> Snapshot:
    """Confirm native version 3 state at graph-binding time."""
    del graph
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return snapshot
