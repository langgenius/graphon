from __future__ import annotations

from typing import Literal

from pydantic import Field

from graphon.runtime.runtime_state import GraphProtocol

from .snapshot import ResponseStreamFilterSnapshot


class Snapshot(ResponseStreamFilterSnapshot[Literal["3.0"]]):
    """Consumed root-edge decisions plus response and stream progress.

    Taken edges and edges skipped before they were taken preserve activation.
    Optional observed skipped edges also preserve template source selection.
    Legacy residual paths cannot be reconstructed from live graph state. Retain
    those already-materialized conditions when reading an older checkpoint;
    fresh runs never enumerate or serialize complete paths.
    """

    version: Literal["3.0"] = "3.0"
    taken_edges: list[str] = Field(default_factory=list)
    rejected_edges: list[str] = Field(default_factory=list)
    skipped_edges: list[str] | None = None
    legacy_paths_map: dict[str, list[list[str]]] | None = None


def loads(data: str) -> Snapshot:
    return Snapshot.model_validate_json(data)


def dumps(snapshot: ResponseStreamFilterSnapshot) -> str:
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    excluded_fields = {"skipped_edges"} if snapshot.skipped_edges is None else set()
    if not snapshot.rejected_edges:
        excluded_fields.add("rejected_edges")
    return snapshot.model_dump_json(exclude=excluded_fields)


def for_graph(
    snapshot: ResponseStreamFilterSnapshot,
    graph: GraphProtocol,
) -> Snapshot:
    del graph
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return snapshot
