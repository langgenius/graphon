from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, model_validator

from graphon.runtime.runtime_state import GraphProtocol

from .ids import EdgeID, NodeID
from .snapshot import ResponseStreamFilterSnapshot, Selector


class Snapshot(ResponseStreamFilterSnapshot[Literal["3.0"]]):
    """Consumed root-edge decisions plus response and stream progress.

    Taken edges and edges skipped before they were taken preserve activation.
    Optional observed skipped edges also preserve template source selection.
    Legacy residual paths cannot be reconstructed from live graph state. Retain
    those already-materialized conditions when reading an older checkpoint;
    fresh runs never enumerate or serialize complete paths.

    The unchanged flat wire fields encode either native decisions or legacy
    residual paths. Mixed modes are rejected before binding state to a graph.
    """

    version: Literal["3.0"] = "3.0"
    # Edge IDs whose first consumed decision was TAKEN. Older inputs may also
    # include takes after a skip; rejected_edges takes precedence for overlaps.
    taken_edges: list[EdgeID] = Field(default_factory=list)
    # Edge IDs whose first consumed decision was SKIPPED. They remain closed
    # when a response treats the edge as blocking.
    rejected_edges: list[EdgeID] = Field(default_factory=list)
    # All observed skipped edge IDs for template selection, including late skips.
    # None retains the paired graph's skip lookup for older checkpoints.
    skipped_edges: list[EdgeID] | None = None
    missing_answer_selectors: dict[str, list[Selector]] = Field(default_factory=dict)
    # Response node ID -> alternative residual paths (OR). Each path contains
    # the remaining blocking edge IDs that must all be taken (AND).
    legacy_paths_map: dict[NodeID, list[list[EdgeID]]] | None = None

    @model_validator(mode="after")
    def _reject_mixed_gating_modes(self) -> Self:
        if self.legacy_paths_map is not None and (
            self.taken_edges or self.rejected_edges
        ):
            msg = "Legacy paths cannot be combined with native edge decisions"
            raise ValueError(msg)
        return self


def loads(data: str) -> Snapshot:
    return Snapshot.model_validate_json(data)


def dumps(snapshot: ResponseStreamFilterSnapshot) -> str:
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    excluded_fields = {"skipped_edges"} if snapshot.skipped_edges is None else set()
    if not snapshot.rejected_edges:
        excluded_fields.add("rejected_edges")
    if not snapshot.missing_answer_selectors:
        excluded_fields.add("missing_answer_selectors")
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
