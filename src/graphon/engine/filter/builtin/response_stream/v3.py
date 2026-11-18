from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    Field,
    NonNegativeInt,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    model_serializer,
    model_validator,
)

from graphon.engine_events.node import ResponseRead
from graphon.runtime.runtime_state import GraphProtocol

from .ids import EdgeID, NodeID
from .snapshot import ResponseSessionState, ResponseStreamFilterSnapshot, Selector


class _RootSnapshot[Version: str, Session: ResponseSessionState](
    ResponseStreamFilterSnapshot[Version, Session]
):
    """Consumed root-edge decisions plus response and stream progress.

    Taken edges and edges skipped before they were taken preserve activation.
    Optional observed skipped edges also preserve template source selection.
    Legacy residual paths cannot be reconstructed from live graph state. Retain
    those already-materialized conditions when reading an older checkpoint;
    fresh runs never enumerate or serialize complete paths.

    The unchanged flat wire fields encode either native decisions or legacy
    residual paths. Mixed modes are rejected before binding state to a graph.
    """

    node_retry_indices: dict[str, NonNegativeInt] = Field(default_factory=dict)
    # Edge IDs whose first consumed decision was TAKEN. Older inputs may also
    # include takes after a skip; rejected_edges takes precedence for overlaps.
    taken_edges: list[EdgeID] = Field(default_factory=list)
    # Edge IDs whose first consumed decision was SKIPPED. They remain closed
    # when a response treats the edge as blocking.
    rejected_edges: list[EdgeID] = Field(default_factory=list)
    # All observed skipped edge IDs for template selection, including late skips.
    # None retains the paired graph's skip lookup for older checkpoints.
    skipped_edges: list[EdgeID] | None = None
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


class SnapshotWithoutReads(_RootSnapshot[Literal["3.0"], ResponseSessionState]):
    version: Literal["3.0"] = "3.0"
    started_responses: list[str] = Field(default_factory=list)


class SessionStateWithReads(ResponseSessionState):
    reads: list[ResponseRead] = Field(default_factory=list)
    legacy_reads: bool | None = False

    @model_serializer(mode="wrap")
    def _omit_default_read_fields(
        self, serialize: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        fields = serialize(self)
        if not self.reads:
            fields.pop("reads", None)
        if self.legacy_reads is False:
            fields.pop("legacy_reads", None)
        return fields


class Snapshot(_RootSnapshot[Literal["3.1"], SessionStateWithReads]):
    version: Literal["3.1"] = "3.1"
    legacy_whole_streams: list[Selector] = Field(default_factory=list)


_SNAPSHOT_ADAPTER = TypeAdapter(
    Annotated[SnapshotWithoutReads | Snapshot, Field(discriminator="version")]
)


def loads(data: str) -> SnapshotWithoutReads | Snapshot:
    return _SNAPSHOT_ADAPTER.validate_json(data)


def dumps(snapshot: ResponseStreamFilterSnapshot) -> str:
    if not isinstance(snapshot, (SnapshotWithoutReads, Snapshot)):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    excluded_fields = {"skipped_edges"} if snapshot.skipped_edges is None else set()
    if not snapshot.rejected_edges:
        excluded_fields.add("rejected_edges")
    if not snapshot.node_retry_indices:
        excluded_fields.add("node_retry_indices")
    if isinstance(snapshot, SnapshotWithoutReads) and not snapshot.started_responses:
        excluded_fields.add("started_responses")
    if isinstance(snapshot, Snapshot) and not snapshot.legacy_whole_streams:
        excluded_fields.add("legacy_whole_streams")
    return snapshot.model_dump_json(exclude=excluded_fields)


def for_graph(
    snapshot: ResponseStreamFilterSnapshot,
    graph: GraphProtocol,
) -> SnapshotWithoutReads | Snapshot:
    del graph
    if not isinstance(snapshot, (SnapshotWithoutReads, Snapshot)):
        msg = "Expected a version 3 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return snapshot
