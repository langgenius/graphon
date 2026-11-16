from __future__ import annotations

from typing import Literal

from pydantic import Field

from graphon.runtime.runtime_state import GraphProtocol

from .snapshot import ResponseStreamFilterSnapshot


class Snapshot(ResponseStreamFilterSnapshot[Literal["2.0"]]):
    """ResponseStreamFilter snapshot written by Graphon 0.8."""

    version: Literal["2.0"] = "2.0"
    # Missing metadata retains the historical paired-graph skip lookup.
    skipped_edges: list[str] | None = None
    taken_edges: list[str] = Field(default_factory=list)


def loads(data: str) -> Snapshot:
    """Validate JSON against the exact current version 2 schema.

    Args:
        data: Serialized ResponseStreamFilter state whose version must be ``2.0``.

    Returns:
        Validated native version 2 state.
    """
    return Snapshot.model_validate_json(data)


def dumps(snapshot: ResponseStreamFilterSnapshot) -> str:
    """Serialize native version 2 state without compatibility metadata.

    Args:
        snapshot: State that must be an instance of this module's exact model.

    Returns:
        JSON with the unchanged ``2.0`` version tag and public edge IDs.

    Raises:
        TypeError: If another version model is passed to the v2 writer.
    """
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 2 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    exclude_fields: set[str] = set()
    if snapshot.skipped_edges is None:
        exclude_fields.add("skipped_edges")
    if not snapshot.taken_edges:
        exclude_fields.add("taken_edges")
    return snapshot.model_dump_json(exclude=exclude_fields)


def for_graph(
    snapshot: ResponseStreamFilterSnapshot,
    graph: GraphProtocol,
) -> Snapshot:
    """Confirm native version 2 state at graph-binding time.

    Version 2 already stores graph-scoped public IDs, so binding requires no graph
    rewrite. This function exists as the version module's uniform dispatch target.

    Args:
        snapshot: State that must be an instance of this module's exact model.
        graph: Bound graph; unused because v2 needs no migration.

    Returns:
        The same validated version 2 snapshot.

    Raises:
        TypeError: If another version model is passed to the v2 binder.
    """
    del graph
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 2 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return snapshot
