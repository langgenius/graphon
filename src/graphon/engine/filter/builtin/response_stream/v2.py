from __future__ import annotations

from typing import Literal

from pydantic import Field

from graphon.runtime.runtime_state import GraphProtocol

from . import v3
from .snapshot import ResponseStreamFilterSnapshot


class Snapshot(ResponseStreamFilterSnapshot[Literal["2.0"]]):
    """Legacy root-scoped snapshot with explicit residual blocking paths."""

    version: Literal["2.0"] = "2.0"
    paths_map: dict[str, list[list[str]]] = Field(default_factory=dict)


def loads(data: str) -> Snapshot:
    """Validate JSON against the exact legacy version 2 schema.

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
    return snapshot.model_dump_json()


def for_graph(
    snapshot: ResponseStreamFilterSnapshot,
    graph: GraphProtocol,
) -> v3.Snapshot:
    """Preserve version 2 residual paths while upgrading the checkpoint version.

    Residual paths describe the filter's event consumption, which cannot be
    inferred from the live graph. Version 3 retains these explicit conditions for
    restored legacy runs without enumerating any new paths.

    Args:
        snapshot: State that must be an instance of this module's exact model.
        graph: Bound graph; public v2 IDs require no translation.

    Returns:
        Version 3 state retaining legacy gating and stream/session progress.

    Raises:
        TypeError: If another version model is passed to the v2 binder.
    """
    del graph
    if not isinstance(snapshot, Snapshot):
        msg = "Expected a version 2 ResponseStreamFilter snapshot"
        raise TypeError(msg)
    return v3.Snapshot(
        **snapshot.model_dump(exclude={"version", "paths_map"}),
        legacy_paths_map=snapshot.paths_map,
    )
