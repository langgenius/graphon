from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import TypeAdapter

from graphon.entities.base_node_data import BaseNodeData

from .scoping import resolve_container_id
from .validation import GraphValidationError, GraphValidationIssue

_ListObjectDict = TypeAdapter(list[dict[str, Any]])


def normalize_nodes(
    node_configs: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Copy nodes and normalize their direct container IDs once.

    Resolve canonical, React Flow, and legacy ownership fields against the
    complete node hierarchy, remove editor-only note widgets, then write the
    result to ``data.container_id`` on copied dictionaries. Filtering happens
    before Pydantic node validation because notes intentionally have no executable
    ``data.type``. The caller's persisted input is never modified. Prevalidated
    :class:`BaseNodeData` values are dumped with ``exclude_unset`` so an unset
    canonical default cannot hide an explicitly supplied legacy owner.

    Args:
        node_configs: Raw external node configurations.

    Returns:
        Copied node configurations with canonical container IDs, followed by
        the same IDs indexed by node ID. ``""`` identifies the root graph.

    Raises:
        GraphValidationError: If node IDs are invalid or duplicated.

    """
    normalized_nodes = [
        dict(node_config)
        for node_config in node_configs
        if node_config.get("type", "") != "custom-note"
    ]
    nodes_by_id: dict[str, dict[str, Any]] = {}
    for node_config in normalized_nodes:
        node_id = node_config.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise GraphValidationError([
                GraphValidationIssue(
                    code="INVALID_NODE_ID",
                    message=(f"Graph node ID must be a non-empty string: {node_id!r}"),
                )
            ])
        if node_id in nodes_by_id:
            raise GraphValidationError([
                GraphValidationIssue(
                    code="DUPLICATE_NODE_ID",
                    message=f"Duplicate graph node ID: {node_id}",
                    node_id=node_id,
                )
            ])
        nodes_by_id[node_id] = node_config
    container_ids = {
        node_id: resolve_container_id(node_config, nodes_by_id=nodes_by_id)
        for node_id, node_config in nodes_by_id.items()
    }
    for node_config in normalized_nodes:
        node_id = node_config["id"]
        data = node_config.get("data")
        if isinstance(data, BaseNodeData):
            normalized_data = data.model_dump(mode="python", exclude_unset=True)
        elif isinstance(data, Mapping):
            normalized_data = dict(data)
        else:
            continue
        normalized_data["container_id"] = container_ids[node_id]
        node_config["data"] = normalized_data
    return normalized_nodes, container_ids


def prepare_edge_configs(
    graph_config: Mapping[str, Any],
    container_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Validate edge fields and copy configs with public DSL IDs.

    Missing IDs start with the historical ``edge_N`` ordinal and advance
    past IDs supplied or generated elsewhere in the same scope.
    Supplied IDs are reserved within their owning graph before
    generation, making mixed explicit and fallback IDs independent of
    config order without preventing separate graphs from reusing a local
    ID. The generated public ID is retained in scoped graph configs, so
    child graphs use the same ID when they are materialized later. Supplied
    IDs must be non-empty strings.

    Duplicate IDs are rejected within their owning scope, allowing separate
    child graphs to reuse the same local edge ID. Each dictionary is copied
    before an ID is added, so the caller's config is never mutated.

    Args:
        graph_config: Complete or previously scoped graph config.
        container_ids: Direct container ID already resolved for every node.

    Returns:
        Copied edge dictionaries carrying public DSL edge IDs.

    Raises:
        GraphValidationError: If edge fields or scoped edge IDs are invalid.
        ValueError: If a supplied edge ID is not a non-empty string.

    """
    edge_configs = [
        dict(edge_config)
        for edge_config in _ListObjectDict.validate_python(
            graph_config.get("edges", []),
        )
    ]
    edge_container_ids: list[str | None] = []
    reserved_edge_ids: defaultdict[str | None, set[str]] = defaultdict(set)
    for edge_index, edge_config in enumerate(edge_configs):
        source = edge_config.get("source")
        target = edge_config.get("target")
        for field, value in (
            ("source", source),
            ("target", target),
            ("sourceHandle", edge_config.get("sourceHandle", "source")),
        ):
            if not isinstance(value, str):
                raise GraphValidationError([
                    GraphValidationIssue(
                        code="INVALID_EDGE",
                        message=(
                            f"Graph edge at index {edge_index}: "
                            f"{field} must be a string."
                        ),
                    )
                ])
        source_container_id = container_ids.get(source)
        target_container_id = container_ids.get(target)
        edge_container_id = (
            source_container_id
            if source_container_id is not None
            and source_container_id == target_container_id
            else None
        )
        edge_container_ids.append(edge_container_id)
        edge_id = edge_config.get("id")
        if isinstance(edge_id, str) and edge_id:
            if (
                edge_container_id is not None
                and edge_id in reserved_edge_ids[edge_container_id]
            ):
                raise GraphValidationError([
                    GraphValidationIssue(
                        code="DUPLICATE_EDGE_ID",
                        message=(
                            f"Duplicate graph edge ID: {edge_id} "
                            f"in scope {edge_container_id!r}"
                        ),
                    )
                ])
            reserved_edge_ids[edge_container_id].add(edge_id)

    for edge_index, (edge_config, edge_container_id) in enumerate(
        zip(edge_configs, edge_container_ids, strict=True),
    ):
        if "id" in edge_config and (
            not isinstance(edge_config["id"], str) or not edge_config["id"]
        ):
            msg = "Graph edge ID must be a non-empty string"
            raise ValueError(msg)
        if "id" not in edge_config:
            fallback_index = edge_index
            edge_id = f"edge_{fallback_index}"
            while edge_id in reserved_edge_ids[edge_container_id]:
                fallback_index += 1
                edge_id = f"edge_{fallback_index}"
            edge_config["id"] = edge_id
            reserved_edge_ids[edge_container_id].add(edge_id)
    return edge_configs
