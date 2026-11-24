"""Read scoped node definitions from the flat layout used by existing DSL files."""

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from graphon.enums import BuiltinNodeTypes
from graphon.graph import EdgeDefinition, Graph, GraphReference, NodeDefinition
from graphon.graph.config import normalize_nodes, prepare_edge_configs
from graphon.graph.validation import GraphValidationError, GraphValidationIssue

# The canvas stores these beside the executable settings in older documents.
_NODE_METADATA = frozenset((
    "type",
    "version",
    "container_id",
    "iteration_id",
    "loop_id",
    "isInIteration",
    "isInLoop",
    "selected",
    "width",
    "height",
    "position",
    "_connectedSourceHandleIds",
    "_connectedTargetHandleIds",
    "_targetBranches",
    "_children",
    "_runningStatus",
    "_runningBranchId",
    "_singleRunningStatus",
    "_iterationIndex",
    "_iterationLength",
    "_loopIndex",
    "_loopLength",
    "_waitingRun",
    "_retryIndex",
))


def resolve_node_version(data: Mapping[str, Any]) -> str:
    # Older Variable Assigner exports select v2 by their items field.
    if data.get("type") == BuiltinNodeTypes.VARIABLE_ASSIGNER and "items" in data:
        return "2"
    return data.get("version", "1")


def _read_edges(
    config: Mapping[str, Any],
    container_ids: Mapping[str, str],
) -> tuple[dict[str, list[EdgeDefinition]], dict[str, str]]:
    edges_by_container: defaultdict[str, list[EdgeDefinition]] = defaultdict(list)
    legacy_edge_ids: dict[str, str] = {}
    for ordinal, edge in enumerate(prepare_edge_configs(config, container_ids)):
        source, target = edge["source"], edge["target"]
        if source not in container_ids or target not in container_ids:
            raise GraphValidationError([
                GraphValidationIssue(
                    "MISSING_NODE", f"Unknown endpoint in edge {edge['id']!r}."
                )
            ])
        if container_ids[source] != container_ids[target]:
            msg = f"Edge '{source}->{target}' crosses container scopes"
            raise ValueError(msg)
        if not container_ids[source]:
            legacy_edge_ids[f"edge_{ordinal}"] = edge["id"]
        edges_by_container[container_ids[source]].append(
            EdgeDefinition(
                edge["id"], source, target, edge.get("sourceHandle", "source")
            )
        )
    return dict(edges_by_container), legacy_edge_ids


def read_graph(config: Mapping[str, Any]) -> tuple[Graph, dict[str, str]]:
    """Read a definition and the root edge ordinals used by v1 response snapshots."""
    nodes, container_ids = normalize_nodes(config.get("nodes", []))
    nodes_by_container: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in nodes:
        nodes_by_container[container_ids[node["id"]]].append(node)
    edges_by_container, legacy_edge_ids = _read_edges(config, container_ids)
    visited_node_ids: set[str] = set()

    def read_container_graph(container_id: str) -> Graph:
        node_definitions: list[NodeDefinition] = []
        for node in nodes_by_container[container_id]:
            node_id = node["id"]
            visited_node_ids.add(node_id)
            data = node["data"]
            target: Graph | GraphReference | None = None
            if node_id in nodes_by_container:
                target = read_container_graph(node_id)
            elif (
                data["type"] == BuiltinNodeTypes.TOOL
                and data.get("provider_type") == "workflow"
                and data.get("provider_id")
            ):
                target = GraphReference(data["provider_id"])
            node_definitions.append(
                NodeDefinition(
                    node_id,
                    data["type"],
                    version=resolve_node_version(data),
                    config={
                        key: value
                        for key, value in data.items()
                        if key not in _NODE_METADATA
                    },
                    call_target=target,
                )
            )
        return Graph(
            nodes=node_definitions, edges=edges_by_container.get(container_id, ())
        )

    graph = read_container_graph("")
    if missing := container_ids.keys() - visited_node_ids:
        msg = f"Nodes reference unknown or cyclic containers: {sorted(missing)}"
        raise ValueError(msg)
    return graph, legacy_edge_ids
