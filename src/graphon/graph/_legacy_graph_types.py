"""Resolve execution types on demand for the published graph builder API."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from graphon.engine.frame_graph import Edge, FrameGraph, GraphBuilder
    from graphon.nodes.factory import NodeFactory

__all__ = ["Edge", "FrameGraph", "GraphBuilder", "NodeFactory"]


def __getattr__(name: str) -> Any:
    if name == "NodeFactory":
        from graphon.nodes.factory import NodeFactory  # ruff: ignore[import-outside-top-level]

        return NodeFactory
    if name in __all__:
        from graphon.engine import frame_graph  # ruff: ignore[import-outside-top-level]

        return getattr(frame_graph, name)
    raise AttributeError(name)


def get_public_type(name: str) -> Any:
    if name in {"Edge", "GraphBuilder", "NodeFactory"}:
        return __getattr__(name)
    raise AttributeError(name)
