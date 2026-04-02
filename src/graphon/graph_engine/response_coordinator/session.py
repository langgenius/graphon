"""Internal response session management for response coordinator.

This module contains the private ResponseSession class used internally
by ResponseStreamCoordinator to manage streaming sessions.
"""

from __future__ import annotations

from dataclasses import dataclass

from graphon.nodes.base.template import Template
from graphon.runtime.graph_runtime_state import NodeProtocol


@dataclass
class ResponseSession:
    """Represents an active response streaming session.

    Note: This is an internal class not exposed in the public API.
    """

    node_id: str
    template: Template  # Template object from the response node
    index: int = 0  # Current position in the template segments

    @classmethod
    def from_node(cls, node: NodeProtocol) -> ResponseSession:
        """Create a ResponseSession from a response-capable node.

        The parameter is typed as `NodeProtocol` because the graph is exposed
        behind a protocol at the runtime layer. At runtime this must be a node
        that implements `get_streaming_template()`.
        The coordinator decides which graph nodes should be treated as
        response-capable before they reach this factory.

        Args:
            node: Node from the materialized workflow graph.

        Returns:
            ResponseSession configured with the node's streaming template

        Raises:
            TypeError: If node does not implement the response-session
            streaming contract.

        """
        get_streaming_template = getattr(node, "get_streaming_template", None)
        if not callable(get_streaming_template):
            msg = (
                "ResponseSession.from_node requires "
                "get_streaming_template() on response nodes"
            )
            raise TypeError(msg)
        template = get_streaming_template()

        return cls(
            node_id=node.id,
            template=template,
        )

    def is_complete(self) -> bool:
        """Check if all segments in the template have been processed."""
        return self.index >= len(self.template.segments)
