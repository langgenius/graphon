from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from typing import Any, Protocol

from graphon.entities.graph_config import NodeConfigDict
from graphon.enums import NodeExecutionType
from graphon.nodes.base.node import Node
from graphon.runtime.runtime_state import RuntimeState


class NodeFactory(Protocol):
    """Protocol for creating Node instances from node data dictionaries.

    A host supplies the node implementations and dependencies for each frame.
    """

    @abstractmethod
    def with_runtime_state(
        self,
        runtime_state: RuntimeState,
    ) -> NodeFactory:
        """Return a factory bound to one execution frame's runtime state.

        Container execution creates a separate :class:`RuntimeState` for every
        child frame before constructing its scoped graph. Implementations must
        ensure every node created by the returned factory uses ``runtime_state``
        instead of the parent frame's state. Stateful factories should return an
        independent copy; factories that do not retain runtime state may return
        themselves.

        Args:
            runtime_state: Runtime state owned by the frame being constructed.

        Returns:
            A factory ready to construct nodes for exactly that execution frame.

        """
        ...

    @abstractmethod
    def with_graph_config(
        self,
        graph_config: Mapping[str, Any],
    ) -> NodeFactory:
        """Return a factory whose nodes receive one frame's scoped graph config.

        FrameGraph scopes are established before node construction because
        :meth:`Node.post_init` runs inside the node constructor. Implementations
        must copy any factory-owned :class:`InitParams` and replace its
        ``graph_config`` rather than mutating configuration shared with parent or
        sibling frames.

        Args:
            graph_config: Container-subtree configuration visible to the frame.

        Returns:
            A factory ready to construct nodes for exactly that graph scope.

        """
        ...

    @abstractmethod
    def validate_node(self, node_config: NodeConfigDict) -> NodeExecutionType:
        """Validate one node against the concrete schema selected by this factory.

        Validation must resolve the same implementation and version as
        :meth:`create_node`, but it must not construct a node, initialize runtime
        dependencies, invoke ``post_init()``, or mutate execution state. FrameGraph
        calls this method for every node in the visible container subtree before
        constructing any direct-frame node, which makes malformed descendants
        fail before graph execution can produce side effects.

        Args:
            node_config: Base-validated node configuration to resolve and validate.

        Returns:
            The execution type declared by the resolved node implementation.

        Raises:
            ValueError: If the node implementation is unknown or its concrete data
                schema is invalid.

        """
        ...

    @abstractmethod
    def create_node(self, node_config: NodeConfigDict) -> Node:
        """Create a Node instance from node configuration data.

        :param node_config: node configuration dictionary containing type and other data
        :return: initialized Node instance
        :raises ValueError: if node type is unknown or no
            implementation exists for the resolved version
        :raises ValidationError: if node_config does not satisfy
            NodeConfigDict/BaseNodeData validation
        """
        ...
