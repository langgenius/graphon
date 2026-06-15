"""Human Input node entities."""

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType


class HumanInputNodeData(BaseNodeData):
    """Human Input node data."""

    type: NodeType = BuiltinNodeTypes.HUMAN_INPUT
