import math
from collections.abc import Sequence
from dataclasses import dataclass

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType


class DocumentExtractorNodeData(BaseNodeData):
    type: NodeType = BuiltinNodeTypes.DOCUMENT_EXTRACTOR
    variable_selector: Sequence[str]


@dataclass(frozen=True)
class UnstructuredApiConfig:
    api_url: str | None = None
    api_key: str = ""
    timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        timeout_seconds = self.timeout_seconds
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            msg = "timeout_seconds must be a finite number greater than 0"
            raise ValueError(msg)
