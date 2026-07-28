"""Identity of the node execution that caused a graph run to fail."""

from pydantic import BaseModel, ConfigDict


class GraphFailureSource(BaseModel):
    """Exact node execution responsible for a fatal graph failure."""

    model_config = ConfigDict(extra="forbid")

    node_execution_id: str
    node_id: str
