from dataclasses import dataclass
from typing import Literal

from graphon.model_runtime.v2.domain.formats import DataFormat
from graphon.model_runtime.v2.domain.identity import ContractRef, ModelRef


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelContract:
    ref: ContractRef
    request: DataFormat
    output: DataFormat
    delivery: frozenset[Literal["complete", "stream", "job"]]
    stream: DataFormat | None = None
    accepts_output_schema: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "delivery", frozenset(self.delivery))
        if not self.delivery or not self.delivery <= {"complete", "stream", "job"}:
            message = "Delivery must include complete, stream, or job only"
            raise ValueError(message)
        if "stream" in self.delivery and self.stream is None:
            message = "Stream delivery requires a stream format"
            raise ValueError(message)


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelDescriptor:
    ref: ModelRef
    contracts: tuple[ModelContract, ...]
    operation_tags: tuple[str, ...] = ()
    label: str | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "contracts", tuple(self.contracts))
        object.__setattr__(self, "operation_tags", tuple(self.operation_tags))
        if not self.contracts or len({
            contract.ref.id for contract in self.contracts
        }) != len(self.contracts):
            message = "A model must declare contracts with distinct IDs"
            raise ValueError(message)
