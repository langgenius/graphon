from dataclasses import dataclass, field

from graphon.model_runtime.v2.domain.identity import (
    ContractRef,
    ModelRef,
    require_identifier,
)
from graphon.model_runtime.v2.domain.json_values import JsonValue, copy_json


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderState:
    model: ModelRef
    contract: ContractRef
    scope: str
    version: str
    value: JsonValue = field(repr=False)

    def __post_init__(self) -> None:
        require_identifier(self.scope)
        require_identifier(self.version)
        object.__setattr__(self, "value", copy_json(self.value))
