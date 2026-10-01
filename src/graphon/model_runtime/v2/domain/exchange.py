from dataclasses import dataclass, field

from graphon.model_runtime.v2.domain.identity import (
    ContractRef,
    ModelRef,
    require_identifier,
)
from graphon.model_runtime.v2.domain.json_values import (
    JsonObject,
    JsonValue,
    copy_json,
    copy_json_object,
    copy_usage,
)
from graphon.model_runtime.v2.domain.provider_state import ProviderState


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelRequest:
    model: ModelRef
    contract: ContractRef
    input: JsonValue
    parameters: JsonObject = field(default_factory=dict)
    output_schema: JsonObject | None = None
    continuation: ProviderState | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "input", copy_json(self.input))
        object.__setattr__(self, "parameters", copy_json_object(self.parameters))
        if self.output_schema is not None:
            object.__setattr__(
                self, "output_schema", copy_json_object(self.output_schema)
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class ModelResult:
    request_id: str
    model: ModelRef
    contract: ContractRef
    output: JsonValue
    usage: JsonValue = 0
    continuation: ProviderState | None = None
    metadata: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_identifier(self.request_id)
        object.__setattr__(self, "output", copy_json(self.output))
        object.__setattr__(self, "usage", copy_usage(self.usage))
        object.__setattr__(self, "metadata", copy_json_object(self.metadata))
