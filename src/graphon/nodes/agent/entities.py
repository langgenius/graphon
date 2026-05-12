from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType

_SUPPORTED_CONSTANT_VALUE_TYPES = (str, int, float, bool, dict, list, type(None))
_SUPPORTED_CONSTANT_VALUE_TYPE_NAMES = ", ".join(
    "None" if value_type is type(None) else value_type.__name__
    for value_type in _SUPPORTED_CONSTANT_VALUE_TYPES
)


class AgentParameterValue(BaseModel):
    value: list[str] | str | int | float | bool | dict[str, Any] | list[Any] | None
    type: Literal["constant", "variable", "mixed"]

    @field_validator("type", mode="before")
    @classmethod
    def check_type(
        cls,
        value: Any,
        validation_info: ValidationInfo,
    ) -> Literal["constant", "variable", "mixed"]:
        wrapper_type = value
        payload = validation_info.data.get("value")

        if payload is None:
            return wrapper_type

        match wrapper_type:
            case "mixed":
                match payload:
                    case str():
                        pass
                    case _:
                        msg = "mixed agent parameter value must be a string"
                        raise ValueError(msg)
            case "variable":
                match payload:
                    case list() if all(isinstance(part, str) for part in payload):
                        pass
                    case _:
                        msg = "variable agent parameter value must be a list of strings"
                        raise ValueError(msg)
            case "constant":
                match payload:
                    case str() | int() | float() | bool() | dict() | list() | None:
                        pass
                    case _:
                        msg = (
                            f"constant agent parameter value must be one of: "
                            f"{_SUPPORTED_CONSTANT_VALUE_TYPE_NAMES}"
                        )
                        raise ValueError(msg)
        return wrapper_type


class AgentNodeData(BaseNodeData):
    """Persisted configuration for an Agent node."""

    type: NodeType = BuiltinNodeTypes.AGENT

    agent_strategy_provider_name: str = Field(
        ...,
        description=(
            "Fully qualified provider name from the agent_strategy plugin, "
            "e.g. 'langgenius/agent/agent'."
        ),
    )
    agent_strategy_name: str = Field(
        ...,
        description="Strategy name within the provider, e.g. 'function_calling'.",
    )
    agent_strategy_label: str | None = Field(
        default=None,
        description="Human-readable strategy label emitted by Dify Studio.",
    )
    plugin_unique_identifier: str = Field(
        ...,
        description="Marketplace plugin identifier of the agent_strategy plugin.",
    )
    agent_parameters: Mapping[str, AgentParameterValue] = Field(default_factory=dict)
    output_schema: Mapping[str, Any] = Field(default_factory=dict)
    meta: Mapping[str, Any] = Field(default_factory=dict)
    tool_node_version: str = Field(
        default="2",
        description=(
            "Tool-node protocol version that the strategy plugin expects. "
            "Forwarded for plugin compatibility; not interpreted by the node."
        ),
    )
