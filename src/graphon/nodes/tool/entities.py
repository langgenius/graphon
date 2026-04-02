from enum import StrEnum, auto
from typing import Any, Literal

from pydantic import BaseModel, field_validator
from pydantic_core.core_schema import ValidationInfo

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType

_SUPPORTED_TOOL_CONFIGURATION_VALUE_TYPES = (str, int, float, bool)
_SUPPORTED_TOOL_INPUT_CONSTANT_VALUE_TYPES = (str, int, float, bool, dict, list)
_SUPPORTED_TOOL_CONFIGURATION_VALUE_TYPE_NAMES = ", ".join(
    value_type.__name__ for value_type in _SUPPORTED_TOOL_CONFIGURATION_VALUE_TYPES
)
_SUPPORTED_TOOL_INPUT_CONSTANT_VALUE_TYPE_NAMES = ", ".join(
    value_type.__name__ for value_type in _SUPPORTED_TOOL_INPUT_CONSTANT_VALUE_TYPES
)


class ToolProviderType(StrEnum):
    """Graph-owned enum for persisted tool provider kinds."""

    PLUGIN = auto()
    BUILT_IN = "builtin"
    WORKFLOW = auto()
    API = auto()
    APP = auto()
    DATASET_RETRIEVAL = "dataset-retrieval"
    MCP = auto()


class ToolEntity(BaseModel):
    provider_id: str
    provider_type: ToolProviderType
    provider_name: str  # redundancy
    tool_name: str
    tool_label: str  # redundancy
    tool_configurations: dict[str, Any]
    credential_id: str | None = None
    plugin_unique_identifier: str | None = None  # redundancy

    @field_validator("tool_configurations", mode="before")
    @classmethod
    def validate_tool_configurations(
        cls,
        value: Any,
        values: ValidationInfo,
    ) -> dict[str, Any]:
        _ = values
        match value:
            case dict():
                configurations = value
            case _:
                msg = "tool_configurations must be a dictionary"
                raise ValueError(msg)

        for key, config_value in configurations.items():
            match config_value:
                case str() | int() | float() | bool():
                    pass
                case _:
                    msg = (
                        f"{key} must be one of: "
                        f"{_SUPPORTED_TOOL_CONFIGURATION_VALUE_TYPE_NAMES}"
                    )
                    raise ValueError(msg)

        return configurations


class ToolNodeData(BaseNodeData, ToolEntity):
    type: NodeType = BuiltinNodeTypes.TOOL

    class ToolInput(BaseModel):
        """Persisted tool input value and its binding mode."""

        # TODO: check this type
        value: Any | list[str]
        type: Literal["mixed", "variable", "constant"]

        @field_validator("type", mode="before")
        @classmethod
        def check_type(
            cls,
            value: Any,
            validation_info: ValidationInfo,
        ) -> Literal["mixed", "variable", "constant"]:
            typ = value
            value = validation_info.data.get("value")

            if value is None:
                return typ

            match typ:
                case "mixed":
                    match value:
                        case str():
                            pass
                        case _:
                            msg = "value must be a string"
                            raise ValueError(msg)
                case "variable":
                    match value:
                        case list() if all(isinstance(val, str) for val in value):
                            pass
                        case list():
                            msg = "value must be a list of strings"
                            raise ValueError(msg)
                        case _:
                            msg = "value must be a list"
                            raise ValueError(msg)
                case "constant":
                    match value:
                        case str() | int() | float() | bool() | dict() | list():
                            pass
                        case _:
                            msg = (
                                f"value must be one of: "
                                f"{_SUPPORTED_TOOL_INPUT_CONSTANT_VALUE_TYPE_NAMES}"
                            )
                            raise ValueError(msg)
            return typ

    tool_parameters: dict[str, ToolInput]
    # The version of the tool parameter.
    # If this value is None, it indicates this is a previous version
    # and requires using the legacy parameter parsing rules.
    tool_node_version: str | None = None

    @field_validator("tool_parameters", mode="before")
    @classmethod
    def filter_none_tool_inputs(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        return {
            key: tool_input
            for key, tool_input in value.items()
            if tool_input is not None and cls._has_valid_value(tool_input)
        }

    @staticmethod
    def _has_valid_value(tool_input: Any) -> bool:
        """Check if the value is valid"""
        match tool_input:
            case dict():
                result = tool_input.get("value") is not None
            case _:
                result = getattr(tool_input, "value", None) is not None
        return result
