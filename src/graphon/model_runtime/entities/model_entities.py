from __future__ import annotations

from decimal import Decimal
from enum import StrEnum, auto
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, model_validator

from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.message_entities import PromptMessageContentType


class ModelType(StrEnum):
    """Enum class for model type."""

    LLM = auto()
    TEXT_EMBEDDING = "text-embedding"
    RERANK = auto()
    SPEECH2TEXT = auto()
    MODERATION = auto()
    TTS = auto()

    @classmethod
    def value_of(cls, origin_model_type: str) -> ModelType:
        """Map a provider-native model type string to a `ModelType`."""
        return _normalize_origin_model_type(origin_model_type)

    def to_origin_model_type(self) -> str:
        """Map `ModelType` back to the provider-native model type string."""
        origin_model_type = _ORIGIN_MODEL_TYPE_BY_MODEL_TYPE.get(self)
        if origin_model_type is None:
            msg = f"invalid model type {self}"
            raise ValueError(msg)
        return origin_model_type


_ORIGIN_MODEL_TYPE_BY_MODEL_TYPE: dict[ModelType, str] = {
    ModelType.LLM: "text-generation",
    ModelType.TEXT_EMBEDDING: "embeddings",
    ModelType.RERANK: "reranking",
    ModelType.SPEECH2TEXT: "speech2text",
    ModelType.MODERATION: "moderation",
    ModelType.TTS: "tts",
}


def _normalize_origin_model_type(origin_model_type: str) -> ModelType:
    match origin_model_type:
        case "text-generation":
            normalized_model_type = ModelType.LLM
        case "embeddings":
            normalized_model_type = ModelType.TEXT_EMBEDDING
        case "reranking":
            normalized_model_type = ModelType.RERANK
        case "speech2text":
            normalized_model_type = ModelType.SPEECH2TEXT
        case "moderation":
            normalized_model_type = ModelType.MODERATION
        case "tts":
            normalized_model_type = ModelType.TTS
        case _:
            try:
                normalized_model_type = ModelType(origin_model_type)
            except ValueError as error:
                msg = f"invalid origin model type {origin_model_type}"
                raise ValueError(msg) from error
    return normalized_model_type


class FetchFrom(StrEnum):
    """Enum class for fetch from."""

    PREDEFINED_MODEL = "predefined-model"
    CUSTOMIZABLE_MODEL = "customizable-model"


class ModelFeature(StrEnum):
    """Enum class for llm feature."""

    TOOL_CALL = "tool-call"
    MULTI_TOOL_CALL = "multi-tool-call"
    AGENT_THOUGHT = "agent-thought"
    VISION = auto()
    STREAM_TOOL_CALL = "stream-tool-call"
    DOCUMENT = auto()
    VIDEO = auto()
    AUDIO = auto()
    STRUCTURED_OUTPUT = "structured-output"


_REQUIRED_MODEL_FEATURE_BY_CONTENT_TYPE: dict[
    PromptMessageContentType,
    ModelFeature,
] = {
    PromptMessageContentType.IMAGE: ModelFeature.VISION,
    PromptMessageContentType.DOCUMENT: ModelFeature.DOCUMENT,
    PromptMessageContentType.VIDEO: ModelFeature.VIDEO,
    PromptMessageContentType.AUDIO: ModelFeature.AUDIO,
}


class DefaultParameterName(StrEnum):
    """Enum class for parameter template variable."""

    TEMPERATURE = auto()
    TOP_P = auto()
    TOP_K = auto()
    PRESENCE_PENALTY = auto()
    FREQUENCY_PENALTY = auto()
    MAX_TOKENS = auto()
    RESPONSE_FORMAT = auto()
    JSON_SCHEMA = auto()

    @classmethod
    def value_of(cls, value: Any) -> Self:
        """Get the enum member for a default parameter name.

        Args:
            value: Raw parameter name value.

        Returns:
            The matching default parameter name.

        """
        return cls(value)


class ParameterType(StrEnum):
    """Enum class for parameter type."""

    FLOAT = auto()
    INT = auto()
    STRING = auto()
    BOOLEAN = auto()
    TEXT = auto()


class ModelPropertyKey(StrEnum):
    """Enum class for model property key."""

    MODE = auto()
    CONTEXT_SIZE = auto()
    MAX_CHUNKS = auto()
    FILE_UPLOAD_LIMIT = auto()
    SUPPORTED_FILE_EXTENSIONS = auto()
    MAX_CHARACTERS_PER_CHUNK = auto()
    DEFAULT_VOICE = auto()
    VOICES = auto()
    WORD_LIMIT = auto()
    AUDIO_TYPE = auto()
    MAX_WORKERS = auto()


class ProviderModel(BaseModel):
    """Model class for provider model."""

    model: str
    label: I18nObject
    model_type: ModelType
    features: list[ModelFeature] | None = None
    fetch_from: FetchFrom
    model_properties: dict[ModelPropertyKey, Any]
    deprecated: bool = False
    model_config = ConfigDict(protected_namespaces=())

    @property
    def support_structure_output(self) -> bool:
        return (
            self.features is not None
            and ModelFeature.STRUCTURED_OUTPUT in self.features
        )


class ParameterRule(BaseModel):
    """Model class for parameter rule."""

    name: str
    use_template: str | None = None
    label: I18nObject
    type: ParameterType
    help: I18nObject | None = None
    required: bool = False
    default: Any | None = None
    min: float | None = None
    max: float | None = None
    precision: int | None = None
    options: list[str] = []


class PriceConfig(BaseModel):
    """Model class for pricing info."""

    input: Decimal
    output: Decimal | None = None
    unit: Decimal
    currency: str


class AIModelEntity(ProviderModel):
    """Model class for AI model."""

    parameter_rules: list[ParameterRule] = []
    pricing: PriceConfig | None = None

    def supports_prompt_content_type(
        self,
        content_type: PromptMessageContentType,
    ) -> bool:
        if not self.features:
            return content_type == PromptMessageContentType.TEXT

        required_feature = _REQUIRED_MODEL_FEATURE_BY_CONTENT_TYPE.get(content_type)
        return required_feature is None or required_feature in self.features

    @model_validator(mode="after")
    def validate_model(self):
        supported_schema_keys = ["json_schema"]
        schema_key = next(
            (
                rule.name
                for rule in self.parameter_rules
                if rule.name in supported_schema_keys
            ),
            None,
        )
        if not schema_key:
            return self
        if self.features is None:
            self.features = [ModelFeature.STRUCTURED_OUTPUT]
        elif ModelFeature.STRUCTURED_OUTPUT not in self.features:
            self.features.append(ModelFeature.STRUCTURED_OUTPUT)
        return self


class ModelUsage(BaseModel):
    pass


class PriceType(StrEnum):
    """Enum class for price type."""

    INPUT = auto()
    OUTPUT = auto()


class PriceInfo(BaseModel):
    """Model class for price info."""

    unit_price: Decimal
    unit: Decimal
    total_amount: Decimal
    currency: str
