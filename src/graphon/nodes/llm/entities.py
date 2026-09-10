from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.entities.message_entities import ImagePromptMessageContent
from graphon.nodes.base.entities import VariableSelector
from graphon.prompt_entities import (
    ChatModelMessage,
    CompletionModelPromptTemplate,
    MemoryConfig,
)


class ModelConfig(BaseModel):
    provider: str
    name: str
    mode: LLMMode
    completion_params: dict[str, Any] = Field(default_factory=dict)


class InvocationConfig(BaseModel):
    """Per-node policy for how a model is called, as opposed to what is asked of it.

    Kept out of `ModelConfig.completion_params`, which is forwarded to the provider
    verbatim. graphon carries these settings; the host applies them.
    """

    first_token_timeout_ms: int | None = Field(default=None, gt=0)

    @property
    def first_token_timeout(self) -> float | None:
        """The first-token timeout in seconds, the unit every host hop works in."""
        if self.first_token_timeout_ms is None:
            return None
        return self.first_token_timeout_ms / 1000


class ContextConfig(BaseModel):
    enabled: bool
    variable_selector: list[str] | None = None


class VisionConfigOptions(BaseModel):
    variable_selector: Sequence[str] = Field(default_factory=lambda: ["sys", "files"])
    detail: ImagePromptMessageContent.DETAIL = ImagePromptMessageContent.DETAIL.HIGH


class VisionConfig(BaseModel):
    enabled: bool = False
    configs: VisionConfigOptions = Field(default_factory=VisionConfigOptions)

    @field_validator("configs", mode="before")
    @classmethod
    def convert_none_configs(cls, v: Any) -> Any:
        if v is None:
            return VisionConfigOptions()
        return v


class PromptConfig(BaseModel):
    jinja2_variables: Sequence[VariableSelector] = Field(default_factory=list)

    @field_validator("jinja2_variables", mode="before")
    @classmethod
    def convert_none_jinja2_variables(cls, v: Any) -> Any:
        if v is None:
            return []
        return v


class LLMNodeChatModelMessage(ChatModelMessage):
    text: str = ""
    jinja2_text: str | None = None


class LLMNodeCompletionModelPromptTemplate(CompletionModelPromptTemplate):
    jinja2_text: str | None = None


class LLMNodeData(BaseNodeData):
    type: NodeType = BuiltinNodeTypes.LLM
    model: ModelConfig
    invocation: InvocationConfig = Field(default_factory=InvocationConfig)
    prompt_template: (
        Sequence[LLMNodeChatModelMessage] | LLMNodeCompletionModelPromptTemplate
    )
    prompt_config: PromptConfig = Field(default_factory=PromptConfig)
    memory: MemoryConfig | None = None
    context: ContextConfig
    vision: VisionConfig = Field(default_factory=VisionConfig)
    structured_output: Mapping[str, Any] | None = None
    structured_output_switch_on: bool = False
    reasoning_format: Literal["separated", "tagged"] = Field(
        # Keep tagged as default for backward compatibility
        default="tagged",
        description=(
            """
            Strategy for handling model reasoning output.

            separated: Return clean text (without <think> tags) plus final
                      reasoning_content. Answer-visible LLM nodes can also emit
                      a filtered reasoning stream for live thinking panels. The
                      live stream is raw and may differ from the normalized final
                      reasoning_content field.

            tagged   : Return original text with <think> tags. Does not produce
                      separate reasoning_content or reasoning stream events.
            """
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_structured_output_switch(cls, data: Any) -> Any:
        if not isinstance(data, Mapping) or "structured_output_enabled" not in data:
            return data
        data = dict(data)
        legacy_value = data.pop("structured_output_enabled")
        data.setdefault("structured_output_switch_on", legacy_value)
        return data

    @field_validator("prompt_config", mode="before")
    @classmethod
    def convert_none_prompt_config(cls, v: Any) -> Any:
        if v is None:
            return PromptConfig()
        return v

    @property
    def structured_output_enabled(self) -> bool:
        return self.structured_output_switch_on and self.structured_output is not None
