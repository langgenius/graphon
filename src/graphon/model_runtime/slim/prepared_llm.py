from __future__ import annotations

import json
from collections.abc import Generator, Mapping, Sequence
from typing import Any, cast

from graphon.model_runtime.entities.llm_entities import (
    LLMResult,
    LLMResultChunk,
    LLMResultChunkWithStructuredOutput,
    LLMResultWithStructuredOutput,
)
from graphon.model_runtime.entities.message_entities import (
    PromptMessage,
    PromptMessageTool,
)
from graphon.model_runtime.entities.model_entities import AIModelEntity, ModelType
from graphon.nodes.llm.runtime_protocols import PreparedLLMProtocol

from .runtime import SlimRuntime


class SlimPreparedLLM(PreparedLLMProtocol):
    def __init__(
        self,
        *,
        runtime: SlimRuntime,
        provider: str,
        model_name: str,
        credentials: Mapping[str, Any],
        parameters: Mapping[str, Any] | None = None,
        stop: Sequence[str] | None = None,
    ) -> None:
        self._runtime = runtime
        self._provider = provider
        self._model_name = model_name
        self._credentials = dict(credentials)
        self._parameters: dict[str, Any] = dict(parameters or {})
        self._stop = list(stop) if stop is not None else None

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def parameters(self) -> Mapping[str, Any]:
        return dict(self._parameters)

    @parameters.setter
    def parameters(self, value: Mapping[str, Any]) -> None:
        self._parameters = dict(value)

    @property
    def stop(self) -> Sequence[str] | None:
        return None if self._stop is None else list(self._stop)

    def get_model_schema(self) -> AIModelEntity:
        schema = self._runtime.get_model_schema(
            provider=self._provider,
            model_type=ModelType.LLM,
            model=self._model_name,
            credentials=self._credentials,
        )
        if schema is None:
            raise ValueError(
                f"Model schema not found for {self._provider}/{self._model_name}"
            )
        return schema

    def get_llm_num_tokens(self, prompt_messages: Sequence[PromptMessage]) -> int:
        return self._runtime.get_llm_num_tokens(
            provider=self._provider,
            model_type=ModelType.LLM,
            model=self._model_name,
            credentials=self._credentials,
            prompt_messages=prompt_messages,
            tools=None,
        )

    def invoke_llm(
        self,
        *,
        prompt_messages: Sequence[PromptMessage],
        model_parameters: Mapping[str, Any],
        tools: Sequence[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: bool,
    ) -> LLMResult | Generator[LLMResultChunk, None, None]:
        merged_parameters = dict(self._parameters)
        merged_parameters.update(model_parameters)
        return self._runtime.invoke_llm(
            provider=self._provider,
            model=self._model_name,
            credentials=self._credentials,
            model_parameters=merged_parameters,
            prompt_messages=prompt_messages,
            tools=list(tools) if tools is not None else None,
            stop=stop if stop is not None else self._stop,
            stream=stream,
        )

    def invoke_llm_with_structured_output(
        self,
        *,
        prompt_messages: Sequence[PromptMessage],
        json_schema: Mapping[str, Any],
        model_parameters: Mapping[str, Any],
        stop: Sequence[str] | None,
        stream: bool,
    ) -> (
        LLMResultWithStructuredOutput
        | Generator[LLMResultChunkWithStructuredOutput, None, None]
    ):
        structured_parameters = dict(model_parameters)
        structured_parameters["json_schema"] = json.dumps(json_schema)
        return cast(
            "LLMResultWithStructuredOutput | Generator[LLMResultChunkWithStructuredOutput, None, None]",
            self.invoke_llm(
                prompt_messages=prompt_messages,
                model_parameters=structured_parameters,
                tools=None,
                stop=stop,
                stream=stream,
            ),
        )

    @staticmethod
    def is_structured_output_parse_error(error: Exception) -> bool:
        _ = error
        return False
