from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess  # noqa: S404
import tempfile
from collections.abc import Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import IO, Any, Literal, NoReturn, overload

from pydantic import StrictStr, TypeAdapter, ValidationError

from graphon.model_runtime.entities.llm_entities import (
    LLMResult,
    LLMResultChunk,
    LLMResultChunkDelta,
    LLMResultChunkWithStructuredOutput,
    LLMResultWithStructuredOutput,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    AudioPromptMessageContent,
    DocumentPromptMessageContent,
    ImagePromptMessageContent,
    PromptMessage,
    PromptMessageContentType,
    PromptMessageRole,
    PromptMessageTool,
    SystemPromptMessage,
    TextPromptMessageContent,
    ToolPromptMessage,
    UserPromptMessage,
    VideoPromptMessageContent,
)
from graphon.model_runtime.entities.model_entities import AIModelEntity, ModelType
from graphon.model_runtime.entities.provider_entities import ProviderEntity
from graphon.model_runtime.entities.rerank_entities import (
    MultimodalRerankInput,
    RerankDocument,
    RerankResult,
)
from graphon.model_runtime.entities.text_embedding_entities import (
    EmbeddingInputType,
    EmbeddingResult,
    EmbeddingUsage,
)
from graphon.model_runtime.errors.invoke import (
    InvokeAuthorizationError,
    InvokeBadRequestError,
    InvokeConnectionError,
    InvokeServerUnavailableError,
)
from graphon.model_runtime.model_providers.base.large_language_model import (
    merge_tool_call_deltas,
)
from graphon.model_runtime.protocols.runtime import ModelRuntime
from graphon.model_runtime.utils.encoders import jsonable_encoder

from .config import SlimConfig
from .package_loader import LoadedSlimProvider, SlimPackageLoader

logger = logging.getLogger(__name__)
_SLIM_BINARY_NAME = "dify-plugin-daemon-slim"
_SLIM_BINARY_PATH_ENV = "SLIM_BINARY_PATH"

_PROMPT_MESSAGE_ROLE_TO_CLASS = {
    "system": SystemPromptMessage,
    "user": UserPromptMessage,
    "assistant": AssistantPromptMessage,
    "tool": ToolPromptMessage,
}

_PROMPT_CONTENT_TYPE_TO_CLASS = {
    PromptMessageContentType.TEXT.value: TextPromptMessageContent,
    PromptMessageContentType.IMAGE.value: ImagePromptMessageContent,
    PromptMessageContentType.AUDIO.value: AudioPromptMessageContent,
    PromptMessageContentType.VIDEO.value: VideoPromptMessageContent,
    PromptMessageContentType.DOCUMENT.value: DocumentPromptMessageContent,
}
_OPTIONAL_STR_ADAPTER = TypeAdapter(StrictStr | None)
_STRUCTURED_OUTPUT_ADAPTER = TypeAdapter(dict[StrictStr, Any] | None)

_I18N_OBJECT_ATTR_BY_LANG = {
    "en": "en_us",
    "en-us": "en_us",
    "en_US": "en_us",
    "en_us": "en_us",
    "zh-hans": "zh_hans",
    "zh_Hans": "zh_hans",
    "zh_CN": "zh_hans",
    "zh_cn": "zh_hans",
}


@dataclass(slots=True, frozen=True)
class _SlimProgressMessage:
    stage: str
    message: str


@dataclass(slots=True, frozen=True)
class _SlimChunkEvent:
    data: Any


@dataclass(slots=True, frozen=True)
class _SlimDoneEvent:
    pass


@dataclass(slots=True, frozen=True)
class _SlimErrorEvent:
    code: str
    message: str


class SlimStructuredOutputParseError(ValueError):
    """Raised when a structured-output response cannot be validated."""


_MISSING_STRUCTURED_OUTPUT_MESSAGE = (
    "Slim structured-output response is missing structured_output data"
)


@dataclass(slots=True)
class _StructuredOutputAccumulator:
    structured_output: Mapping[str, Any] | None = None
    has_structured_output: bool = False

    def consume(self, structured_output: Mapping[str, Any] | None) -> None:
        if structured_output is None:
            return
        self.structured_output = structured_output
        self.has_structured_output = True

    def finalize(self, *, expect_structured_output: bool) -> Mapping[str, Any] | None:
        if self.has_structured_output:
            return self.structured_output
        if expect_structured_output:
            raise SlimStructuredOutputParseError(_MISSING_STRUCTURED_OUTPUT_MESSAGE)
        return None


@dataclass(slots=True)
class _CollectedLLMResult:
    content_text: str = ""
    content_parts: list[Any] = field(default_factory=list)
    usage: LLMUsage = field(default_factory=LLMUsage.empty_usage)
    tool_calls: list[AssistantPromptMessage.ToolCall] = field(default_factory=list)
    structured_output: Mapping[str, Any] | None = None
    system_fingerprint: str | None = None


class SlimRuntime(ModelRuntime):
    def __init__(self, config: SlimConfig) -> None:
        self._config = config
        self._binary_path = self._resolve_binary_path()
        self._package_loader = SlimPackageLoader(config)
        self._providers_by_name: dict[str, LoadedSlimProvider] = {}
        self._lock = Lock()

    @property
    def binary_path(self) -> str:
        """Return the resolved slim daemon binary path."""
        return self._binary_path

    def _resolve_binary_path(self) -> str:
        configured_path = os.environ.get(_SLIM_BINARY_PATH_ENV, "").strip()
        if configured_path:
            binary_path = Path(configured_path).expanduser().resolve()
            if not binary_path.is_file():
                message = (
                    f"{_SLIM_BINARY_PATH_ENV} points to a missing file: {binary_path}"
                )
                logger.error(message)
                raise RuntimeError(message)
            if not os.access(binary_path, os.X_OK):
                message = (
                    f"{_SLIM_BINARY_PATH_ENV} points to a non-executable file: "
                    f"{binary_path}"
                )
                logger.error(message)
                raise RuntimeError(message)
            return str(binary_path)

        binary_path = shutil.which(_SLIM_BINARY_NAME)
        if binary_path is None:
            message = (
                f"{_SLIM_BINARY_NAME} is not available in PATH. "
                f"Set {_SLIM_BINARY_PATH_ENV} to override it."
            )
            logger.error(message)
            raise RuntimeError(message)
        return binary_path

    def fetch_model_providers(self) -> Sequence[ProviderEntity]:
        self._ensure_loaded()
        return [
            provider.provider_entity.model_copy(deep=True)
            for provider in self._providers_by_name.values()
        ]

    def get_provider_icon(
        self,
        *,
        provider: str,
        icon_type: str,
        lang: str,
    ) -> tuple[bytes, str]:
        loaded = self._get_loaded_provider(provider)
        icon_meta = getattr(loaded.provider_entity, icon_type, None)
        if icon_meta is None:
            msg = f"Provider {provider} does not expose {icon_type}"
            raise ValueError(msg)

        lang_attr = _I18N_OBJECT_ATTR_BY_LANG.get(lang, lang)
        candidate = getattr(icon_meta, lang_attr, None) or icon_meta.en_us
        if not candidate:
            msg = f"Provider {provider} has no icon for language {lang}"
            raise ValueError(msg)

        icon_path = loaded.asset_root / candidate
        if not icon_path.exists():
            msg = f"Provider icon {candidate} not found under {loaded.asset_root}"
            raise ValueError(msg)

        return icon_path.read_bytes(), icon_path.suffix or ""

    def validate_provider_credentials(
        self,
        *,
        provider: str,
        credentials: dict[str, Any],
    ) -> None:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="validate_provider_credentials",
            data={
                "provider": loaded.provider_entity.provider,
                "credentials": credentials,
            },
        )
        if not result.get("result", False):
            msg = "Slim provider credential validation failed."
            raise InvokeAuthorizationError(msg)

    def validate_model_credentials(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> None:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="validate_model_credentials",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": self._to_slim_model_type(model_type),
                "model": model,
                "credentials": credentials,
            },
        )
        if not result.get("result", False):
            msg = "Slim model credential validation failed."
            raise InvokeAuthorizationError(msg)

    def get_model_schema(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> AIModelEntity | None:
        loaded = self._get_loaded_provider(provider)

        predefined = next(
            (
                item
                for item in loaded.provider_entity.models
                if item.model == model and item.model_type == model_type
            ),
            None,
        )
        if predefined is not None:
            return predefined.model_copy(deep=True)

        result = self._invoke_unary_action(
            loaded=loaded,
            action="get_ai_model_schemas",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": self._to_slim_model_type(model_type),
                "model": model,
                "credentials": credentials,
            },
        )
        model_schema = result.get("model_schema")
        if model_schema is None:
            return None

        converted = self._package_loader.convert_model_entity(model_schema)
        if converted is None:
            return None
        return converted

    @overload
    def invoke_llm(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[False],
    ) -> LLMResult: ...

    @overload
    def invoke_llm(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[True],
    ) -> Generator[LLMResultChunk, None, None]: ...

    def invoke_llm(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: bool,
    ) -> LLMResult | Generator[LLMResultChunk, None, None]:
        return self._invoke_llm_internal(
            provider=provider,
            model=model,
            credentials=credentials,
            model_parameters=model_parameters,
            prompt_messages=prompt_messages,
            tools=tools,
            stop=stop,
            stream=stream,
            expect_structured_output=False,
        )

    @overload
    def invoke_llm_with_structured_output(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        json_schema: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        stop: Sequence[str] | None,
        stream: Literal[False],
    ) -> LLMResultWithStructuredOutput: ...

    @overload
    def invoke_llm_with_structured_output(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        json_schema: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        stop: Sequence[str] | None,
        stream: Literal[True],
    ) -> Generator[LLMResultChunkWithStructuredOutput, None, None]: ...

    def invoke_llm_with_structured_output(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        json_schema: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        stop: Sequence[str] | None,
        stream: bool,
    ) -> (
        LLMResultWithStructuredOutput
        | Generator[LLMResultChunkWithStructuredOutput, None, None]
    ):
        structured_parameters = dict(model_parameters)
        structured_parameters["json_schema"] = json.dumps(json_schema)
        return self._invoke_llm_internal(
            provider=provider,
            model=model,
            credentials=credentials,
            model_parameters=structured_parameters,
            prompt_messages=prompt_messages,
            tools=None,
            stop=stop,
            stream=stream,
            expect_structured_output=True,
        )

    @overload
    def _invoke_llm_internal(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[False],
        expect_structured_output: Literal[False],
    ) -> LLMResult: ...

    @overload
    def _invoke_llm_internal(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[True],
        expect_structured_output: Literal[False],
    ) -> Generator[LLMResultChunk, None, None]: ...

    @overload
    def _invoke_llm_internal(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[False],
        expect_structured_output: Literal[True],
    ) -> LLMResultWithStructuredOutput: ...

    @overload
    def _invoke_llm_internal(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: Literal[True],
        expect_structured_output: Literal[True],
    ) -> Generator[LLMResultChunkWithStructuredOutput, None, None]: ...

    def _invoke_llm_internal(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        model_parameters: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: list[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: bool,
        expect_structured_output: bool,
    ) -> (
        LLMResult
        | Generator[LLMResultChunk, None, None]
        | LLMResultWithStructuredOutput
        | Generator[LLMResultChunkWithStructuredOutput, None, None]
    ):
        loaded = self._get_loaded_provider(provider)
        payload = {
            "provider": loaded.provider_entity.provider,
            "model_type": "llm",
            "model": model,
            "credentials": credentials,
            "prompt_messages": [
                self._serialize_prompt_message(item) for item in prompt_messages
            ],
            "model_parameters": dict(model_parameters),
            "stop": list(stop or []),
            "tools": [jsonable_encoder(tool) for tool in tools or []],
            "stream": bool(stream),
        }

        event_iter = self._invoke_streaming_action(
            loaded=loaded,
            action="invoke_llm",
            data=payload,
        )

        if expect_structured_output:
            generator = self._llm_chunk_generator(
                model=model,
                prompt_messages=prompt_messages,
                event_iter=event_iter,
                expect_structured_output=True,
            )
            if stream:
                return generator
            return self._collect_llm_result(
                model=model,
                prompt_messages=prompt_messages,
                chunks=generator,
                expect_structured_output=True,
            )

        generator = self._llm_chunk_generator(
            model=model,
            prompt_messages=prompt_messages,
            event_iter=event_iter,
            expect_structured_output=False,
        )
        if stream:
            return generator
        return self._collect_llm_result(
            model=model,
            prompt_messages=prompt_messages,
            chunks=generator,
            expect_structured_output=False,
        )

    def get_llm_num_tokens(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
        prompt_messages: Sequence[PromptMessage],
        tools: Sequence[PromptMessageTool] | None,
    ) -> int:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="get_llm_num_tokens",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": self._to_slim_model_type(model_type),
                "model": model,
                "credentials": credentials,
                "prompt_messages": [
                    self._serialize_prompt_message(item) for item in prompt_messages
                ],
                "tools": [jsonable_encoder(tool) for tool in tools or []],
            },
        )
        return int(result["num_tokens"])

    def invoke_text_embedding(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        texts: list[str],
        input_type: EmbeddingInputType,
    ) -> EmbeddingResult:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_text_embedding",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "text-embedding",
                "model": model,
                "credentials": credentials,
                "texts": texts,
                "input_type": input_type.value,
            },
        )
        return EmbeddingResult(
            model=str(result["model"]),
            embeddings=result["embeddings"],
            usage=self._parse_embedding_usage(result["usage"]),
        )

    def invoke_multimodal_embedding(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        documents: list[dict[str, Any]],
        input_type: EmbeddingInputType,
    ) -> EmbeddingResult:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_multimodal_embedding",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "multimodal-embedding",
                "model": model,
                "credentials": credentials,
                "documents": documents,
                "input_type": input_type.value,
            },
        )
        return EmbeddingResult(
            model=str(result["model"]),
            embeddings=result["embeddings"],
            usage=self._parse_embedding_usage(result["usage"]),
        )

    def get_text_embedding_num_tokens(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        texts: list[str],
    ) -> list[int]:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="get_text_embedding_num_tokens",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "text-embedding",
                "model": model,
                "credentials": credentials,
                "texts": texts,
            },
        )
        return [int(item) for item in result["num_tokens"]]

    def invoke_rerank(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        query: str,
        docs: list[str],
        score_threshold: float | None,
        top_n: int | None,
    ) -> RerankResult:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_rerank",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "rerank",
                "model": model,
                "credentials": credentials,
                "query": query,
                "docs": docs,
                "score_threshold": score_threshold,
                "top_n": top_n,
            },
        )
        return self._parse_rerank_result(result)

    def invoke_multimodal_rerank(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        query: MultimodalRerankInput,
        docs: list[MultimodalRerankInput],
        score_threshold: float | None,
        top_n: int | None,
    ) -> RerankResult:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_multimodal_rerank",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "multimodal-rerank",
                "model": model,
                "credentials": credentials,
                "query": query,
                "docs": docs,
                "score_threshold": score_threshold,
                "top_n": top_n,
            },
        )
        return self._parse_rerank_result(result)

    def invoke_tts(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        content_text: str,
        voice: str,
    ) -> Iterable[bytes]:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_tts",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "tts",
                "model": model,
                "credentials": credentials,
                "content_text": content_text,
                "voice": voice,
            },
        )
        return [bytes.fromhex(str(result["result"]))]

    def get_tts_model_voices(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        language: str | None,
    ) -> Any:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="get_tts_model_voices",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "tts",
                "model": model,
                "credentials": credentials,
                "language": language,
            },
        )
        return result["voices"]

    def invoke_speech_to_text(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        file: IO[bytes],
    ) -> str:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_speech2text",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "speech2text",
                "model": model,
                "credentials": credentials,
                "file": file.read().hex(),
            },
        )
        return str(result["result"])

    def invoke_moderation(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        text: str,
    ) -> bool:
        loaded = self._get_loaded_provider(provider)
        result = self._invoke_unary_action(
            loaded=loaded,
            action="invoke_moderation",
            data={
                "provider": loaded.provider_entity.provider,
                "model_type": "moderation",
                "model": model,
                "credentials": credentials,
                "text": text,
            },
        )
        return bool(result["result"])

    def _ensure_loaded(self) -> None:
        if self._providers_by_name:
            return
        with self._lock:
            if self._providers_by_name:
                return
            for binding in self._config.bindings:
                loaded = self._package_loader.load(binding)
                self._providers_by_name[loaded.provider_entity.provider] = loaded

    def _get_loaded_provider(self, provider: str) -> LoadedSlimProvider:
        self._ensure_loaded()
        try:
            return self._providers_by_name[provider]
        except KeyError as exc:
            msg = f"Unknown Slim provider: {provider}"
            raise ValueError(msg) from exc

    def _invoke_unary_action(
        self,
        *,
        loaded: LoadedSlimProvider,
        action: str,
        data: Mapping[str, Any],
    ) -> dict[str, Any]:
        chunks: list[Any] = []
        for event in self._run_slim(loaded=loaded, action=action, data=data):
            match event:
                case _SlimProgressMessage():
                    logger.debug("slim[%s] %s: %s", action, event.stage, event.message)
                case _SlimChunkEvent():
                    chunks.append(event.data)
                case _SlimDoneEvent():
                    break
                case _SlimErrorEvent():
                    raise self._map_slim_error(event)

        if not chunks:
            return {}
        if len(chunks) > 1:
            logger.debug(
                "slim[%s] returned %s chunks for unary action",
                action,
                len(chunks),
            )
        payload = chunks[-1]
        if not isinstance(payload, dict):
            msg = f"Expected dict payload for action {action}, got {type(payload)}"
            raise TypeError(msg)
        return payload

    def _invoke_streaming_action(
        self,
        *,
        loaded: LoadedSlimProvider,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[_SlimChunkEvent | _SlimDoneEvent, None, None]:
        for event in self._run_slim(loaded=loaded, action=action, data=data):
            match event:
                case _SlimProgressMessage():
                    logger.debug("slim[%s] %s: %s", action, event.stage, event.message)
                case _SlimErrorEvent():
                    raise self._map_slim_error(event)
                case _SlimChunkEvent() | _SlimDoneEvent():
                    yield event

    def _run_slim(
        self,
        *,
        loaded: LoadedSlimProvider,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[
        _SlimProgressMessage | _SlimChunkEvent | _SlimDoneEvent | _SlimErrorEvent,
        None,
        None,
    ]:
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr_file:
            process = subprocess.Popen(  # noqa: S603
                [
                    self._binary_path,
                    "-id",
                    loaded.binding.plugin_id,
                    "-action",
                    action,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                text=True,
                encoding="utf-8",
                env=self._config.build_env(),
            )

            request_payload = {"data": jsonable_encoder(dict(data))}

            if process.stdin is None:
                msg = "Slim subprocess did not expose stdin."
                raise RuntimeError(msg)
            process.stdin.write(json.dumps(request_payload))
            process.stdin.close()

            try:
                if process.stdout is None:
                    msg = "Slim subprocess did not expose stdout."
                    raise RuntimeError(msg)
                yield from self._iter_slim_stdout_events(process.stdout)
            finally:
                self._check_slim_process_exit(process=process, stderr_file=stderr_file)

    def _iter_slim_stdout_events(
        self,
        stdout: IO[str],
    ) -> Generator[
        _SlimProgressMessage | _SlimChunkEvent | _SlimDoneEvent | _SlimErrorEvent,
        None,
        None,
    ]:
        for line in stdout:
            if not line.strip():
                continue
            event = self._parse_slim_event(json.loads(line))
            yield event
            if isinstance(event, (_SlimDoneEvent, _SlimErrorEvent)):
                break

    @staticmethod
    def _parse_slim_event(
        payload: Mapping[str, Any],
    ) -> _SlimProgressMessage | _SlimChunkEvent | _SlimDoneEvent | _SlimErrorEvent:
        event_type = payload.get("event")
        match event_type:
            case "message":
                message = payload.get("data") or {}
                return _SlimProgressMessage(
                    stage=str(message.get("stage", "")),
                    message=str(message.get("message", "")),
                )
            case "chunk":
                return _SlimChunkEvent(data=payload.get("data"))
            case "done":
                return _SlimDoneEvent()
            case "error":
                error = payload.get("data") or {}
                return _SlimErrorEvent(
                    code=str(error.get("code", "PLUGIN_EXEC_ERROR")),
                    message=str(
                        error.get(
                            "message",
                            "Slim returned an error event.",
                        ),
                    ),
                )
            case _:
                msg = f"Unknown Slim event type: {event_type}"
                raise ValueError(msg)

    def _check_slim_process_exit(
        self,
        *,
        process: subprocess.Popen[str],
        stderr_file: IO[str],
    ) -> None:
        return_code = process.wait()
        stderr_file.seek(0)
        stderr_text = stderr_file.read().strip()
        if return_code == 0:
            return
        self._raise_slim_process_error(
            return_code=return_code,
            stderr_text=stderr_text,
        )

    def _raise_slim_process_error(
        self,
        *,
        return_code: int,
        stderr_text: str,
    ) -> NoReturn:
        if not stderr_text:
            msg = f"Slim process exited with code {return_code}"
            raise ValueError(msg)
        try:
            stderr_payload = json.loads(stderr_text.splitlines()[-1])
        except json.JSONDecodeError:
            msg = f"Slim process exited with code {return_code}: {stderr_text}"
            raise ValueError(msg) from None
        raise self._map_slim_error(
            _SlimErrorEvent(
                code=str(stderr_payload.get("code", "PLUGIN_EXEC_ERROR")),
                message=str(
                    stderr_payload.get(
                        "message",
                        f"Slim process exited with code {return_code}",
                    ),
                ),
            ),
        )

    @overload
    def _llm_chunk_generator(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        event_iter: Iterable[_SlimChunkEvent | _SlimDoneEvent],
        expect_structured_output: Literal[False],
    ) -> Generator[LLMResultChunk, None, None]: ...

    @overload
    def _llm_chunk_generator(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        event_iter: Iterable[_SlimChunkEvent | _SlimDoneEvent],
        expect_structured_output: Literal[True],
    ) -> Generator[LLMResultChunkWithStructuredOutput, None, None]: ...

    def _llm_chunk_generator(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        event_iter: Iterable[_SlimChunkEvent | _SlimDoneEvent],
        expect_structured_output: bool,
    ) -> Generator[LLMResultChunk, None, None]:
        structured_output_accumulator = (
            _StructuredOutputAccumulator() if expect_structured_output else None
        )
        for event in event_iter:
            if isinstance(event, _SlimDoneEvent):
                break
            chunk = event.data
            if not isinstance(chunk, dict):
                msg = f"Unexpected LLM chunk payload: {chunk!r}"
                raise TypeError(msg)
            parsed_chunk = self._parse_llm_chunk(
                model=model,
                prompt_messages=prompt_messages,
                chunk=chunk,
                expect_structured_output=expect_structured_output,
            )
            self._consume_structured_output_chunk(
                chunk=parsed_chunk,
                accumulator=structured_output_accumulator,
            )
            yield parsed_chunk
        self._finalize_structured_output(
            accumulator=structured_output_accumulator,
            expect_structured_output=expect_structured_output,
        )

    @overload
    def _collect_llm_result(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunks: Iterable[LLMResultChunk],
        expect_structured_output: Literal[False],
    ) -> LLMResult: ...

    @overload
    def _collect_llm_result(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunks: Iterable[LLMResultChunkWithStructuredOutput],
        expect_structured_output: Literal[True],
    ) -> LLMResultWithStructuredOutput: ...

    def _collect_llm_result(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunks: Iterable[LLMResultChunk],
        expect_structured_output: bool,
    ) -> LLMResult:
        collected = _CollectedLLMResult()
        structured_output_accumulator = (
            _StructuredOutputAccumulator() if expect_structured_output else None
        )

        for chunk in chunks:
            self._accumulate_llm_chunk(
                collected=collected,
                chunk=chunk,
                structured_output_accumulator=structured_output_accumulator,
            )

        collected.structured_output = self._finalize_structured_output(
            accumulator=structured_output_accumulator,
            expect_structured_output=expect_structured_output,
        )

        prompt_messages_list = list(prompt_messages)
        assistant_message = AssistantPromptMessage(
            content=collected.content_text or collected.content_parts,
            tool_calls=collected.tool_calls,
        )
        if collected.structured_output is not None:
            return LLMResultWithStructuredOutput(
                model=model,
                prompt_messages=prompt_messages_list,
                message=assistant_message,
                usage=collected.usage,
                system_fingerprint=collected.system_fingerprint,
                structured_output=collected.structured_output,
            )
        return LLMResult(
            model=model,
            prompt_messages=prompt_messages_list,
            message=assistant_message,
            usage=collected.usage,
            system_fingerprint=collected.system_fingerprint,
        )

    def _accumulate_llm_chunk(
        self,
        *,
        collected: _CollectedLLMResult,
        chunk: LLMResultChunk,
        structured_output_accumulator: _StructuredOutputAccumulator | None = None,
    ) -> None:
        delta_message = chunk.delta.message
        if isinstance(delta_message.content, str):
            collected.content_text += delta_message.content
        elif isinstance(delta_message.content, list):
            collected.content_parts.extend(delta_message.content)

        if delta_message.tool_calls:
            merge_tool_call_deltas(delta_message.tool_calls, collected.tool_calls)
        if chunk.delta.usage is not None:
            collected.usage = chunk.delta.usage
        self._consume_structured_output_chunk(
            chunk=chunk,
            accumulator=structured_output_accumulator,
        )
        if chunk.system_fingerprint is not None:
            collected.system_fingerprint = chunk.system_fingerprint

    @staticmethod
    def _consume_structured_output_chunk(
        *,
        chunk: LLMResultChunk,
        accumulator: _StructuredOutputAccumulator | None,
    ) -> None:
        if accumulator is None or not isinstance(
            chunk, LLMResultChunkWithStructuredOutput
        ):
            return
        accumulator.consume(chunk.structured_output)

    @staticmethod
    def _finalize_structured_output(
        *,
        accumulator: _StructuredOutputAccumulator | None,
        expect_structured_output: bool,
    ) -> Mapping[str, Any] | None:
        if accumulator is None:
            return None
        return accumulator.finalize(expect_structured_output=expect_structured_output)

    @overload
    def _parse_llm_chunk(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunk: dict[str, Any],
        expect_structured_output: Literal[False],
    ) -> LLMResultChunk: ...

    @overload
    def _parse_llm_chunk(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunk: dict[str, Any],
        expect_structured_output: Literal[True],
    ) -> LLMResultChunkWithStructuredOutput: ...

    def _parse_llm_chunk(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunk: dict[str, Any],
        expect_structured_output: bool,
    ) -> LLMResultChunk:
        delta_payload = chunk.get("delta") or {}
        usage_payload = delta_payload.get("usage")
        if not isinstance(delta_payload, Mapping):
            msg = f"Unexpected LLM delta payload: {delta_payload!r}"
            raise TypeError(msg)
        message = self._deserialize_assistant_prompt_message(
            delta_payload.get("message") or {},
        )
        finish_reason = delta_payload.get("finish_reason")
        delta = LLMResultChunkDelta(
            index=int(delta_payload.get("index", 0)),
            message=message,
            usage=self._parse_optional_llm_usage(usage_payload),
            finish_reason=_OPTIONAL_STR_ADAPTER.validate_python(finish_reason),
        )
        prompt_messages_list = list(prompt_messages)
        system_fingerprint = _OPTIONAL_STR_ADAPTER.validate_python(
            chunk.get("system_fingerprint"),
        )
        if expect_structured_output:
            try:
                structured_output = _STRUCTURED_OUTPUT_ADAPTER.validate_python(
                    chunk.get("structured_output"),
                )
            except ValidationError as e:
                msg = "Invalid structured_output payload"
                raise SlimStructuredOutputParseError(msg) from e
            return LLMResultChunkWithStructuredOutput(
                model=model,
                prompt_messages=prompt_messages_list,
                system_fingerprint=system_fingerprint,
                delta=delta,
                structured_output=structured_output,
            )
        return LLMResultChunk(
            model=model,
            prompt_messages=prompt_messages_list,
            system_fingerprint=system_fingerprint,
            delta=delta,
        )

    def _serialize_prompt_message(self, message: PromptMessage) -> dict[str, Any]:
        return jsonable_encoder(message)

    def _normalize_prompt_message_payload(
        self,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized_payload = dict(payload)
        content = normalized_payload.get("content")
        if isinstance(content, list):
            converted_content = []
            for item in content:
                if not isinstance(item, dict):
                    converted_content.append(item)
                    continue
                content_cls = _PROMPT_CONTENT_TYPE_TO_CLASS.get(item.get("type"))
                if content_cls is None:
                    converted_content.append(item)
                    continue
                converted_content.append(content_cls.model_validate(item))
            normalized_payload["content"] = converted_content
        return normalized_payload

    def _deserialize_prompt_message(self, payload: dict[str, Any]) -> PromptMessage:
        normalized_payload = self._normalize_prompt_message_payload(payload)
        role = str(normalized_payload.get("role", "assistant"))
        message_cls = _PROMPT_MESSAGE_ROLE_TO_CLASS.get(role, AssistantPromptMessage)
        return message_cls.model_validate(normalized_payload)

    def _deserialize_assistant_prompt_message(
        self,
        payload: Mapping[str, Any],
    ) -> AssistantPromptMessage:
        normalized_payload = self._normalize_prompt_message_payload(payload)
        normalized_payload["role"] = PromptMessageRole.ASSISTANT.value
        return AssistantPromptMessage.model_validate(normalized_payload)

    @staticmethod
    def _parse_optional_llm_usage(payload: object) -> LLMUsage | None:
        if payload is None:
            return None
        if not isinstance(payload, Mapping):
            msg = f"Unexpected LLM usage payload: {payload!r}"
            raise TypeError(msg)
        normalized_payload: dict[str, object] = {}
        for key, value in payload.items():
            if not isinstance(key, str):
                msg = f"Unexpected LLM usage payload key: {key!r}"
                raise TypeError(msg)
            normalized_payload[key] = value
        return SlimRuntime._parse_llm_usage(normalized_payload)

    @staticmethod
    def _parse_llm_usage(payload: Mapping[str, object]) -> LLMUsage:
        return LLMUsage.from_metadata(dict(payload))

    @staticmethod
    def _parse_embedding_usage(payload: Mapping[str, Any]) -> EmbeddingUsage:
        return EmbeddingUsage(
            tokens=int(payload["tokens"]),
            total_tokens=int(payload["total_tokens"]),
            unit_price=payload["unit_price"],
            price_unit=payload["price_unit"],
            total_price=payload["total_price"],
            currency=str(payload["currency"]),
            latency=float(payload["latency"]),
        )

    @staticmethod
    def _parse_rerank_result(payload: Mapping[str, Any]) -> RerankResult:
        return RerankResult(
            model=str(payload["model"]),
            docs=[
                RerankDocument(
                    index=int(item["index"]),
                    text=str(item["text"]),
                    score=float(item["score"]),
                )
                for item in payload["docs"]
            ],
        )

    @staticmethod
    def _to_slim_model_type(model_type: ModelType) -> str:
        if model_type == ModelType.TEXT_EMBEDDING:
            return "text-embedding"
        return model_type.value

    @staticmethod
    def _map_slim_error(event: _SlimErrorEvent) -> Exception:
        message = event.message
        code = event.code.upper()
        if code in {
            "NETWORK_ERROR",
            "PLUGIN_DOWNLOAD_ERROR",
            "PLUGIN_DOWNLOAD_TIMEOUT",
        }:
            return InvokeConnectionError(message)
        if code in {"DAEMON_ERROR", "PLUGIN_INIT_ERROR"}:
            return InvokeServerUnavailableError(message)
        if code in {"INVALID_INPUT", "INVALID_ARGS_JSON", "CONFIG_INVALID"}:
            return InvokeBadRequestError(message)
        if code == "PLUGIN_NOT_FOUND":
            return ValueError(message)
        if "credential" in message.lower():
            return InvokeAuthorizationError(message)
        return ValueError(message)
