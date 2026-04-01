from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import IO, Any

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
from graphon.model_runtime.model_providers.__base.large_language_model import (
    _increase_tool_call,
)
from graphon.model_runtime.runtime import ModelRuntime
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


class SlimRuntime(ModelRuntime):
    def __init__(self, config: SlimConfig) -> None:
        self._config = config
        self._binary_path = self._resolve_binary_path()
        self._package_loader = SlimPackageLoader(config)
        self._providers_by_name: dict[str, LoadedSlimProvider] = {}
        self._lock = Lock()

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
        self, *, provider: str, icon_type: str, lang: str
    ) -> tuple[bytes, str]:
        loaded = self._get_loaded_provider(provider)
        icon_meta = getattr(loaded.provider_entity, icon_type, None)
        if icon_meta is None:
            raise ValueError(f"Provider {provider} does not expose {icon_type}")

        candidate = getattr(icon_meta, lang, None) or icon_meta.en_US
        if not candidate:
            raise ValueError(f"Provider {provider} has no icon for language {lang}")

        icon_path = loaded.asset_root / candidate
        if not icon_path.exists():
            raise ValueError(
                f"Provider icon {candidate} not found under {loaded.asset_root}"
            )

        return icon_path.read_bytes(), icon_path.suffix or ""

    def validate_provider_credentials(
        self, *, provider: str, credentials: dict[str, Any]
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
            raise InvokeAuthorizationError(
                "Slim provider credential validation failed."
            )

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
            raise InvokeAuthorizationError("Slim model credential validation failed.")

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

        converted = self._package_loader._convert_model_entity(model_schema)
        if converted is None:
            return None
        return converted

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

        generator = self._llm_chunk_generator(
            model=model,
            prompt_messages=prompt_messages,
            event_iter=event_iter,
        )
        if stream:
            return generator
        return self._collect_llm_result(
            model=model,
            prompt_messages=prompt_messages,
            chunks=generator,
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
            raise ValueError(f"Unknown Slim provider: {provider}") from exc

    def _invoke_unary_action(
        self,
        *,
        loaded: LoadedSlimProvider,
        action: str,
        data: Mapping[str, Any],
    ) -> dict[str, Any]:
        chunks: list[Any] = []
        for event in self._run_slim(loaded=loaded, action=action, data=data):
            if isinstance(event, _SlimProgressMessage):
                logger.debug("slim[%s] %s: %s", action, event.stage, event.message)
            elif isinstance(event, _SlimChunkEvent):
                chunks.append(event.data)
            elif isinstance(event, _SlimDoneEvent):
                break
            elif isinstance(event, _SlimErrorEvent):
                raise self._map_slim_error(event)

        if not chunks:
            return {}
        if len(chunks) > 1:
            logger.debug(
                "slim[%s] returned %s chunks for unary action", action, len(chunks)
            )
        payload = chunks[-1]
        if not isinstance(payload, dict):
            raise ValueError(
                f"Expected dict payload for action {action}, got {type(payload)}"
            )
        return payload

    def _invoke_streaming_action(
        self,
        *,
        loaded: LoadedSlimProvider,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[_SlimChunkEvent | _SlimDoneEvent, None, None]:
        for event in self._run_slim(loaded=loaded, action=action, data=data):
            if isinstance(event, _SlimProgressMessage):
                logger.debug("slim[%s] %s: %s", action, event.stage, event.message)
                continue
            if isinstance(event, _SlimErrorEvent):
                raise self._map_slim_error(event)
            if isinstance(event, (_SlimChunkEvent, _SlimDoneEvent)):
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
            process = subprocess.Popen(
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

            assert process.stdin is not None
            process.stdin.write(json.dumps(request_payload))
            process.stdin.close()

            try:
                assert process.stdout is not None
                for line in process.stdout:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    event_type = payload.get("event")
                    if event_type == "message":
                        message = payload.get("data") or {}
                        yield _SlimProgressMessage(
                            stage=str(message.get("stage", "")),
                            message=str(message.get("message", "")),
                        )
                    elif event_type == "chunk":
                        yield _SlimChunkEvent(data=payload.get("data"))
                    elif event_type == "done":
                        yield _SlimDoneEvent()
                        break
                    elif event_type == "error":
                        error = payload.get("data") or {}
                        yield _SlimErrorEvent(
                            code=str(error.get("code", "PLUGIN_EXEC_ERROR")),
                            message=str(
                                error.get("message", "Slim returned an error event.")
                            ),
                        )
                        break
                    else:
                        raise ValueError(f"Unknown Slim event type: {event_type}")
            finally:
                return_code = process.wait()
                stderr_file.seek(0)
                stderr_text = stderr_file.read().strip()
                if return_code != 0:
                    if stderr_text:
                        try:
                            stderr_payload = json.loads(stderr_text.splitlines()[-1])
                        except json.JSONDecodeError:
                            raise ValueError(
                                f"Slim process exited with code {return_code}: {stderr_text}"
                            ) from None
                        raise self._map_slim_error(
                            _SlimErrorEvent(
                                code=str(
                                    stderr_payload.get("code", "PLUGIN_EXEC_ERROR")
                                ),
                                message=str(
                                    stderr_payload.get(
                                        "message",
                                        f"Slim process exited with code {return_code}",
                                    )
                                ),
                            )
                        )
                    raise ValueError(f"Slim process exited with code {return_code}")

    def _llm_chunk_generator(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        event_iter: Iterable[_SlimChunkEvent | _SlimDoneEvent],
    ) -> Generator[LLMResultChunk, None, None]:
        for event in event_iter:
            if isinstance(event, _SlimDoneEvent):
                break
            chunk = event.data
            if not isinstance(chunk, dict):
                raise ValueError(f"Unexpected LLM chunk payload: {chunk!r}")
            yield self._parse_llm_chunk(
                model=model,
                prompt_messages=prompt_messages,
                chunk=chunk,
            )

    def _collect_llm_result(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunks: Iterable[LLMResultChunk],
    ) -> LLMResult:
        content_text = ""
        content_parts: list[Any] = []
        usage = LLMUsage.empty_usage()
        finish_reason: str | None = None
        tool_calls: list[AssistantPromptMessage.ToolCall] = []
        structured_output: Mapping[str, Any] | None = None
        system_fingerprint: str | None = None

        for chunk in chunks:
            delta_message = chunk.delta.message
            if isinstance(delta_message.content, str):
                content_text += delta_message.content
            elif isinstance(delta_message.content, list):
                content_parts.extend(delta_message.content)

            if delta_message.tool_calls:
                _increase_tool_call(delta_message.tool_calls, tool_calls)
            if chunk.delta.usage is not None:
                usage = chunk.delta.usage
            if chunk.delta.finish_reason is not None:
                finish_reason = chunk.delta.finish_reason
            if isinstance(chunk, LLMResultChunkWithStructuredOutput):
                structured_output = chunk.structured_output
            if chunk.system_fingerprint is not None:
                system_fingerprint = chunk.system_fingerprint

        _ = finish_reason
        result_kwargs = {
            "model": model,
            "prompt_messages": list(prompt_messages),
            "message": AssistantPromptMessage(
                content=content_text or content_parts,
                tool_calls=tool_calls,
            ),
            "usage": usage,
            "system_fingerprint": system_fingerprint,
        }
        if structured_output is not None:
            return LLMResultWithStructuredOutput(
                **result_kwargs,
                structured_output=structured_output,
            )
        return LLMResult(**result_kwargs)

    def _parse_llm_chunk(
        self,
        *,
        model: str,
        prompt_messages: Sequence[PromptMessage],
        chunk: dict[str, Any],
    ) -> LLMResultChunk:
        delta_payload = chunk.get("delta") or {}
        usage_payload = delta_payload.get("usage")
        message = self._deserialize_prompt_message(delta_payload.get("message") or {})
        finish_reason = delta_payload.get("finish_reason")
        base_kwargs = {
            "model": model,
            "prompt_messages": list(prompt_messages),
            "system_fingerprint": chunk.get("system_fingerprint"),
            "delta": LLMResultChunkDelta(
                index=int(delta_payload.get("index", 0)),
                message=message,
                usage=(
                    self._parse_llm_usage(usage_payload)
                    if usage_payload is not None
                    else None
                ),
                finish_reason=str(finish_reason) if finish_reason is not None else None,
            ),
        }
        if "structured_output" in chunk:
            return LLMResultChunkWithStructuredOutput(
                **base_kwargs,
                structured_output=chunk.get("structured_output"),
            )
        return LLMResultChunk(**base_kwargs)

    def _serialize_prompt_message(self, message: PromptMessage) -> dict[str, Any]:
        return jsonable_encoder(message)

    def _deserialize_prompt_message(self, payload: dict[str, Any]) -> PromptMessage:
        role = str(payload.get("role", "assistant"))
        message_cls = _PROMPT_MESSAGE_ROLE_TO_CLASS.get(role, AssistantPromptMessage)
        content = payload.get("content")
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
            payload = dict(payload)
            payload["content"] = converted_content
        return message_cls.model_validate(payload)

    @staticmethod
    def _parse_llm_usage(payload: Mapping[str, Any]) -> LLMUsage:
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
