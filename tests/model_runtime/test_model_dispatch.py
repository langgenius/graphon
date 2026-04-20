from collections.abc import Generator, Sequence
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.llm_entities import (
    LLMResult,
    LLMResultChunkWithStructuredOutput,
    LLMResultWithStructuredOutput,
    LLMUsage,
)
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    PromptMessage,
    PromptMessageTool,
    UserPromptMessage,
)
from graphon.model_runtime.entities.model_entities import AIModelEntity, ModelType
from graphon.model_runtime.entities.provider_entities import ProviderEntity
from graphon.model_runtime.entities.text_embedding_entities import (
    EmbeddingInputType,
    EmbeddingResult,
    EmbeddingUsage,
)
from graphon.model_runtime.model_providers.base.large_language_model import (
    LargeLanguageModel,
)
from graphon.model_runtime.model_providers.base.moderation_model import (
    ModerationModel,
)
from graphon.model_runtime.model_providers.base.rerank_model import RerankModel
from graphon.model_runtime.model_providers.base.speech2text_model import (
    Speech2TextModel,
)
from graphon.model_runtime.model_providers.base.text_embedding_model import (
    TextEmbeddingModel,
)
from graphon.model_runtime.model_providers.base.tts_model import TTSModel
from graphon.model_runtime.model_providers.model_provider_factory import (
    ModelProviderFactory,
)
from graphon.model_runtime.protocols.llm_runtime import LLMModelRuntime


class _ProviderRuntimeStub:
    def fetch_model_providers(self) -> tuple[ProviderEntity, ...]:
        return ()

    def get_provider_icon(
        self,
        *,
        provider: str,
        icon_type: str,
        lang: str,
    ) -> tuple[bytes, str]:
        _ = provider, icon_type, lang
        return b"", ""

    def validate_provider_credentials(
        self,
        *,
        provider: str,
        credentials: dict[str, Any],
    ) -> None:
        _ = provider, credentials

    def validate_model_credentials(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> None:
        _ = provider, model_type, model, credentials

    def get_model_schema(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> AIModelEntity | None:
        _ = provider, model_type, model, credentials
        return None


class _LLMRuntimeStub(_ProviderRuntimeStub):
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
    ) -> LLMResult:
        _ = provider, credentials, model_parameters, tools, stop, stream
        return LLMResult(
            model=model,
            prompt_messages=list(prompt_messages),
            message=AssistantPromptMessage(content="ok"),
            usage=LLMUsage.empty_usage(),
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
        _ = provider, model_type, model, credentials, prompt_messages, tools
        return 7

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
        _ = provider, credentials, json_schema, model_parameters, stop, stream
        return LLMResultWithStructuredOutput(
            model=model,
            prompt_messages=list(prompt_messages),
            message=AssistantPromptMessage(content="ok"),
            usage=LLMUsage.empty_usage(),
            structured_output={"ok": True},
        )


class _EmbeddingRuntimeStub(_ProviderRuntimeStub):
    def invoke_text_embedding(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        texts: list[str],
        input_type: EmbeddingInputType,
    ) -> EmbeddingResult:
        _ = provider, model, credentials, texts, input_type
        return EmbeddingResult(
            model=model,
            embeddings=[[0.1, 0.2]],
            usage=EmbeddingUsage(
                tokens=1,
                total_tokens=1,
                unit_price=Decimal(0),
                price_unit=Decimal(0),
                total_price=Decimal(0),
                currency="USD",
                latency=0.0,
            ),
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
        _ = provider, model, credentials, documents, input_type
        return EmbeddingResult(
            model=model,
            embeddings=[[0.1, 0.2]],
            usage=EmbeddingUsage(
                tokens=1,
                total_tokens=1,
                unit_price=Decimal(0),
                price_unit=Decimal(0),
                total_price=Decimal(0),
                currency="USD",
                latency=0.0,
            ),
        )

    def get_text_embedding_num_tokens(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        texts: list[str],
    ) -> list[int]:
        _ = provider, model, credentials, texts
        return [3]


class _TTSRuntimeStub(_ProviderRuntimeStub):
    def invoke_tts(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        content_text: str,
        voice: str,
    ) -> list[bytes]:
        _ = provider, model, credentials, content_text, voice
        return [b"audio"]

    def get_tts_model_voices(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        language: str | None,
    ) -> list[str]:
        _ = provider, model, credentials, language
        return ["nova"]


@pytest.mark.parametrize(
    ("origin_model_type", "expected_model_type"),
    [
        ("text-generation", ModelType.LLM),
        (ModelType.LLM.value, ModelType.LLM),
        ("embeddings", ModelType.TEXT_EMBEDDING),
        (ModelType.TEXT_EMBEDDING.value, ModelType.TEXT_EMBEDDING),
        ("reranking", ModelType.RERANK),
        ("speech2text", ModelType.SPEECH2TEXT),
        ("moderation", ModelType.MODERATION),
        ("tts", ModelType.TTS),
    ],
)
def test_model_type_value_of_uses_model_map(
    origin_model_type: str,
    expected_model_type: ModelType,
) -> None:
    assert ModelType.value_of(origin_model_type) == expected_model_type


@pytest.mark.parametrize(
    ("model_type", "expected_origin_model_type"),
    [
        (ModelType.LLM, "text-generation"),
        (ModelType.TEXT_EMBEDDING, "embeddings"),
        (ModelType.RERANK, "reranking"),
        (ModelType.SPEECH2TEXT, "speech2text"),
        (ModelType.MODERATION, "moderation"),
        (ModelType.TTS, "tts"),
    ],
)
def test_model_type_to_origin_model_type_uses_model_map(
    model_type: ModelType,
    expected_origin_model_type: str,
) -> None:
    assert model_type.to_origin_model_type() == expected_origin_model_type


@pytest.mark.parametrize(
    ("model_type", "expected_model_class"),
    [
        (ModelType.LLM, LargeLanguageModel),
        (ModelType.TEXT_EMBEDDING, TextEmbeddingModel),
        (ModelType.RERANK, RerankModel),
        (ModelType.SPEECH2TEXT, Speech2TextModel),
        (ModelType.MODERATION, ModerationModel),
        (ModelType.TTS, TTSModel),
    ],
)
def test_model_provider_factory_uses_model_class_map(
    model_type: ModelType,
    expected_model_class: type,
) -> None:
    provider = ProviderEntity(
        provider="test-provider",
        label=I18nObject(en_US="Test Provider"),
        supported_model_types=[model_type],
        configurate_methods=[],
    )
    runtime = MagicMock()
    runtime.fetch_model_providers.return_value = [provider]
    factory = ModelProviderFactory(model_runtime=runtime)

    model = factory.get_model_type_instance("test-provider", model_type)

    assert isinstance(model, expected_model_class)
    assert model.provider_schema is provider
    assert model.model_runtime is runtime


def test_large_language_model_accepts_llm_only_runtime_surface() -> None:
    provider = ProviderEntity(
        provider="test-provider",
        label=I18nObject(en_US="Test Provider"),
        supported_model_types=[ModelType.LLM],
        configurate_methods=[],
    )
    runtime = cast("LLMModelRuntime", _LLMRuntimeStub())
    model = LargeLanguageModel(provider_schema=provider, model_runtime=runtime)

    result = cast(
        "LLMResult",
        model.invoke(
            model="fake-chat",
            credentials={},
            prompt_messages=[UserPromptMessage(content="hello")],
            stream=False,
        ),
    )

    assert result.message.content == "ok"
    assert (
        model.get_num_tokens(
            model="fake-chat",
            credentials={},
            prompt_messages=[UserPromptMessage(content="hello")],
        )
        == 7
    )


def test_text_embedding_model_accepts_embedding_only_runtime_surface() -> None:
    provider = ProviderEntity(
        provider="test-provider",
        label=I18nObject(en_US="Test Provider"),
        supported_model_types=[ModelType.TEXT_EMBEDDING],
        configurate_methods=[],
    )
    runtime = _EmbeddingRuntimeStub()
    model = TextEmbeddingModel(provider_schema=provider, model_runtime=runtime)

    assert model.get_num_tokens(
        model="embedding-model",
        credentials={},
        texts=["hello"],
    ) == [3]


def test_tts_model_accepts_tts_only_runtime_surface() -> None:
    provider = ProviderEntity(
        provider="test-provider",
        label=I18nObject(en_US="Test Provider"),
        supported_model_types=[ModelType.TTS],
        configurate_methods=[],
    )
    runtime = _TTSRuntimeStub()
    model = TTSModel(provider_schema=provider, model_runtime=runtime)

    assert list(
        model.invoke(
            model="voice-model",
            credentials={},
            content_text="hello",
            voice="nova",
        ),
    ) == [b"audio"]
    assert model.get_tts_model_voices(
        model="voice-model",
        credentials={},
    ) == ["nova"]
