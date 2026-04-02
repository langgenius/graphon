from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.model_entities import ModelType
from graphon.model_runtime.entities.provider_entities import ProviderEntity
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
        label=I18nObject(en_us="Test Provider"),
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
