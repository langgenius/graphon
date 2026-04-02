from unittest.mock import MagicMock

import pytest

from graphon.model_runtime.model_providers.base.text_embedding_model import (
    TextEmbeddingModel,
)


def test_text_embedding_model_invoke_requires_texts_or_documents() -> None:
    model = TextEmbeddingModel(
        provider_schema=MagicMock(provider="provider"),
        model_runtime=MagicMock(),
    )

    with pytest.raises(ValueError, match="No texts or files provided"):
        model.invoke(model="embedding-model", credentials={})
