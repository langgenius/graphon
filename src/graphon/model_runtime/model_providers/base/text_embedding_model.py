from graphon.model_runtime.entities.model_entities import ModelPropertyKey, ModelType
from graphon.model_runtime.entities.text_embedding_entities import (
    EmbeddingInputType,
    EmbeddingResult,
)
from graphon.model_runtime.model_providers.base.ai_model import AIModel


class TextEmbeddingModel(AIModel):
    """Model class for text embedding model."""

    model_type: ModelType = ModelType.TEXT_EMBEDDING

    def invoke(
        self,
        model: str,
        credentials: dict,
        texts: list[str] | None = None,
        multimodel_documents: list[dict] | None = None,
        input_type: EmbeddingInputType = EmbeddingInputType.DOCUMENT,
    ) -> EmbeddingResult:
        """Invoke text or multimodal embedding generation for the provided inputs."""
        if not texts and not multimodel_documents:
            msg = "No texts or files provided"
            raise ValueError(msg)

        try:
            if texts:
                return self.model_runtime.invoke_text_embedding(
                    provider=self.provider,
                    model=model,
                    credentials=credentials,
                    texts=texts,
                    input_type=input_type,
                )
            if multimodel_documents:
                return self.model_runtime.invoke_multimodal_embedding(
                    provider=self.provider,
                    model=model,
                    credentials=credentials,
                    documents=multimodel_documents,
                    input_type=input_type,
                )
        except Exception as e:
            raise self._transform_invoke_error(e) from e

    def get_num_tokens(
        self,
        model: str,
        credentials: dict,
        texts: list[str],
    ) -> list[int]:
        """Count tokens for each text input sent to the embedding model."""
        return self.model_runtime.get_text_embedding_num_tokens(
            provider=self.provider,
            model=model,
            credentials=credentials,
            texts=texts,
        )

    def _get_context_size(self, model: str, credentials: dict) -> int:
        """Get the embedding model context size, falling back to the default."""
        model_schema = self.get_model_schema(model, credentials)

        if (
            model_schema
            and ModelPropertyKey.CONTEXT_SIZE in model_schema.model_properties
        ):
            content_size: int = model_schema.model_properties[
                ModelPropertyKey.CONTEXT_SIZE
            ]
            return content_size

        return 1000

    def _get_max_chunks(self, model: str, credentials: dict) -> int:
        """Get the maximum chunk count supported by the embedding model."""
        model_schema = self.get_model_schema(model, credentials)

        if (
            model_schema
            and ModelPropertyKey.MAX_CHUNKS in model_schema.model_properties
        ):
            max_chunks: int = model_schema.model_properties[ModelPropertyKey.MAX_CHUNKS]
            return max_chunks

        return 1
