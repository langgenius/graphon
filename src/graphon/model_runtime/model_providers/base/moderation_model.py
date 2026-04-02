import time

from graphon.model_runtime.entities.model_entities import ModelType
from graphon.model_runtime.model_providers.base.ai_model import AIModel


class ModerationModel(AIModel):
    """Model class for moderation model."""

    model_type: ModelType = ModelType.MODERATION

    def invoke(self, model: str, credentials: dict, text: str) -> bool:
        """Invoke the moderation model and return whether the text is unsafe."""
        self.started_at = time.perf_counter()

        try:
            return self.model_runtime.invoke_moderation(
                provider=self.provider,
                model=model,
                credentials=credentials,
                text=text,
            )
        except Exception as e:
            raise self._transform_invoke_error(e) from e
