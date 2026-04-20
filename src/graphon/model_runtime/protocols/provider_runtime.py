from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from graphon.model_runtime.entities.model_entities import AIModelEntity, ModelType
from graphon.model_runtime.entities.provider_entities import ProviderEntity


@runtime_checkable
class ModelProviderRuntime(Protocol):
    """Shared provider discovery, credential validation, and schema lookup."""

    def fetch_model_providers(self) -> Sequence[ProviderEntity]: ...

    def get_provider_icon(
        self,
        *,
        provider: str,
        icon_type: str,
        lang: str,
    ) -> tuple[bytes, str]: ...

    def validate_provider_credentials(
        self,
        *,
        provider: str,
        credentials: dict[str, Any],
    ) -> None: ...

    def validate_model_credentials(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> None: ...

    def get_model_schema(
        self,
        *,
        provider: str,
        model_type: ModelType,
        model: str,
        credentials: dict[str, Any],
    ) -> AIModelEntity | None: ...
