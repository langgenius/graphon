from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from graphon.model_runtime.protocols.provider_runtime import ModelProviderRuntime


@runtime_checkable
class ModerationModelRuntime(ModelProviderRuntime, Protocol):
    """Runtime surface required by moderation model wrappers."""

    def invoke_moderation(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        text: str,
    ) -> bool: ...
