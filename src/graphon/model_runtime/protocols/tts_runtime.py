from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

from graphon.model_runtime.protocols.provider_runtime import ModelProviderRuntime


@runtime_checkable
class TTSModelRuntime(ModelProviderRuntime, Protocol):
    """Runtime surface required by text-to-speech model wrappers."""

    def invoke_tts(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        content_text: str,
        voice: str,
    ) -> Iterable[bytes]: ...

    def get_tts_model_voices(
        self,
        *,
        provider: str,
        model: str,
        credentials: dict[str, Any],
        language: str | None,
    ) -> Any: ...
