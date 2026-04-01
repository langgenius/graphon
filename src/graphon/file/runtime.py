from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING, Literal, NoReturn, override

from .protocols import HttpResponseProtocol, WorkflowFileRuntimeProtocol

if TYPE_CHECKING:
    from .models import File


class WorkflowFileRuntimeNotConfiguredError(RuntimeError):
    """Raised when workflow file runtime dependencies were not configured."""


class _UnconfiguredWorkflowFileRuntime(WorkflowFileRuntimeProtocol):
    def _raise(self) -> NoReturn:
        raise WorkflowFileRuntimeNotConfiguredError(
            "workflow file runtime is not configured, call "
            "set_workflow_file_runtime(...) first"
        )

    @property
    @override
    def multimodal_send_format(self) -> str:
        self._raise()

    @override
    def http_get(
        self, url: str, *, follow_redirects: bool = True
    ) -> HttpResponseProtocol:
        self._raise()

    @override
    def storage_load(self, path: str, *, stream: bool = False) -> bytes | Generator:
        self._raise()

    @override
    def load_file_bytes(self, *, file: File) -> bytes:
        self._raise()

    @override
    def resolve_file_url(self, *, file: File, for_external: bool = True) -> str | None:
        self._raise()

    @override
    def resolve_upload_file_url(
        self,
        *,
        upload_file_id: str,
        as_attachment: bool = False,
        for_external: bool = True,
    ) -> str:
        self._raise()

    @override
    def resolve_tool_file_url(
        self, *, tool_file_id: str, extension: str, for_external: bool = True
    ) -> str:
        self._raise()

    @override
    def verify_preview_signature(
        self,
        *,
        preview_kind: Literal["image", "file"],
        file_id: str,
        timestamp: str,
        nonce: str,
        sign: str,
    ) -> bool:
        self._raise()


_runtime: WorkflowFileRuntimeProtocol = _UnconfiguredWorkflowFileRuntime()


def set_workflow_file_runtime(runtime: WorkflowFileRuntimeProtocol) -> None:
    global _runtime
    _runtime = runtime


def get_workflow_file_runtime() -> WorkflowFileRuntimeProtocol:
    return _runtime
