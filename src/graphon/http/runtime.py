from __future__ import annotations

from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar

from .client import HttpxHttpClient
from .protocols import HttpClientProtocol


class HttpClientRuntimeSlot:
    """Store the process default HTTP client plus scoped overrides."""

    def __init__(self, default_http_client: HttpClientProtocol | None = None) -> None:
        self._default_http_client = default_http_client or HttpxHttpClient()
        self._current_http_client: ContextVar[HttpClientProtocol | None] = ContextVar(
            "graphon_http_client",
            default=None,
        )

    def get(self) -> HttpClientProtocol:
        http_client = self._current_http_client.get()
        if http_client is not None:
            return http_client
        return self._default_http_client

    def get_default(self) -> HttpClientProtocol:
        return self._default_http_client

    def set_default(self, http_client: HttpClientProtocol) -> None:
        self._default_http_client = http_client

    @contextmanager
    def use(self, http_client: HttpClientProtocol) -> Generator[HttpClientProtocol]:
        token = self._current_http_client.set(http_client)
        try:
            yield http_client
        finally:
            self._current_http_client.reset(token)


http_client_runtime = HttpClientRuntimeSlot()


def use_http_client(
    http_client: HttpClientProtocol,
) -> AbstractContextManager[HttpClientProtocol]:
    """Bind an HTTP client for the current context."""
    return http_client_runtime.use(http_client)


def set_http_client(http_client: HttpClientProtocol) -> None:
    """Compatibility wrapper for replacing the process default client."""
    http_client_runtime.set_default(http_client)


def get_http_client() -> HttpClientProtocol:
    """Return the HTTP client visible in the current runtime context."""
    return http_client_runtime.get()


def get_default_http_client() -> HttpClientProtocol:
    """Return the process default HTTP client."""
    return http_client_runtime.get_default()
