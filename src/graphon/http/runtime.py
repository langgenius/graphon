from .client import HttpxHttpClient
from .protocols import HttpClientProtocol

_default_http_client: HttpClientProtocol = HttpxHttpClient()


def set_http_client(http_client: HttpClientProtocol) -> None:
    """Compatibility wrapper for replacing the process default client."""
    global _default_http_client  # noqa: PLW0603
    _default_http_client = http_client


def get_http_client() -> HttpClientProtocol:
    """Return the configured process default HTTP client."""
    return _default_http_client


def get_default_http_client() -> HttpClientProtocol:
    """Return the process default HTTP client."""
    return _default_http_client
