from .client import HttpxHttpClient
from .protocols import HttpClientProtocol

_http_client: HttpClientProtocol = HttpxHttpClient()


def set_http_client(http_client: HttpClientProtocol) -> None:
    global _http_client
    _http_client = http_client


def get_http_client() -> HttpClientProtocol:
    return _http_client


def get_default_http_client() -> HttpClientProtocol:
    return get_http_client()
