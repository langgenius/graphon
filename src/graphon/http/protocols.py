from collections.abc import Mapping
from typing import Any, Protocol

from .response import HttpResponse


class HttpResponseProtocol(Protocol):
    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def content(self) -> bytes: ...

    @property
    def status_code(self) -> int: ...

    @property
    def text(self) -> str: ...

    @property
    def is_success(self) -> bool: ...

    def raise_for_status(self) -> None: ...


class HttpClientProtocol(Protocol):
    @property
    def max_retries_exceeded_error(self) -> type[Exception]: ...

    @property
    def request_error(self) -> type[Exception]: ...

    def get(self, url: str, max_retries: int = ..., **kwargs: Any) -> HttpResponse: ...

    def head(self, url: str, max_retries: int = ..., **kwargs: Any) -> HttpResponse: ...

    def post(self, url: str, max_retries: int = ..., **kwargs: Any) -> HttpResponse: ...

    def put(self, url: str, max_retries: int = ..., **kwargs: Any) -> HttpResponse: ...

    def delete(
        self,
        url: str,
        max_retries: int = ...,
        **kwargs: Any,
    ) -> HttpResponse: ...

    def patch(
        self,
        url: str,
        max_retries: int = ...,
        **kwargs: Any,
    ) -> HttpResponse: ...
