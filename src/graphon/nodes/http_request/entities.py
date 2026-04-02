import mimetypes
from collections.abc import Sequence
from dataclasses import dataclass
from email.message import Message
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from graphon.entities.base_node_data import BaseNodeData
from graphon.enums import BuiltinNodeTypes, NodeType
from graphon.http import HttpResponse

HTTP_REQUEST_CONFIG_FILTER_KEY = "http_request_config"
_BINARY_CONTENT_MAIN_TYPES = frozenset((
    "application",
    "image",
    "audio",
    "video",
))


class HttpRequestNodeAuthorizationConfig(BaseModel):
    type: Literal["basic", "bearer", "custom"]
    api_key: str
    header: str = ""


class HttpRequestNodeAuthorization(BaseModel):
    type: Literal["no-auth", "api-key"]
    config: HttpRequestNodeAuthorizationConfig | None = None

    @field_validator("config", mode="before")
    @classmethod
    def check_config(
        cls,
        v: Any,
        values: ValidationInfo,
    ) -> Any:
        """Validate auth config for `no-auth` and API-key modes."""
        if values.data["type"] == "no-auth":
            return None
        if not v or not isinstance(v, dict):
            msg = "config should be a dict"
            raise ValueError(msg)

        return v


class BodyData(BaseModel):
    key: str = ""
    type: Literal["file", "text"]
    value: str = ""
    file: Sequence[str] = Field(default_factory=list)


class HttpRequestNodeBody(BaseModel):
    type: Literal[
        "none",
        "form-data",
        "x-www-form-urlencoded",
        "raw-text",
        "json",
        "binary",
    ]
    data: Sequence[BodyData] = Field(default_factory=list)

    @field_validator("data", mode="before")
    @classmethod
    def check_data(cls, v: Any) -> Any:
        """For compatibility, if body is not set, return empty list."""
        if not v:
            return []
        if isinstance(v, str):
            return [BodyData(key="", type="text", value=v)]
        return v


class HttpRequestNodeTimeout(BaseModel):
    connect: int | None = None
    read: int | None = None
    write: int | None = None


@dataclass(frozen=True, slots=True)
class HttpRequestNodeConfig:
    max_connect_timeout: int
    max_read_timeout: int
    max_write_timeout: int
    max_binary_size: int
    max_text_size: int
    ssl_verify: bool
    ssrf_default_max_retries: int

    def default_timeout(self) -> "HttpRequestNodeTimeout":
        return HttpRequestNodeTimeout(
            connect=self.max_connect_timeout,
            read=self.max_read_timeout,
            write=self.max_write_timeout,
        )


class HttpRequestNodeData(BaseNodeData):
    """Code Node Data."""

    type: NodeType = BuiltinNodeTypes.HTTP_REQUEST
    method: Literal[
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "head",
        "options",
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
    ]
    url: str
    authorization: HttpRequestNodeAuthorization
    headers: str
    params: str
    body: HttpRequestNodeBody | None = None
    timeout: HttpRequestNodeTimeout | None = None
    ssl_verify: bool | None = None


class Response:
    headers: dict[str, str]
    response: HttpResponse

    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.headers = dict(response.headers)

    @property
    def is_file(self):
        """Determine if the response contains a file by checking:
        1. Content-Disposition header (RFC 6266)
        2. Content characteristics
        3. MIME type analysis
        """
        content_type = self.content_type.split(";")[0].strip().lower()
        parsed_content_disposition = self.parsed_content_disposition
        is_file = False

        # Check if it's explicitly marked as an attachment
        if parsed_content_disposition:
            disp_type = (
                parsed_content_disposition.get_content_disposition()
            )  # Returns 'attachment', 'inline', or None
            filename = (
                parsed_content_disposition.get_filename()
            )  # Returns filename if present, None otherwise
            if disp_type == "attachment" or filename is not None:
                is_file = True

        # For 'text/' types, only 'csv' should be downloaded as file
        if is_file:
            return is_file
        if content_type.startswith("text/") and "csv" not in content_type:
            return is_file

        # For application types, try to detect if it's a text-based format
        if content_type.startswith("application/"):
            # Common text-based application types
            if any(
                text_type in content_type
                for text_type in (
                    "json",
                    "xml",
                    "javascript",
                    "x-www-form-urlencoded",
                    "yaml",
                    "graphql",
                )
            ):
                return is_file

            # Try to detect if content is text-based by sampling first few bytes
            try:
                # Sample first 1024 bytes for text detection
                content_sample = self.response.content[:1024]
                content_sample.decode("utf-8")
                # If we can decode as UTF-8 and find common
                # text patterns, likely not a file
                text_markers = (
                    b"{",
                    b"[",
                    b"<",
                    b"function",
                    b"var ",
                    b"const ",
                    b"let ",
                )
                if any(marker in content_sample for marker in text_markers):
                    return is_file
            except UnicodeDecodeError:
                # If we can't decode as UTF-8, likely a binary file
                is_file = True

        # For other types, use MIME type analysis
        main_type, _ = mimetypes.guess_type(
            "dummy" + (mimetypes.guess_extension(content_type) or ""),
        )
        if main_type:
            is_file = main_type.split("/")[0] in _BINARY_CONTENT_MAIN_TYPES

        # For unknown types, check if it's a media type
        if not is_file:
            is_file = any(
                media_type in content_type
                for media_type in ("image/", "audio/", "video/")
            )
        return is_file

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "")

    @property
    def text(self) -> str:
        return self.response.text

    @property
    def content(self) -> bytes:
        return self.response.content

    @property
    def status_code(self) -> int:
        return self.response.status_code

    @property
    def size(self) -> int:
        return len(self.content)

    @property
    def readable_size(self) -> str:
        if self.size < 1024:
            return f"{self.size} bytes"
        if self.size < 1024 * 1024:
            return f"{(self.size / 1024):.2f} KB"
        return f"{(self.size / 1024 / 1024):.2f} MB"

    @property
    def parsed_content_disposition(self) -> Message | None:
        content_disposition = self.headers.get("content-disposition", "")
        if content_disposition:
            msg = Message()
            msg["content-disposition"] = content_disposition
            return msg
        return None
