import base64
import json
import secrets
import string
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, Literal
from urllib.parse import ParseResult, urlencode, urlparse

from json_repair import repair_json

from graphon.file.enums import FileTransferMethod
from graphon.http import HttpClientProtocol, HttpResponse
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.segments import ArrayFileSegment, FileSegment

from ..protocols import FileManagerProtocol
from .entities import (
    BodyData,
    HttpRequestNodeAuthorization,
    HttpRequestNodeConfig,
    HttpRequestNodeData,
    HttpRequestNodeTimeout,
    Response,
)
from .exc import (
    AuthorizationConfigError,
    FileFetchError,
    HttpRequestNodeError,
    InvalidHttpMethodError,
    InvalidURLError,
    RequestBodyError,
    ResponseSizeError,
)

BODY_TYPE_TO_CONTENT_TYPE = {
    "json": "application/json",
    "x-www-form-urlencoded": "application/x-www-form-urlencoded",
    "form-data": "multipart/form-data",
    "raw-text": "text/plain",
}


class Executor:
    method: Literal[
        "get",
        "head",
        "post",
        "put",
        "delete",
        "patch",
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
    params: list[tuple[str, str]] | None
    content: str | bytes | None
    data: Mapping[str, Any] | None
    files: list[tuple[str, tuple[str | None, bytes, str]]] | None
    json: Any
    headers: dict[str, str]
    auth: HttpRequestNodeAuthorization
    timeout: HttpRequestNodeTimeout
    max_retries: int

    boundary: str

    def __init__(
        self,
        *,
        node_data: HttpRequestNodeData,
        timeout: HttpRequestNodeTimeout,
        variable_pool: VariablePool,
        http_request_config: HttpRequestNodeConfig,
        max_retries: int | None = None,
        ssl_verify: bool | None = None,
        http_client: HttpClientProtocol,
        file_manager: FileManagerProtocol,
    ) -> None:
        self._http_request_config = http_request_config
        # If authorization API key is present, convert it using
        # the variable pool.
        if node_data.authorization.type == "api-key":
            if node_data.authorization.config is None:
                msg = "authorization config is required"
                raise AuthorizationConfigError(msg)
            node_data.authorization.config.api_key = variable_pool.convert_template(
                node_data.authorization.config.api_key,
            ).text
            # Validate that API key is not empty after template conversion
            if (
                not node_data.authorization.config.api_key
                or not node_data.authorization.config.api_key.strip()
            ):
                msg = (
                    "API key is required for authorization but was empty. "
                    "Please provide a valid API key."
                )
                raise AuthorizationConfigError(msg)

        self.url = node_data.url
        self.method = node_data.method
        self.auth = node_data.authorization
        self.timeout = timeout
        self.ssl_verify = ssl_verify if ssl_verify is not None else node_data.ssl_verify
        if self.ssl_verify is None:
            self.ssl_verify = self._http_request_config.ssl_verify
        if not isinstance(self.ssl_verify, bool):
            msg = "ssl_verify must be a boolean"
            raise TypeError(msg)
        self.params = None
        self.headers = {}
        self.content = None
        self.files = None
        self.data = None
        self.json = None
        self.max_retries = (
            max_retries
            if max_retries is not None
            else self._http_request_config.ssrf_default_max_retries
        )
        self._http_client = http_client
        self._file_manager = file_manager

        # init template
        self.variable_pool = variable_pool
        self.node_data = node_data
        self._initialize()

    def _initialize(self) -> None:
        self._init_url()
        self._init_params()
        self._init_headers()
        self._init_body()

    def _init_url(self) -> None:
        self.url = self.variable_pool.convert_template(self.node_data.url).text

        # check if url is a valid URL
        if not self.url:
            msg = "url is required"
            raise InvalidURLError(msg)
        if not self.url.startswith(("http://", "https://")):
            msg = "url should start with http:// or https://"
            raise InvalidURLError(msg)

    def _init_params(self) -> None:
        r"""Almost same as _init_headers(), difference:
        1. response a list tuple to support same key, like 'aa=1&aa=2'
        2. param value may have '\n', splitlines then extract
        the variable value.
        """
        result = []
        for line in self.node_data.params.splitlines():
            if not (line := line.strip()):
                continue

            key, *value = line.split(":", 1)
            if not (key := key.strip()):
                continue

            value_str = value[0].strip() if value else ""
            result.append((
                self.variable_pool.convert_template(key).text,
                self.variable_pool.convert_template(value_str).text,
            ))

        if result:
            self.params = result

    def _init_headers(self) -> None:
        r"""Convert the header string of frontend to a dictionary.

        Each line in the header string represents a key-value pair.
        Keys and values are separated by ':'.
        Empty values are allowed.

        Examples:
            'aa:bb\n cc:dd'  -> {'aa': 'bb', 'cc': 'dd'}
            'aa:\n cc:dd\n'  -> {'aa': '', 'cc': 'dd'}
            'aa\n cc : dd'   -> {'aa': '', 'cc': 'dd'}

        """
        headers = self.variable_pool.convert_template(self.node_data.headers).text
        self.headers = {
            key.strip(): (value[0].strip() if value else "")
            for line in headers.splitlines()
            if line.strip()
            for key, *value in [line.split(":", 1)]
        }

    def _init_body(self) -> None:
        body = self.node_data.body
        if body is None:
            return

        match body.type:
            case "none":
                self._init_empty_body(body.data)
            case "raw-text":
                self._init_raw_text_body(body.data)
            case "json":
                self._init_json_body(body.data)
            case "binary":
                self._init_binary_body(body.data)
            case "x-www-form-urlencoded":
                self._init_urlencoded_body(body.data)
            case "form-data":
                self._init_form_data_body(body.data)

    @staticmethod
    def _require_single_body_item(
        data: Sequence[BodyData],
        *,
        body_type: str,
    ) -> BodyData:
        if len(data) != 1:
            msg = f"{body_type} body type should have exactly one item"
            raise RequestBodyError(msg)
        return data[0]

    def _init_empty_body(self, data: Sequence[BodyData]) -> None:
        _ = data
        self.content = ""

    def _init_raw_text_body(self, data: Sequence[BodyData]) -> None:
        item = self._require_single_body_item(data, body_type="raw-text")
        self.content = self.variable_pool.convert_template(item.value).text

    def _init_json_body(self, data: Sequence[BodyData]) -> None:
        item = self._require_single_body_item(data, body_type="json")
        json_string = self.variable_pool.convert_template(item.value).text
        try:
            repaired = repair_json(json_string)
            self.json = json.loads(repaired, strict=False)
        except json.JSONDecodeError as e:
            msg = f"Failed to parse JSON: {json_string}"
            raise RequestBodyError(msg) from e

    def _init_binary_body(self, data: Sequence[BodyData]) -> None:
        item = self._require_single_body_item(data, body_type="binary")
        file_selector = item.file
        file_variable = self.variable_pool.get_file(file_selector)
        if file_variable is None:
            msg = f"cannot fetch file with selector {file_selector}"
            raise FileFetchError(msg)
        self.content = self._file_manager.download(file_variable.value)

    def _init_urlencoded_body(self, data: Sequence[BodyData]) -> None:
        self.data = {
            self.variable_pool.convert_template(
                item.key,
            ).text: self.variable_pool.convert_template(item.value).text
            for item in data
        }

    def _init_form_data_body(self, data: Sequence[BodyData]) -> None:
        self.data = self._build_form_text_data(data)
        file_selectors = self._build_form_file_selectors(data)
        files_list = self._resolve_form_files(file_selectors)
        self.files = self._build_request_files(files_list)

    def _build_form_text_data(self, data: Sequence[BodyData]) -> dict[str, str]:
        return {
            self.variable_pool.convert_template(
                item.key,
            ).text: self.variable_pool.convert_template(item.value).text
            for item in data
            if item.type == "text"
        }

    def _build_form_file_selectors(
        self,
        data: Sequence[BodyData],
    ) -> dict[str, Sequence[str]]:
        return {
            self.variable_pool.convert_template(item.key).text: item.file
            for item in data
            if item.type == "file"
        }

    def _resolve_form_files(
        self,
        file_selectors: Mapping[str, Sequence[str]],
    ) -> list[tuple[str, list[Any]]]:
        files_list: list[tuple[str, list[Any]]] = []
        for key, selector in file_selectors.items():
            segment = self.variable_pool.get(selector)
            if isinstance(segment, FileSegment):
                files_list.append((key, [segment.value]))
            elif isinstance(segment, ArrayFileSegment):
                files_list.append((key, list(segment.value)))
        return files_list

    def _build_request_files(
        self,
        files_list: Sequence[tuple[str, Sequence[Any]]],
    ) -> list[tuple[str, tuple[str | None, bytes, str]]]:
        files: list[tuple[str, tuple[str | None, bytes, str]]] = []
        for key, files_in_segment in files_list:
            for file in files_in_segment:
                if file.reference is None and (
                    file.transfer_method != FileTransferMethod.REMOTE_URL
                    or file.remote_url is None
                ):
                    continue
                files.append((
                    key,
                    (
                        file.filename,
                        self._file_manager.download(file),
                        file.mime_type or "application/octet-stream",
                    ),
                ))

        if files:
            return files

        return [
            (
                "__multipart_placeholder__",
                ("", b"", "application/octet-stream"),
            ),
        ]

    def _assembling_headers(self) -> dict[str, Any]:
        authorization = deepcopy(self.auth)
        headers = deepcopy(self.headers) or {}
        headers.update(self._build_authorization_headers(authorization))
        return self._apply_content_type_header(headers)

    def _build_authorization_headers(
        self,
        authorization: HttpRequestNodeAuthorization,
    ) -> dict[str, str]:
        if self.auth.type != "api-key":
            return {}
        if self.auth.config is None:
            msg = "self.authorization config is required"
            raise AuthorizationConfigError(msg)
        if authorization.config is None:
            msg = "authorization config is required"
            raise AuthorizationConfigError(msg)

        authorization.config.header = authorization.config.header or "Authorization"
        header = authorization.config.header
        api_key = authorization.config.api_key

        match authorization.config.type:
            case "bearer" if api_key:
                return {header: f"Bearer {api_key}"}
            case "basic" if api_key:
                return {header: f"Basic {self._encode_basic_credentials(api_key)}"}
            case "custom" if header and api_key:
                return {header: api_key}
            case _:
                return {}

    @staticmethod
    def _encode_basic_credentials(credentials: str) -> str:
        if ":" not in credentials:
            return credentials
        return base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    def _apply_content_type_header(self, headers: dict[str, str]) -> dict[str, str]:
        body = self.node_data.body
        if body is None:
            return headers

        lower_headers = {key.lower() for key in headers}
        if body.type == "form-data":
            if self.files:
                return {
                    key: value
                    for key, value in headers.items()
                    if key.lower() != "content-type"
                }
            if "content-type" not in lower_headers:
                headers["Content-Type"] = "multipart/form-data"
            return headers

        if (
            body.type in BODY_TYPE_TO_CONTENT_TYPE
            and "content-type" not in lower_headers
        ):
            headers["Content-Type"] = BODY_TYPE_TO_CONTENT_TYPE[body.type]
        return headers

    def _validate_and_parse_response(self, response: HttpResponse) -> Response:
        executor_response = Response(response)

        threshold_size = (
            self._http_request_config.max_binary_size
            if executor_response.is_file
            else self._http_request_config.max_text_size
        )
        if executor_response.size > threshold_size:
            msg = (
                f"{'File' if executor_response.is_file else 'Text'} size is too large,"
                f" max size is {threshold_size / 1024 / 1024:.2f} MB,"
                f" but current size is {executor_response.readable_size}."
            )
            raise ResponseSizeError(msg)

        return executor_response

    def _do_http_request(self, headers: dict[str, Any]) -> HttpResponse:
        """Do http request depending on api bundle"""
        method_map: dict[str, Callable[..., HttpResponse]] = {
            "get": self._http_client.get,
            "head": self._http_client.head,
            "post": self._http_client.post,
            "put": self._http_client.put,
            "delete": self._http_client.delete,
            "patch": self._http_client.patch,
        }
        method_lc = self.method.lower()
        if method_lc not in method_map:
            msg = f"Invalid http method {self.method}"
            raise InvalidHttpMethodError(msg)

        request_args: dict[str, Any] = {
            "data": self.data,
            "files": self.files,
            "json": self.json,
            "content": self.content,
            "headers": headers,
            "params": self.params,
            "timeout": (self.timeout.connect, self.timeout.read, self.timeout.write),
            "ssl_verify": self.ssl_verify,
            "follow_redirects": True,
        }
        # request_args = {k: v for k, v in request_args.items() if v is not None}
        try:
            response = method_map[method_lc](
                url=self.url,
                **request_args,
                max_retries=self.max_retries,
            )
        except self._http_client.max_retries_exceeded_error as e:
            msg = f"Reached maximum retries for URL {self.url}"
            raise HttpRequestNodeError(msg) from e
        except self._http_client.request_error as e:
            raise HttpRequestNodeError(str(e)) from e
        return response

    def invoke(self) -> Response:
        # assemble headers
        headers = self._assembling_headers()
        # do http request
        response = self._do_http_request(headers)
        # validate response
        return self._validate_and_parse_response(response)

    def to_log(self):
        url_parts = urlparse(self.url)
        path = self._build_log_path(url_parts)
        raw = f"{self.method.upper()} {path} HTTP/1.1\r\n"
        raw += f"Host: {url_parts.netloc}\r\n"

        boundary = f"----WebKitFormBoundary{_generate_random_string(16)}"
        headers = self._build_log_headers(boundary)
        raw += self._render_log_headers(headers)

        body_string = self._build_log_body(boundary)
        if body_string:
            raw += f"Content-Length: {len(body_string)}\r\n"
        raw += "\r\n"  # Empty line between headers and body
        raw += body_string

        return raw

    def _build_log_path(self, url_parts: ParseResult) -> str:
        path = url_parts.path or "/"
        if self.params:
            return path + f"?{urlencode(self.params)}"
        if url_parts.query:
            return path + f"?{url_parts.query}"
        return path

    def _build_log_headers(self, boundary: str) -> dict[str, Any]:
        headers = self._assembling_headers()
        body = self.node_data.body
        if body is None:
            return headers
        if (
            "content-type" not in (k.lower() for k in self.headers)
            and body.type in BODY_TYPE_TO_CONTENT_TYPE
        ):
            headers["Content-Type"] = BODY_TYPE_TO_CONTENT_TYPE[body.type]
        if body.type == "form-data":
            headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        return headers

    def _render_log_headers(self, headers: Mapping[str, Any]) -> str:
        raw_headers = ""
        for key, value in headers.items():
            masked_value = self._mask_log_header_value(key, str(value))
            raw_headers += f"{key}: {masked_value}\r\n"
        return raw_headers

    def _mask_log_header_value(self, key: str, value: str) -> str:
        if self.auth.type != "api-key":
            return value
        authorization_header = "Authorization"
        if self.auth.config and self.auth.config.header:
            authorization_header = self.auth.config.header
        if key.lower() == authorization_header.lower():
            return "*" * len(value)
        return value

    def _build_log_body(self, boundary: str) -> str:
        body_string = ""
        if self._has_loggable_files():
            body_string = self._build_file_log_body(boundary)
        elif self.node_data.body is None:
            body_string = ""
        elif self.content:
            body_string = self._build_content_log_body()
        elif self.data and self.node_data.body.type == "x-www-form-urlencoded":
            body_string = urlencode(self.data)
        elif self.data and self.node_data.body.type == "form-data":
            body_string = self._build_form_data_log_body(boundary)
        elif self.json:
            body_string = json.dumps(self.json)
        elif self.node_data.body.type == "raw-text":
            item = self._require_single_body_item(
                self.node_data.body.data,
                body_type="raw-text",
            )
            body_string = item.value
        return body_string

    def _has_loggable_files(self) -> bool:
        return bool(self.files) and not all(
            file_entry[0] == "__multipart_placeholder__" for file_entry in self.files
        )

    def _build_file_log_body(self, boundary: str) -> str:
        body_string = ""
        for file_entry in self.files or []:
            if len(file_entry) != 2 or len(file_entry[1]) < 2:
                continue
            key = file_entry[0]
            filename, content = file_entry[1][0], file_entry[1][1]
            file_mime_type = file_entry[1][2] if len(file_entry[1]) > 2 else "unknown"
            body_string += f"--{boundary}\r\n"
            body_string += f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
            body_string += (
                f"<file_content_binary: '{filename or 'unknown'}', "
                f"type='{file_mime_type}', "
                f"size={len(content)} bytes>\r\n"
            )
        body_string += f"--{boundary}--\r\n"
        return body_string

    def _build_content_log_body(self) -> str:
        if isinstance(self.content, bytes):
            return f"<binary_content: size={len(self.content)} bytes>"
        return str(self.content)

    def _build_form_data_log_body(self, boundary: str) -> str:
        body_string = ""
        for key, value in (self.data or {}).items():
            body_string += f"--{boundary}\r\n"
            body_string += f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
            body_string += f"{value}\r\n"
        body_string += f"--{boundary}--\r\n"
        return body_string


def _generate_random_string(n: int) -> str:
    """Generate a random string of lowercase ASCII letters.

    Args:
        n (int): The length of the random string to generate.

    Returns:
        str: A random string of lowercase ASCII letters with length n.

    Example:
        >>> _generate_random_string(5)
        'abcde'

    """
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(n))
