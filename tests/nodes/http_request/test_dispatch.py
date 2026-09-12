import json
from unittest.mock import MagicMock

from graphon.nodes.http_request.entities import (
    HttpRequestNodeConfig,
    HttpRequestNodeData,
    HttpRequestNodeTimeout,
)
from graphon.nodes.http_request.executor import Executor
from graphon.nodes.http_request.node import HttpRequestNode
from tests.helpers.builders import build_variable_pool


def _build_http_request_config() -> HttpRequestNodeConfig:
    return HttpRequestNodeConfig(
        max_connect_timeout=10,
        max_read_timeout=10,
        max_write_timeout=10,
        max_binary_size=1024 * 1024,
        max_text_size=1024 * 1024,
        ssl_verify=True,
        ssrf_default_max_retries=0,
    )


def test_http_request_node_extracts_variable_selectors_from_form_data() -> None:
    node_data = HttpRequestNodeData.model_validate({
        "method": "post",
        "url": "https://example.com/{{#start.id#}}",
        "authorization": {"type": "no-auth"},
        "headers": "X-Trace: {{#sys.trace#}}",
        "params": "q: {{#input.query#}}",
        "body": {
            "type": "form-data",
            "data": [
                {
                    "key": "name",
                    "type": "text",
                    "value": "{{#input.name#}}",
                },
                {
                    "key": "file",
                    "type": "file",
                    "file": ["input", "file"],
                },
            ],
        },
    })

    mapping = HttpRequestNode.extract_variable_selector_to_variable_mapping(
        graph_config={},
        config={
            "id": "node-1",
            "data": node_data.model_dump(mode="json"),
        },
    )

    assert mapping == {
        "node-1.#start.id#": ["start", "id"],
        "node-1.#sys.trace#": ["sys", "trace"],
        "node-1.#input.query#": ["input", "query"],
        "node-1.#input.name#": ["input", "name"],
        "node-1.#input.file#": ["input", "file"],
    }


def test_executor_initializes_json_body_with_body_handler_map() -> None:
    executor = Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com",
            "authorization": {"type": "no-auth"},
            "headers": "",
            "params": "",
            "body": {
                "type": "json",
                "data": [
                    {
                        "type": "text",
                        "value": '{"answer": 1}',
                    },
                ],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=MagicMock(),
    )

    assert executor.json == {"answer": 1}


def _json_body_executor(template: str, variables: object = ()) -> Executor:
    return Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com",
            "authorization": {"type": "no-auth"},
            "headers": "",
            "params": "",
            "body": {
                "type": "json",
                "data": [{"type": "text", "value": template}],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(variables=variables),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=MagicMock(),
    )


# A Code node returning markdown: double quotes, backslashes, newlines, pipes.
_MARKDOWN = '### a\n\n| a | a\\a\\a | a : "[a.a](a://a)" | a + "&a\\b=c" |\n'


def test_json_body_preserves_a_string_containing_json_metacharacters() -> None:
    """langgenius/dify#31927.

    The template is valid JSON; substituting a raw value into it is what
    breaks the document, and repair_json then dropped the value entirely,
    sending an empty string to the upstream API.
    """
    template = (
        '{"model": "pro", "messages": [{"role": "user", "content": {{#node.result#}}}]}'
    )
    executor = _json_body_executor(
        template,
        variables=[(["node", "result"], _MARKDOWN)],
    )

    assert executor.json["messages"][0]["content"] == _MARKDOWN


def test_json_body_preserves_a_string_the_author_already_quoted() -> None:
    executor = _json_body_executor(
        '{"content": "{{#node.result#}}"}',
        variables=[(["node", "result"], 'he said "hi"\nbye')],
    )

    assert executor.json["content"] == 'he said "hi"\nbye'


def test_json_body_preserves_a_string_embedded_partway_through_a_literal() -> None:
    """Quote state is tracked across literals, not sniffed from neighbours."""
    executor = _json_body_executor(
        '{"content": "prefix {{#node.result#}} suffix"}',
        variables=[(["node", "result"], 'a"b')],
    )

    assert executor.json["content"] == 'prefix a"b suffix'


def test_json_body_keeps_non_string_values_as_json_tokens() -> None:
    executor = _json_body_executor(
        '{"count": {{#node.n#}}, "ok": {{#node.b#}}, "obj": {{#node.o#}}}',
        variables=[
            (["node", "n"], 42),
            (["node", "b"], True),
            (["node", "o"], {"k": "v"}),
        ],
    )

    assert executor.json == {"count": 42, "ok": True, "obj": {"k": "v"}}


def test_json_body_still_repairs_a_template_that_was_malformed_to_begin_with() -> None:
    """Escaping cannot fix a broken template, so the repair path is retained."""
    executor = _json_body_executor("{'answer': 1,}")

    assert executor.json == {"answer": 1}


def test_json_body_encodes_the_log_copy_the_same_way() -> None:
    """The log copy must also be valid JSON, not the raw concatenation."""
    plain = 'a"b\nc'
    executor = _json_body_executor(
        '{"note": {{#node.value#}}}',
        variables=[(["node", "value"], plain)],
    )

    assert executor.json["note"] == plain
    assert json.loads(executor._log_json_text)["note"] == plain


def test_executor_initializes_form_data_placeholder_when_no_files_resolve() -> None:
    file_manager = MagicMock()
    executor = Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com",
            "authorization": {"type": "no-auth"},
            "headers": "",
            "params": "",
            "body": {
                "type": "form-data",
                "data": [
                    {"key": "note", "type": "text", "value": "hello"},
                    {
                        "key": "upload",
                        "type": "file",
                        "file": ["missing", "file"],
                    },
                ],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=file_manager,
    )

    assert executor.data == {"note": "hello"}
    assert executor.files == [
        ("__multipart_placeholder__", ("", b"", "application/octet-stream")),
    ]
    file_manager.download.assert_not_called()


def test_executor_assembling_headers_applies_bearer_auth_and_content_type() -> None:
    executor = Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com",
            "authorization": {
                "type": "api-key",
                "config": {
                    "type": "bearer",
                    "api_key": "secret-token",
                },
            },
            "headers": "X-Test: 1",
            "params": "",
            "body": {
                "type": "raw-text",
                "data": [{"type": "text", "value": "payload"}],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=MagicMock(),
    )

    headers = executor.build_headers()

    assert headers["Authorization"] == "Bearer secret-token"
    assert headers["Content-Type"] == "text/plain"
    assert headers["X-Test"] == "1"


def test_executor_to_log_masks_authorization_and_logs_raw_text_body() -> None:
    executor = Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com/api?debug=1",
            "authorization": {
                "type": "api-key",
                "config": {
                    "type": "bearer",
                    "api_key": "secret-token",
                },
            },
            "headers": "X-Test: 1",
            "params": "",
            "body": {
                "type": "raw-text",
                "data": [{"type": "text", "value": "payload"}],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=MagicMock(),
    )

    raw = executor.to_log()

    assert raw.startswith("POST /api?debug=1 HTTP/1.1\r\n")
    assert "Authorization: *******************" in raw
    assert "X-Test: 1" in raw
    assert raw.endswith("\r\npayload")


def test_executor_to_log_renders_file_entries_for_multipart_body() -> None:
    executor = Executor(
        node_data=HttpRequestNodeData.model_validate({
            "method": "post",
            "url": "https://example.com/upload",
            "authorization": {"type": "no-auth"},
            "headers": "",
            "params": "",
            "body": {
                "type": "form-data",
                "data": [{"key": "note", "type": "text", "value": "hello"}],
            },
        }),
        timeout=HttpRequestNodeTimeout(connect=1, read=1, write=1),
        variable_pool=build_variable_pool(),
        http_request_config=_build_http_request_config(),
        http_client=MagicMock(),
        file_manager=MagicMock(),
    )
    executor.files = [("upload", ("photo.png", b"abc", "image/png"))]

    raw = executor.to_log()

    assert "Content-Type: multipart/form-data; boundary=" in raw
    assert 'Content-Disposition: form-data; name="upload"' in raw
    assert "<file_content_binary: 'photo.png', type='image/png', size=3 bytes>" in raw
