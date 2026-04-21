# ruff: noqa: SLF001

import json
from unittest.mock import MagicMock

import pytest

from graphon.nodes.document_extractor import node as document_extractor_node
from graphon.nodes.document_extractor.entities import UnstructuredApiConfig
from graphon.nodes.document_extractor.exc import UnsupportedFileTypeError


def test_extract_text_by_file_extension_routes_registered_extractor() -> None:
    payload = {"name": "graphon", "nested": {"value": 1}}

    extracted = document_extractor_node._extract_text_by_file_extension(
        file_content=json.dumps(payload).encode(),
        file_extension=".json",
        unstructured_api_config=UnstructuredApiConfig(),
    )

    assert extracted == json.dumps(payload, indent=2, ensure_ascii=False)


def test_extract_text_by_mime_type_routes_registered_extractor() -> None:
    extracted = document_extractor_node._extract_text_by_mime_type(
        file_content=b"# comment\nfoo=bar\n",
        mime_type="text/properties",
        unstructured_api_config=UnstructuredApiConfig(),
    )

    assert extracted == "# comment\nfoo: bar"


def test_extract_text_from_file_prefers_extension_over_mime_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file = MagicMock()
    file.extension = ".json"
    file.mime_type = "text/plain"

    monkeypatch.setattr(
        document_extractor_node,
        "_download_file_content",
        lambda _http_client, _file: b'{"name":"graphon"}',
    )

    extracted = document_extractor_node._extract_text_from_file(
        MagicMock(),
        file,
        unstructured_api_config=UnstructuredApiConfig(),
    )

    assert extracted == '{\n  "name": "graphon"\n}'


def test_extract_text_by_file_extension_rejects_unknown_type() -> None:
    with pytest.raises(
        UnsupportedFileTypeError,
        match=r"Unsupported Extension Type: \.unknown",
    ):
        document_extractor_node._extract_text_by_file_extension(
            file_content=b"data",
            file_extension=".unknown",
            unstructured_api_config=UnstructuredApiConfig(),
        )
