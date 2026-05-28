import io
import json
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pandas as pd
import pytest

from graphon.nodes.document_extractor import node as document_extractor_node
from graphon.nodes.document_extractor.entities import UnstructuredApiConfig
from graphon.nodes.document_extractor.exc import (
    TextExtractionError,
    UnsupportedFileTypeError,
)


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


def test_extract_text_from_csv_handles_empty_payload() -> None:
    assert document_extractor_node._extract_text_from_csv(b"") == ""


def test_extract_text_from_csv_normalizes_multiline_cells() -> None:
    extracted = document_extractor_node._extract_text_from_csv(
        b'name,notes\nalice,"hello\nworld"\n',
    )

    assert extracted == (
        "| name | notes |\n| ---- | ----- |\n| alice | hello world |\n"
    )


def test_extract_text_from_excel_reads_memory_workbook() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame({"Name": ["Alice\nSmith"], "Value": [1]}).to_excel(
            writer,
            index=False,
        )

    extracted = document_extractor_node._extract_text_from_excel(buffer.getvalue())

    assert "| Name | Value |" in extracted
    assert "| Alice Smith | 1 |" in extracted


def test_excel_file_to_markdown_skips_invalid_sheet() -> None:
    class ExcelFile:
        sheet_names: ClassVar[list[str]] = ["good", "bad"]

        def parse(self, *, sheet_name: str) -> Any:
            if sheet_name == "bad":
                msg = "bad sheet"
                raise ValueError(msg)
            return pd.DataFrame({"Name": ["Alice\nSmith"]})

    extracted = document_extractor_node._excel_file_to_markdown(ExcelFile())

    assert "| Name |" in extracted
    assert "Alice Smith" in extracted
    assert "bad sheet" not in extracted


def test_partition_unstructured_file_uses_local_partition() -> None:
    prepared = []

    def load_partition() -> Any:
        return lambda **_kwargs: [SimpleNamespace(text="slide")]

    extracted = document_extractor_node._partition_unstructured_file(
        b"ppt",
        suffix=".ppt",
        unstructured_api_config=UnstructuredApiConfig(),
        load_local_partition=load_partition,
        render_element=lambda element: element.text,
        prepare=lambda: prepared.append(True),
    )

    assert extracted == "slide"
    assert prepared == [True]


def test_partition_unstructured_file_uses_api_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        document_extractor_node,
        "_partition_unstructured_file_via_api",
        lambda *_args, **_kwargs: [SimpleNamespace(text="remote slide")],
    )

    extracted = document_extractor_node._partition_unstructured_file(
        b"ppt",
        suffix=".ppt",
        unstructured_api_config=UnstructuredApiConfig(api_url="https://api.example"),
        load_local_partition=lambda: lambda **_kwargs: [],
        render_element=lambda element: element.text,
    )

    assert extracted == "remote slide"


@pytest.mark.parametrize(
    ("extractor", "label"),
    [
        (document_extractor_node._extract_text_from_ppt, "PPT"),
        (document_extractor_node._extract_text_from_pptx, "PPTX"),
        (document_extractor_node._extract_text_from_epub, "EPUB"),
    ],
)
def test_unstructured_extractors_convert_partition_errors(
    monkeypatch: pytest.MonkeyPatch,
    extractor: Any,
    label: str,
) -> None:
    def fail_partition(*_args: Any, **_kwargs: Any) -> str:
        msg = "partition failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        document_extractor_node,
        "_partition_unstructured_file",
        fail_partition,
    )

    with pytest.raises(
        TextExtractionError,
        match=f"Failed to extract text from {label}",
    ):
        extractor(b"data", unstructured_api_config=UnstructuredApiConfig())


def test_extract_text_from_properties_preserves_supported_line_shapes() -> None:
    extracted = document_extractor_node._extract_text_from_properties(
        b"# comment\n\nkey=value\nother: entry\nflag\n",
    )

    assert extracted == "# comment\n\nkey: value\nother: entry\nflag: "
