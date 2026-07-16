import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, Self
from unittest.mock import MagicMock

import pandas as pd
import pytest

from graphon.nodes.document_extractor import node as document_extractor_node
from graphon.nodes.document_extractor.entities import UnstructuredApiConfig
from graphon.nodes.document_extractor.exc import (
    TextExtractionError,
    UnsupportedFileTypeError,
)

_PDF_BYTES = b"%PDF"


@pytest.mark.parametrize(
    "timeout_seconds",
    [0, -1, float("inf"), float("-inf"), float("nan")],
)
def test_unstructured_api_config_rejects_invalid_timeout(
    timeout_seconds: float,
) -> None:
    with pytest.raises(
        ValueError,
        match="timeout_seconds must be a finite number greater than 0",
    ):
        UnstructuredApiConfig(timeout_seconds=timeout_seconds)


def _minimal_text_pdf(text: str) -> bytes:
    stream = f"BT /F1 24 Tf 72 720 Td ({text}) Tj ET".encode()
    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        (
            b"3 0 obj\n"
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>\n"
            b"endobj\n"
        ),
        b"4 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
        (
            b"5 0 obj\n<< /Length "
            + str(len(stream)).encode()
            + b" >>\nstream\n"
            + stream
            + b"\nendstream\nendobj\n"
        ),
    ]
    content = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(content))
        content += obj

    xref_offset = len(content)
    xref = [b"xref\n0 6\n", b"0000000000 65535 f \n"]
    xref.extend(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return (
        content
        + b"".join(xref)
        + b"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n"
        + str(xref_offset).encode()
        + b"\n%%EOF\n"
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


@pytest.mark.parametrize(
    ("extract", "route_kwargs"),
    [
        (
            document_extractor_node._extract_text_by_file_extension,
            {"file_extension": ".odt"},
        ),
        (
            document_extractor_node._extract_text_by_mime_type,
            {"mime_type": "application/vnd.oasis.opendocument.text"},
        ),
    ],
)
def test_opendocument_text_routes_to_unstructured_extractor(
    monkeypatch: pytest.MonkeyPatch,
    extract: Any,
    route_kwargs: dict[str, str],
) -> None:
    def extract_odt(
        _file_content: bytes, *, unstructured_api_config: UnstructuredApiConfig
    ) -> str:
        _ = unstructured_api_config
        return "OpenDocument text"

    monkeypatch.setattr(document_extractor_node, "_extract_text_from_odt", extract_odt)
    monkeypatch.setattr(
        document_extractor_node,
        "_TEXT_EXTRACTOR_REGISTRY",
        document_extractor_node._build_text_extractor_registry(),
    )

    assert (
        extract(
            file_content=b"odt",
            unstructured_api_config=UnstructuredApiConfig(),
            **route_kwargs,
        )
        == "OpenDocument text"
    )


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


@pytest.mark.parametrize("route_kind", ["direct", "extension", "mime_type"])
def test_pdf_extractors_read_real_text_pdf(route_kind: str) -> None:
    file_content = _minimal_text_pdf("Graphon PDF")

    if route_kind == "direct":
        extracted = document_extractor_node._extract_text_from_pdf(file_content)
    elif route_kind == "extension":
        extracted = document_extractor_node._extract_text_by_file_extension(
            file_content=file_content,
            file_extension=".pdf",
            unstructured_api_config=UnstructuredApiConfig(),
        )
    else:
        extracted = document_extractor_node._extract_text_by_mime_type(
            file_content=file_content,
            mime_type="application/pdf",
            unstructured_api_config=UnstructuredApiConfig(),
        )

    assert "Graphon PDF" in extracted


@pytest.mark.parametrize(
    ("pdfium_text", "fallback_text", "expected", "fallback_calls"),
    [
        ("pdfium text", "unused fallback", "pdfium text", 0),
        ("\n  ", "fallback text", "fallback text", 1),
    ],
)
def test_extract_text_from_pdf_uses_fallback_only_for_empty_pdfium_text(
    monkeypatch: pytest.MonkeyPatch,
    pdfium_text: str,
    fallback_text: str,
    expected: str,
    fallback_calls: int,
) -> None:
    fallback_inputs = []

    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pdfium",
        lambda _file_content: pdfium_text,
    )

    def read_fallback(file_content: bytes) -> str:
        fallback_inputs.append(file_content)
        return fallback_text

    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pypdf",
        read_fallback,
    )

    extracted = document_extractor_node._extract_text_from_pdf(_PDF_BYTES)

    assert extracted == expected
    assert fallback_inputs == [_PDF_BYTES] * fallback_calls


@pytest.mark.parametrize("fallback_text", ["", "  "])
def test_extract_text_from_pdf_keeps_pdfium_empty_text_when_fallback_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    fallback_text: str,
) -> None:
    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pdfium",
        lambda _file_content: "\n  ",
    )
    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pypdf",
        lambda _file_content: fallback_text,
    )

    extracted = document_extractor_node._extract_text_from_pdf(_PDF_BYTES)

    assert extracted == "\n  "


def test_extract_text_from_pdf_keeps_pdfium_empty_text_when_fallback_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_fallback_error(_file_content: bytes) -> str:
        msg = "cannot parse fallback"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pdfium",
        lambda _file_content: "\n  ",
    )
    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pypdf",
        raise_fallback_error,
    )

    extracted = document_extractor_node._extract_text_from_pdf(_PDF_BYTES)

    assert extracted == "\n  "


def test_extract_text_from_pdf_closes_reader_when_fallback_page_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Page:
        def extract_text(self) -> str:
            msg = "bad page"
            raise RuntimeError(msg)

    class Reader:
        def __init__(self) -> None:
            self.pages = [Page()]

        def close(self) -> None:
            events.append("reader.close")

    events: list[str] = []

    monkeypatch.setattr(
        document_extractor_node,
        "_read_pdf_text_with_pdfium",
        lambda _file_content: "\n  ",
    )
    monkeypatch.setattr(
        document_extractor_node.pypdf,
        "PdfReader",
        lambda *_args, **_kwargs: Reader(),
    )

    extracted = document_extractor_node._extract_text_from_pdf(_PDF_BYTES)

    assert extracted == "\n  "
    assert events == ["reader.close"]


def test_read_pdf_text_with_pdfium_closes_resources_when_text_page_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Page:
        def get_textpage(self) -> None:
            events.append("page.get_textpage")
            msg = "missing text page"
            raise RuntimeError(msg)

        def close(self) -> None:
            events.append("page.close")

    class Document:
        def __enter__(self) -> Self:
            events.append("document.enter")
            return self

        def __exit__(self, *_args: object) -> None:
            events.append("document.close")

        def __iter__(self) -> Any:
            events.append("document.iter")
            return iter([Page()])

    events: list[str] = []

    monkeypatch.setattr(
        document_extractor_node.pypdfium2,
        "PdfDocument",
        lambda *_args, **_kwargs: Document(),
    )

    with pytest.raises(RuntimeError, match="missing text page"):
        document_extractor_node._read_pdf_text_with_pdfium(_PDF_BYTES)

    assert events == [
        "document.enter",
        "document.iter",
        "page.get_textpage",
        "page.close",
        "document.close",
    ]


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


@pytest.mark.parametrize(
    ("timeout_seconds", "expected_timeout_ms"),
    [(300.0, 300_000), (12.5, 12_500)],
)
def test_partition_file_via_unstructured_api_configures_sdk_request(
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: float,
    expected_timeout_ms: int | None,
) -> None:
    partition_calls: list[dict[str, Any]] = []

    def partition(**kwargs: Any) -> SimpleNamespace:
        request = kwargs["request"]
        files = request.partition_parameters.files
        partition_calls.append({
            "content": files.content.read(),
            "file_name": files.file_name,
            "retries": kwargs["retries"],
            "timeout_ms": kwargs["timeout_ms"],
        })
        return SimpleNamespace(
            elements=[
                {
                    "type": "NarrativeText",
                    "text": "remote text",
                    "metadata": {},
                },
            ],
        )

    client = MagicMock()
    client.__enter__.return_value = client
    client.general.partition.side_effect = partition
    client_factory = MagicMock(return_value=client)
    monkeypatch.setattr("unstructured_client.UnstructuredClient", client_factory)

    elements = document_extractor_node._partition_file_via_unstructured_api(
        b"document",
        suffix=".doc",
        unstructured_api_config=UnstructuredApiConfig(
            api_url="https://api.example/general/v0/general",
            api_key="secret",
            timeout_seconds=timeout_seconds,
        ),
    )

    assert [element.text for element in elements] == ["remote text"]
    client_factory.assert_called_once_with(
        api_key_auth="secret",
        server_url="https://api.example",
    )
    assert len(partition_calls) == 1
    partition_call = partition_calls[0]
    assert partition_call["content"] == b"document"
    assert Path(partition_call["file_name"]).suffix == ".doc"
    assert not Path(partition_call["file_name"]).exists()
    assert partition_call["retries"] is None
    assert partition_call["timeout_ms"] == expected_timeout_ms
    client.__exit__.assert_called_once()


def test_partition_file_via_unstructured_api_uses_default_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.__enter__.return_value = client
    client.general.partition.return_value = SimpleNamespace(elements=[])
    monkeypatch.setattr(
        "unstructured_client.UnstructuredClient",
        MagicMock(return_value=client),
    )

    document_extractor_node._partition_file_via_unstructured_api(
        b"document",
        suffix=".doc",
        unstructured_api_config=UnstructuredApiConfig(
            api_url="https://api.example",
        ),
    )

    assert client.general.partition.call_args.kwargs["timeout_ms"] == 300_000


def test_partition_file_via_unstructured_api_cleans_up_after_sdk_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_paths: list[Path] = []

    def partition(**kwargs: Any) -> None:
        request = kwargs["request"]
        temp_paths.append(Path(request.partition_parameters.files.file_name))
        msg = "partition failed"
        raise RuntimeError(msg)

    client = MagicMock()
    client.__enter__.return_value = client
    client.general.partition.side_effect = partition
    monkeypatch.setattr(
        "unstructured_client.UnstructuredClient",
        MagicMock(return_value=client),
    )

    with pytest.raises(RuntimeError, match="partition failed"):
        document_extractor_node._partition_file_via_unstructured_api(
            b"document",
            suffix=".ppt",
            unstructured_api_config=UnstructuredApiConfig(
                api_url="https://api.example",
            ),
        )

    assert len(temp_paths) == 1
    assert not temp_paths[0].exists()
    client.__exit__.assert_called_once()


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
        (document_extractor_node._extract_text_from_odt, "ODT"),
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
