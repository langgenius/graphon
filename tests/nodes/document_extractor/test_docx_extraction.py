import io
import struct
import zipfile
from unittest.mock import MagicMock

import pytest
from docx import Document

from graphon.engine import Engine
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import GraphRunFailedEvent
from graphon.engine_events.node import NodeRunFailedEvent
from graphon.file import File
from graphon.file.enums import FileTransferMethod, FileType
from graphon.nodes.document_extractor import node as document_extractor_node
from graphon.nodes.document_extractor.exc import FileDownloadError, TextExtractionError
from tests.engine.test_file_runtime import _make_file_engine
from tests.helpers.workflow_events import final_outputs

_OFFICE_DOCUMENT_RELATIONSHIP = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
)
_STRICT_OFFICE_DOCUMENT_RELATIONSHIP = (
    "http://purl.oclc.org/ooxml/officeDocument/relationships/officeDocument"
)
_WORD_NAMESPACE = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_STRICT_WORD_NAMESPACE = "http://purl.oclc.org/ooxml/wordprocessingml/main"
_EXTRACTED_TEXT = (
    "Hello\tworld\nNext line\n"
    "| Heading | Heading | Value |\n| --- | --- | --- |\n"
    "| Group | Color | Blue<br>Green\tRed |\n"
    "| Group | Shape | Round |\n\nOutro"
)


def test_extract_text_from_docx_keeps_paragraph_and_table_order() -> None:
    document = Document()
    document.add_paragraph("Intro")
    table = document.add_table(rows=2, cols=2)
    table.rows[0].cells[0].text = "Name"
    table.rows[0].cells[1].text = "Value"
    table.rows[1].cells[0].text = "Color"
    table.rows[1].cells[1].text = "Blue"
    document.add_paragraph("Outro")

    buffer = io.BytesIO()
    document.save(buffer)

    extracted = document_extractor_node.extract_text_from_docx(buffer.getvalue())

    assert extracted == (
        "Intro\n| Name | Value |\n| --- | --- |\n| Color | Blue |\n\nOutro"
    )


def _make_sample_docx() -> bytes:
    document = Document()
    document.add_paragraph("Hello\tworld\nNext line")
    table = document.add_table(rows=3, cols=3)
    table.cell(0, 0).merge(table.cell(0, 1)).text = "Heading"
    table.cell(0, 2).text = "Value"
    table.cell(1, 0).merge(table.cell(2, 0)).text = "Group"
    table.cell(1, 1).text = "Color"
    table.cell(1, 2).text = "Blue\nGreen\tRed"
    table.cell(2, 1).text = "Shape"
    table.cell(2, 2).text = "Round"
    document.add_table(rows=1, cols=1)
    document.add_paragraph("Outro")
    buffer = io.BytesIO()
    document.save(buffer)
    return buffer.getvalue()


def _read_docx_parts(payload: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _pack_docx(parts: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in parts.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def _make_docx_variant(payload: bytes, variant: str) -> bytes:
    parts = _read_docx_parts(payload)
    if variant == "strict-relationship":
        parts["_rels/.rels"] = parts["_rels/.rels"].replace(
            _OFFICE_DOCUMENT_RELATIONSHIP.encode(),
            _STRICT_OFFICE_DOCUMENT_RELATIONSHIP.encode(),
        )
    elif variant == "strict-document":
        parts["_rels/.rels"] = parts["_rels/.rels"].replace(
            _OFFICE_DOCUMENT_RELATIONSHIP.encode(),
            _STRICT_OFFICE_DOCUMENT_RELATIONSHIP.encode(),
        )
        parts["word/document.xml"] = parts["word/document.xml"].replace(
            _WORD_NAMESPACE.encode(),
            _STRICT_WORD_NAMESPACE.encode(),
        )
    elif variant == "missing-relationships":
        del parts["_rels/.rels"]
    elif variant == "backslash-paths":
        parts = {name.replace("/", "\\"): data for name, data in parts.items()}
    elif variant == "standard":
        pass
    else:
        msg = f"unknown docx variant: {variant}"
        raise AssertionError(msg)
    return _pack_docx(parts)


@pytest.mark.parametrize(
    "variant",
    [
        "standard",
        "strict-relationship",
        "strict-document",
        "missing-relationships",
        "backslash-paths",
    ],
)
def test_extract_text_from_docx_preserves_text_across_package_variants(
    variant: str,
) -> None:
    extracted = document_extractor_node.extract_text_from_docx(
        _make_docx_variant(_make_sample_docx(), variant)
    )

    assert extracted == _EXTRACTED_TEXT


@pytest.mark.parametrize(
    "variant", ["standard", "missing-relationships", "strict-document"]
)
def test_extract_text_from_docx_preserves_carriage_return_references(
    variant: str,
) -> None:
    document = Document()
    # A literal text node preserves CR; assigning paragraph text inserts line breaks.
    document.add_paragraph().add_run()._r.add_t("alpha\rbeta\r\ngamma")
    table = document.add_table(rows=1, cols=1)
    table.cell(0, 0).paragraphs[0].add_run()._r.add_t("alpha\rbeta\r\ngamma")
    buffer = io.BytesIO()
    document.save(buffer)

    extracted = document_extractor_node.extract_text_from_docx(
        _make_docx_variant(buffer.getvalue(), variant)
    )

    assert extracted == "alpha\rbeta\r\ngamma\n| alpha\rbeta\r<br>gamma |\n| --- |\n"


def test_extract_text_from_docx_uses_main_document_before_nested_document() -> None:
    document_xml = _read_docx_parts(_make_sample_docx())["word/document.xml"]
    payload = _pack_docx({
        "attachment/word/document.xml": document_xml.replace(b"Hello", b"Attachment"),
        "word/document.xml": document_xml,
    })

    assert document_extractor_node.extract_text_from_docx(payload) == _EXTRACTED_TEXT


@pytest.mark.parametrize(
    "document_xml",
    [
        b"<junk/>",
        (
            b'<document xmlns="urn:not-word">'
            b"<body><p><r><t>Text</t></r></p></body></document>"
        ),
        f'<w:document xmlns:w="{_WORD_NAMESPACE}"/>'.encode(),
        b"<document>",
        (
            '<!DOCTYPE document [<!ENTITY text "Injected">]>'
            f'<w:document xmlns:w="{_WORD_NAMESPACE}">'
            "<w:body><w:p><w:r><w:t>&text;</w:t></w:r></w:p></w:body></w:document>"
        ).encode(),
    ],
    ids=["wrong-root", "foreign-namespace", "missing-body", "malformed", "entity"],
)
def test_extract_text_from_docx_rejects_invalid_document_xml(
    document_xml: bytes,
) -> None:
    payload = _pack_docx({"word/document.xml": document_xml})

    with pytest.raises(TextExtractionError, match="Failed to extract text from DOCX"):
        document_extractor_node.extract_text_from_docx(payload)


def test_extract_text_from_docx_accepts_empty_document_without_relationships() -> None:
    document_xml = (
        f'<w:document xmlns:w="{_WORD_NAMESPACE}"><w:body/></w:document>'
    ).encode()

    assert (
        document_extractor_node.extract_text_from_docx(
            _pack_docx({"word/document.xml": document_xml})
        )
        == ""
    )


def _make_docx_with_unsupported_compression() -> bytes:
    document_xml = _read_docx_parts(_make_sample_docx())["word/document.xml"]
    payload = bytearray(_pack_docx({"word/document.xml": document_xml}))
    # ZIP method 99 cannot be decoded; set it in both the local and central headers.
    struct.pack_into("<H", payload, payload.index(b"PK\x03\x04") + 8, 99)
    struct.pack_into("<H", payload, payload.index(b"PK\x01\x02") + 10, 99)
    return bytes(payload)


def test_extract_text_from_docx_wraps_zip_read_errors() -> None:
    with pytest.raises(TextExtractionError, match="Failed to extract text from DOCX"):
        document_extractor_node.extract_text_from_docx(
            _make_docx_with_unsupported_compression()
        )


def _make_docx_with_bad_crc() -> bytes:
    parts = _read_docx_parts(_make_sample_docx())
    payload = bytearray(
        _pack_docx({
            "[Content_Types].xml": parts.pop("[Content_Types].xml"),
            **parts,
        })
    )
    # Corrupt the first member's CRC while leaving word/document.xml intact.
    crc_offset = payload.index(b"PK\x01\x02") + 16
    crc = struct.unpack_from("<I", payload, crc_offset)[0]
    struct.pack_into("<I", payload, crc_offset, crc ^ 1)
    return bytes(payload)


def test_extract_text_from_docx_rejects_corrupt_package_member() -> None:
    with pytest.raises(TextExtractionError, match="Failed to extract text from DOCX"):
        document_extractor_node.extract_text_from_docx(_make_docx_with_bad_crc())


def _make_docx_engine(payload: bytes) -> Engine:
    file = File(
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference="upload",
        filename="report.docx",
        extension=".docx",
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    file_runtime = MagicMock(
        load_file_bytes=MagicMock(return_value=payload),
        resolve_file_url=MagicMock(return_value="https://files.example/report.docx"),
    )
    return _make_file_engine(file, file_runtime=file_runtime)


def test_engine_extracts_strict_docx_from_file_runtime() -> None:
    engine = _make_docx_engine(
        _make_docx_variant(_make_sample_docx(), "strict-document")
    )

    assert final_outputs(list(engine.run()))["text"] == _EXTRACTED_TEXT


@pytest.mark.parametrize("variant", ["invalid-xml", "unreadable-zip", "bad-crc"])
def test_engine_reports_docx_extraction_failure_with_document_context(
    variant: str,
) -> None:
    if variant == "invalid-xml":
        payload = _pack_docx({"word/document.xml": b"<junk/>"})
    elif variant == "unreadable-zip":
        payload = _make_docx_with_unsupported_compression()
    else:
        payload = _make_docx_with_bad_crc()
    events: list[EngineEvent] = []
    with pytest.raises(RuntimeError, match="Failed to extract text from DOCX"):
        events.extend(_make_docx_engine(payload).run())

    assert isinstance(events[-1], GraphRunFailedEvent)
    failure = next(event for event in events if isinstance(event, NodeRunFailedEvent))
    assert "Failed to extract text from DOCX" in failure.error
    result = failure.node_run_result
    assert result.inputs == {"variable_selector": ["sys", "file"]}
    assert result.process_data["documents"][0].filename == "report.docx"


def test_extract_text_from_docx_rejects_package_without_document_xml() -> None:
    payload = _pack_docx({"[Content_Types].xml": b"<Types></Types>"})

    with pytest.raises(TextExtractionError, match="Failed to extract text from DOCX"):
        document_extractor_node.extract_text_from_docx(payload)


def test_download_file_content_requires_remote_url() -> None:
    file = MagicMock()
    file.transfer_method = FileTransferMethod.REMOTE_URL
    file.remote_url = None
    file.type = FileType.DOCUMENT

    with pytest.raises(FileDownloadError, match="Missing URL for remote file"):
        document_extractor_node.download_file_content(MagicMock(), file)
