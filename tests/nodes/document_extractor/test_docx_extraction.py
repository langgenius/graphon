import io
import zipfile
from unittest.mock import MagicMock

import pytest
from docx import Document

from graphon.file.enums import FileTransferMethod, FileType
from graphon.nodes.document_extractor import node as document_extractor_node
from graphon.nodes.document_extractor.exc import FileDownloadError, TextExtractionError

_OFFICE_DOCUMENT_REL = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
)
_STRICT_OFFICE_DOCUMENT_REL = (
    "http://purl.oclc.org/ooxml/officeDocument/relationships/officeDocument"
)
_WML_TRANSITIONAL = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_WML_STRICT = "http://purl.oclc.org/ooxml/wordprocessingml/main"


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


def _sample_docx_bytes() -> bytes:
    document = Document()
    document.add_paragraph("Hello world")
    table = document.add_table(rows=2, cols=2)
    table.rows[0].cells[0].text = "Name"
    table.rows[0].cells[1].text = "Value"
    table.rows[1].cells[0].text = "Color"
    table.rows[1].cells[1].text = "Blue"
    document.add_paragraph("Outro")
    buffer = io.BytesIO()
    document.save(buffer)
    return buffer.getvalue()


def _docx_parts(payload: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _pack_docx(parts: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in parts.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def _nonstandard_docx(variant: str) -> bytes:
    parts = _docx_parts(_sample_docx_bytes())
    if variant == "strict-relationship":
        parts["_rels/.rels"] = parts["_rels/.rels"].replace(
            _OFFICE_DOCUMENT_REL.encode(),
            _STRICT_OFFICE_DOCUMENT_REL.encode(),
        )
    elif variant == "strict-document":
        parts["_rels/.rels"] = parts["_rels/.rels"].replace(
            _OFFICE_DOCUMENT_REL.encode(),
            _STRICT_OFFICE_DOCUMENT_REL.encode(),
        )
        parts["word/document.xml"] = parts["word/document.xml"].replace(
            _WML_TRANSITIONAL.encode(),
            _WML_STRICT.encode(),
        )
    elif variant == "missing-relationships":
        del parts["_rels/.rels"]
    else:
        msg = f"unknown docx variant: {variant}"
        raise AssertionError(msg)
    return _pack_docx(parts)


@pytest.mark.parametrize(
    "variant",
    ["strict-relationship", "strict-document", "missing-relationships"],
)
def test_extract_text_from_docx_reads_package_when_opc_relationships_fail(
    variant: str,
) -> None:
    expected = document_extractor_node.extract_text_from_docx(_sample_docx_bytes())

    extracted = document_extractor_node.extract_text_from_docx(
        _nonstandard_docx(variant)
    )

    assert extracted == expected


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
