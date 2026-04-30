import pytest

from graphon.file import helpers
from graphon.file.enums import (
    FileTransferMethod,
    FileType,
)
from graphon.file.models import File

from ..helpers import build_file_reference

_HISTORICAL_FILE_JSON_FROM_749751D_PARENT = (
    """{
  "dify_model_identity": "__dify__file__",
  "id": "message-file-id",
  "type": "document",
  "transfer_method": "local_file",
  "remote_url": null,
  "reference": "upload-file-id",
  "filename": "report.pdf",
  "extension": ".pdf",
  "mime_type": "application/pdf",
  "size": 128
}"""
)


def _build_local_file(*, reference: str, storage_key: str | None = None) -> File:
    return File(
        file_id="file-id",
        file_type=FileType.DOCUMENT,
        transfer_method=FileTransferMethod.LOCAL_FILE,
        reference=reference,
        filename="report.pdf",
        extension=".pdf",
        mime_type="application/pdf",
        size=128,
        storage_key=storage_key,
    )


def test_file_exposes_legacy_aliases_from_opaque_reference() -> None:
    reference = build_file_reference(
        record_id="upload-file-id",
        storage_key="files/report.pdf",
    )

    file = _build_local_file(reference=reference)

    assert file.reference == reference
    assert file.related_id == "upload-file-id"
    assert file.storage_key == "files/report.pdf"


def test_file_falls_back_to_raw_reference_when_opaque_reference_is_invalid() -> None:
    file = _build_local_file(
        reference="dify-file-ref:not-base64",
        storage_key="fallback-key",
    )

    assert file.related_id == "dify-file-ref:not-base64"
    assert file.storage_key == "fallback-key"


def test_file_to_dict_keeps_reference_and_legacy_related_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = build_file_reference(
        record_id="upload-file-id",
        storage_key="files/report.pdf",
    )
    file = _build_local_file(reference=reference)

    def fake_resolve_file_url(_file: File, *, for_external: bool = True) -> str:
        _ = for_external
        return "https://example.com/report.pdf"

    monkeypatch.setattr(
        helpers,
        "resolve_file_url",
        fake_resolve_file_url,
    )

    serialized = file.to_dict()

    assert serialized["reference"] == reference
    assert serialized["related_id"] == "upload-file-id"
    assert serialized["url"] == "https://example.com/report.pdf"


def test_file_related_id_setter_updates_reference_alias() -> None:
    file = _build_local_file(reference="upload-file-id", storage_key="files/report.pdf")

    file.related_id = "replacement-upload-id"

    assert file.reference == "replacement-upload-id"
    assert file.related_id == "replacement-upload-id"


def test_file_model_validate_accepts_historical_payload_from_749751d_parent() -> None:
    restored = File.model_validate_json(_HISTORICAL_FILE_JSON_FROM_749751D_PARENT)

    assert restored.id == "message-file-id"
    assert restored.type == FileType.DOCUMENT
    assert restored.reference == "upload-file-id"


def test_file_constructor_rejects_conflicting_identity_kwargs() -> None:
    with pytest.raises(ValueError, match="Conflicting file identifiers") as exc_info:
        File(
            id="message-file-id",
            file_id="other-file-id",
            type=FileType.DOCUMENT,
            transfer_method=FileTransferMethod.LOCAL_FILE,
            reference="upload-file-id",
        )

    assert exc_info.type.__name__ == "_FileConstructorConflictError"
