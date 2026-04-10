from graphon.file.enums import FileType
from graphon.file.file_factory import get_file_type_by_mime_type, standardize_file_type


def test_standardize_file_type_recognizes_case_insensitive_extension() -> None:
    assert standardize_file_type(extension=".PNG") == FileType.IMAGE


def test_standardize_file_type_recognizes_document_extension() -> None:
    assert standardize_file_type(extension=".txt") == FileType.DOCUMENT


def test_standardize_file_type_falls_back_to_mime_type() -> None:
    assert standardize_file_type(mime_type="video/mp4") == FileType.VIDEO


def test_get_file_type_by_mime_type_returns_custom_for_unknown_type() -> None:
    assert get_file_type_by_mime_type("application/octet-stream") == FileType.CUSTOM
