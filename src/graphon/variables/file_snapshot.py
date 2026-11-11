"""Snapshot untyped values without interpreting ordinary JSON as files."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING, Any

from pydantic import (
    SerializationInfo,
    SerializerFunctionWrapHandler,
    StrictInt,
    StrictStr,
    TypeAdapter,
)

from graphon.file.models import File
from graphon.file.serialization import SerializedFile, mark_serialized_files

if TYPE_CHECKING:
    from .segments import Segment

type FilePath = tuple[StrictStr | StrictInt, ...]

_VALUE_ADAPTER = TypeAdapter(Any)
_FILE_PATHS_ADAPTER = TypeAdapter(list[FilePath])


def serialize_file_snapshot(
    model: Segment, handler: SerializerFunctionWrapHandler, info: SerializationInfo
) -> dict[str, Any]:
    """Project models once, then record files in the resulting snapshot tree."""
    if info.include is not None or info.exclude is not None:
        with mark_serialized_files():
            data = handler(model)
            if "value" in data:
                data["value"] = _collect_file_paths(data["value"], None)
        return data
    with mark_serialized_files():
        serialized_value = _VALUE_ADAPTER.dump_python(
            model.value,
            context=info.context,
            by_alias=info.by_alias,
            exclude_unset=info.exclude_unset,
            exclude_defaults=info.exclude_defaults,
            exclude_none=info.exclude_none,
            round_trip=info.round_trip,
            serialize_as_any=info.serialize_as_any,
        )
        paths: list[FilePath] = []
        value = _collect_file_paths(serialized_value, paths)
    data = handler(model.model_copy(update={"value": value}))
    if paths and "value" in data:
        data["file_paths"] = paths
    return data


def _collect_file_paths(
    value: Any, paths: list[FilePath] | None, path: FilePath = ()
) -> Any:
    if isinstance(value, SerializedFile):
        if paths is not None:
            paths.append(path)
        return value.data
    if isinstance(value, Mapping):
        path_count = len(paths or ())
        result = {
            key: _collect_file_paths(item, paths, (*path, key))
            for key, item in value.items()
        }
        if (
            paths is not None
            and len(paths) > path_count
            and any(not isinstance(key, str) for key in value)
        ):
            msg = "File snapshots require string object keys"
            raise ValueError(msg)
        return result
    if isinstance(value, (Sequence, AbstractSet, Iterator)) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return [
            _collect_file_paths(item, paths, (*path, index))
            for index, item in enumerate(value)
        ]
    return value


def restore_file_snapshot(value: Any, file_paths: Any) -> Any:
    paths = set(_FILE_PATHS_ADAPTER.validate_python(file_paths))
    restored = _restore_files(value, paths)
    if paths:
        msg = "Invalid file path in array snapshot"
        raise ValueError(msg)
    return restored


def _restore_files(value: Any, paths: set[FilePath], path: FilePath = ()) -> Any:
    if path in paths:
        paths.remove(path)
        return File.model_validate(value)
    if isinstance(value, (list, tuple)):
        return type(value)(
            _restore_files(item, paths, (*path, index))
            for index, item in enumerate(value)
        )
    if isinstance(value, dict):
        return {
            key: _restore_files(item, paths, (*path, key))
            for key, item in value.items()
        }
    return value
