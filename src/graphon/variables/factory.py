"""Graph-owned helpers for converting runtime values, segments, and variables.

These conversions are part of the `graphon` runtime model and must stay
independent from top-level API factory modules so graph nodes and state
containers can operate without importing application-layer packages.
"""

from collections.abc import Mapping, Sequence
from typing import Any, cast
from uuid import uuid4

from graphon.file.models import File

from .segments import (
    ArrayAnySegment,
    ArrayBooleanSegment,
    ArrayFileSegment,
    ArrayNumberSegment,
    ArrayObjectSegment,
    ArraySegment,
    ArrayStringSegment,
    BooleanSegment,
    FileSegment,
    FloatSegment,
    IntegerSegment,
    NoneSegment,
    ObjectSegment,
    Segment,
    StringSegment,
)
from .types import SegmentType
from .variables import (
    ArrayAnyVariable,
    ArrayBooleanVariable,
    ArrayFileVariable,
    ArrayNumberVariable,
    ArrayObjectVariable,
    ArrayStringVariable,
    BooleanVariable,
    FileVariable,
    FloatVariable,
    IntegerVariable,
    NoneVariable,
    ObjectVariable,
    StringVariable,
    Variable,
    VariableBase,
)


class UnsupportedSegmentTypeError(Exception):
    pass


class TypeMismatchError(Exception):
    pass


SEGMENT_TO_VARIABLE_MAP: Mapping[type[Segment], type[Variable]] = {
    ArrayAnySegment: ArrayAnyVariable,
    ArrayBooleanSegment: ArrayBooleanVariable,
    ArrayFileSegment: ArrayFileVariable,
    ArrayNumberSegment: ArrayNumberVariable,
    ArrayObjectSegment: ArrayObjectVariable,
    ArrayStringSegment: ArrayStringVariable,
    BooleanSegment: BooleanVariable,
    FileSegment: FileVariable,
    FloatSegment: FloatVariable,
    IntegerSegment: IntegerVariable,
    NoneSegment: NoneVariable,
    ObjectSegment: ObjectVariable,
    StringSegment: StringVariable,
}

_NUMERICAL_SEGMENT_TYPES = frozenset((
    SegmentType.NUMBER,
    SegmentType.INTEGER,
    SegmentType.FLOAT,
))
_ARRAY_SEGMENT_FACTORY_BY_VALUE_TYPE: Mapping[SegmentType, type[Segment]] = {
    SegmentType.STRING: ArrayStringSegment,
    SegmentType.NUMBER: ArrayNumberSegment,
    SegmentType.INTEGER: ArrayNumberSegment,
    SegmentType.FLOAT: ArrayNumberSegment,
    SegmentType.BOOLEAN: ArrayBooleanSegment,
    SegmentType.OBJECT: ArrayObjectSegment,
    SegmentType.FILE: ArrayFileSegment,
    SegmentType.NONE: ArrayAnySegment,
}
_EMPTY_ARRAY_SEGMENT_FACTORY: Mapping[SegmentType, type[Segment]] = {
    SegmentType.ARRAY_ANY: ArrayAnySegment,
    SegmentType.ARRAY_STRING: ArrayStringSegment,
    SegmentType.ARRAY_BOOLEAN: ArrayBooleanSegment,
    SegmentType.ARRAY_NUMBER: ArrayNumberSegment,
    SegmentType.ARRAY_OBJECT: ArrayObjectSegment,
    SegmentType.ARRAY_FILE: ArrayFileSegment,
}


def _build_non_list_segment(value: Any) -> Segment | None:
    match value:
        case None:
            segment = NoneSegment()
        case Segment():
            segment = value
        case str():
            segment = StringSegment(value=value)
        case bool():
            segment = BooleanSegment(value=value)
        case int():
            segment = IntegerSegment(value=value)
        case float():
            segment = FloatSegment(value=value)
        case dict():
            segment = ObjectSegment(value=value)
        case File():
            segment = FileSegment(value=value)
        case _:
            segment = None
    return segment


def _build_list_segment(value: list[Any]) -> Segment:
    items = [build_segment(item) for item in value]
    types = {item.value_type for item in items}

    if all(isinstance(item, ArraySegment) for item in items):
        return ArrayAnySegment(value=value)
    if len(types) != 1:
        return (
            ArrayNumberSegment(value=value)
            if types.issubset(_NUMERICAL_SEGMENT_TYPES)
            else ArrayAnySegment(value=value)
        )

    segment_class = _ARRAY_SEGMENT_FACTORY_BY_VALUE_TYPE.get(types.pop())
    if segment_class is None:
        msg = f"not supported value {value}"
        raise ValueError(msg)
    return segment_class(value=value)


def _build_empty_array_segment(
    *,
    segment_type: SegmentType,
    value: list[Any],
) -> Segment | None:
    segment_class = _EMPTY_ARRAY_SEGMENT_FACTORY.get(segment_type)
    return None if segment_class is None else segment_class(value=value)


def _resolve_segment_class_for_type_match(
    *,
    segment_type: SegmentType,
    inferred_type: SegmentType,
) -> type[Segment] | None:
    if inferred_type == segment_type:
        return _SEGMENT_FACTORY[segment_type]
    if segment_type == SegmentType.NUMBER and inferred_type in frozenset((
        SegmentType.INTEGER,
        SegmentType.FLOAT,
    )):
        return _SEGMENT_FACTORY[inferred_type]
    return None


def build_segment(value: Any, /) -> Segment:
    """Build a runtime segment from a Python value."""
    segment = _build_non_list_segment(value)
    if segment is not None:
        return segment
    if isinstance(value, list):
        return _build_list_segment(value)
    msg = f"not supported value {value}"
    raise ValueError(msg)


_SEGMENT_FACTORY: Mapping[SegmentType, type[Segment]] = {
    SegmentType.NONE: NoneSegment,
    SegmentType.STRING: StringSegment,
    SegmentType.INTEGER: IntegerSegment,
    SegmentType.FLOAT: FloatSegment,
    SegmentType.FILE: FileSegment,
    SegmentType.BOOLEAN: BooleanSegment,
    SegmentType.OBJECT: ObjectSegment,
    SegmentType.ARRAY_ANY: ArrayAnySegment,
    SegmentType.ARRAY_STRING: ArrayStringSegment,
    SegmentType.ARRAY_NUMBER: ArrayNumberSegment,
    SegmentType.ARRAY_OBJECT: ArrayObjectSegment,
    SegmentType.ARRAY_FILE: ArrayFileSegment,
    SegmentType.ARRAY_BOOLEAN: ArrayBooleanSegment,
}


def build_segment_with_type(segment_type: SegmentType, value: Any) -> Segment:
    """Build a segment while enforcing compatibility with the expected runtime type."""
    if value is None:
        if segment_type == SegmentType.NONE:
            return NoneSegment()
        msg = f"Type mismatch: expected {segment_type}, but got None"
        raise TypeMismatchError(msg)

    if isinstance(value, list) and len(value) == 0:
        empty_segment = _build_empty_array_segment(
            segment_type=segment_type,
            value=value,
        )
        if empty_segment is not None:
            return empty_segment
        msg = f"Type mismatch: expected {segment_type}, but got empty list"
        raise TypeMismatchError(msg)

    inferred_type = SegmentType.infer_segment_type(value)
    if inferred_type is None:
        msg = (
            f"Type mismatch: expected {segment_type}, but got python object, "
            f"type={type(value)}, value={value}"
        )
        raise TypeMismatchError(msg)

    segment_class = _resolve_segment_class_for_type_match(
        segment_type=segment_type,
        inferred_type=inferred_type,
    )
    if segment_class is not None:
        value_type = (
            inferred_type if segment_type == SegmentType.NUMBER else segment_type
        )
        return segment_class(value_type=value_type, value=value)
    msg = (
        f"Type mismatch: expected {segment_type}, but got {inferred_type}, "
        f"value={value}"
    )
    raise TypeMismatchError(msg)


def segment_to_variable(
    *,
    segment: Segment,
    selector: Sequence[str],
    variable_id: str | None = None,
    name: str | None = None,
    description: str = "",
) -> VariableBase:
    """Convert a runtime segment into a runtime variable for storage in the pool."""
    if isinstance(segment, VariableBase):
        return segment
    name = name or selector[-1]
    resolved_variable_id = variable_id or str(uuid4())

    segment_type = type(segment)
    if segment_type not in SEGMENT_TO_VARIABLE_MAP:
        msg = f"not supported segment type {segment_type}"
        raise UnsupportedSegmentTypeError(msg)

    variable_class = SEGMENT_TO_VARIABLE_MAP[segment_type]
    return cast(
        "VariableBase",
        variable_class(
            id=resolved_variable_id,
            name=name,
            description=description,
            value=segment.value,
            selector=list(selector),
        ),
    )
