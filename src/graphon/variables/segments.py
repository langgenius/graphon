from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from graphon.file.constants import maybe_file_object
from graphon.file.models import File

from .types import SegmentType

SHORT_TOKEN_MAX_LENGTH = 8


def _mask_token(token: str) -> str:
    if not token:
        return token
    if len(token) <= SHORT_TOKEN_MAX_LENGTH:
        return "*" * 20
    return token[:6] + "*" * 12 + token[-2:]


def _restore_files(value: Any) -> Any:
    if maybe_file_object(value):
        return File.model_validate(value)
    if isinstance(value, (list, tuple)):
        return type(value)(_restore_files(item) for item in value)
    if isinstance(value, dict):
        return {key: _restore_files(item) for key, item in value.items()}
    return value


class Segment(BaseModel):
    """Segment is runtime type used during the execution of workflow.

    Note: this class is abstract, you should use subclasses of this class instead.
    """

    model_config = ConfigDict(frozen=True)

    value_type: SegmentType
    value: Any

    @property
    def text(self) -> str:
        return str(self.value)

    @property
    def log(self) -> str:
        return str(self.value)

    @property
    def markdown(self) -> str:
        return str(self.value)

    @property
    def size(self) -> int:
        """Return the size of the value in bytes."""
        return sys.getsizeof(self.value)

    def to_object(self) -> object:
        return self.value

    def to_segment(self) -> SerializableSegment:
        """Preserve the declared type and value without variable metadata."""
        msg = f"Unsupported segment type: {type(self).__name__}"
        raise TypeError(msg)


class NoneSegment(Segment):
    value_type: Literal[SegmentType.NONE] = SegmentType.NONE
    value: None = None

    @property
    def text(self) -> str:
        return ""

    @property
    def log(self) -> str:
        return ""

    @property
    def markdown(self) -> str:
        return ""

    def to_segment(self) -> NoneSegment:
        return NoneSegment()


class StringSegment(Segment):
    value_type: Literal[SegmentType.STRING] = SegmentType.STRING
    value: str

    def to_segment(self) -> StringSegment:
        return StringSegment(value=self.value)


class SecretSegment(Segment):
    value_type: Literal[SegmentType.SECRET] = SegmentType.SECRET
    value: str

    @property
    def log(self) -> str:
        return _mask_token(self.value)

    def to_segment(self) -> SecretSegment:
        return SecretSegment(value=self.value)


class FloatSegment(Segment):
    value_type: Literal[SegmentType.FLOAT] = SegmentType.FLOAT
    value: float
    # NOTE(QuantumGhost): seems that the equality for FloatSegment with `NaN`
    # value has some problems.
    # The following tests cannot pass.
    #
    #     def test_float_segment_and_nan():
    #         nan = float("nan")
    #         assert nan != nan
    #
    #         f1 = FloatSegment(value=float("nan"))
    #         f2 = FloatSegment(value=float("nan"))
    #         assert f1 != f2
    #
    #         f3 = FloatSegment(value=nan)
    #         f4 = FloatSegment(value=nan)
    #         assert f3 != f4

    def to_segment(self) -> FloatSegment:
        return FloatSegment(value=self.value)


class IntegerSegment(Segment):
    value_type: Literal[SegmentType.INTEGER] = SegmentType.INTEGER
    value: int

    def to_segment(self) -> IntegerSegment:
        return IntegerSegment(value=self.value)


class ObjectSegment(Segment):
    value_type: Literal[SegmentType.OBJECT] = SegmentType.OBJECT
    value: Mapping[str, Any]

    @property
    def text(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False)

    @property
    def log(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False, indent=2)

    @property
    def markdown(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False, indent=2)

    def to_segment(self) -> ObjectSegment:
        return ObjectSegment(value=self.value)


class ArraySegment(Segment):
    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return super().text

    @property
    def markdown(self) -> str:
        return "\n".join([f"- {item}" for item in self.value])


class FileSegment(Segment):
    value_type: Literal[SegmentType.FILE] = SegmentType.FILE
    value: File

    @property
    def markdown(self) -> str:
        return self.value.markdown

    @property
    def log(self) -> str:
        return ""

    @property
    def text(self) -> str:
        return ""

    def to_segment(self) -> FileSegment:
        return FileSegment(value=self.value)


class BooleanSegment(Segment):
    value_type: Literal[SegmentType.BOOLEAN] = SegmentType.BOOLEAN
    value: bool

    def to_segment(self) -> BooleanSegment:
        return BooleanSegment(value=self.value)


class ArrayAnySegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_ANY] = SegmentType.ARRAY_ANY
    value: Annotated[Sequence[Any], AfterValidator(_restore_files)]

    def to_segment(self) -> ArrayAnySegment:
        return ArrayAnySegment(value=self.value)


class ArrayStringSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_STRING] = SegmentType.ARRAY_STRING
    value: Sequence[str]

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.value, ensure_ascii=False)

    def to_segment(self) -> ArrayStringSegment:
        return ArrayStringSegment(value=self.value)


class ArrayNumberSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_NUMBER] = SegmentType.ARRAY_NUMBER
    value: Sequence[float | int]

    def to_segment(self) -> ArrayNumberSegment:
        return ArrayNumberSegment(value=self.value)


class ArrayObjectSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_OBJECT] = SegmentType.ARRAY_OBJECT
    value: Sequence[Mapping[str, Any]]

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.model_dump()["value"], ensure_ascii=False)

    def to_segment(self) -> ArrayObjectSegment:
        return ArrayObjectSegment(value=self.value)


class ArrayFileSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_FILE] = SegmentType.ARRAY_FILE
    value: Sequence[File]

    @property
    def markdown(self) -> str:
        return "\n".join([item.markdown for item in self.value])

    @property
    def log(self) -> str:
        return ""

    @property
    def text(self) -> str:
        return ""

    def to_segment(self) -> ArrayFileSegment:
        return ArrayFileSegment(value=self.value)


class ArrayBooleanSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_BOOLEAN] = SegmentType.ARRAY_BOOLEAN
    value: Sequence[bool]

    def to_segment(self) -> ArrayBooleanSegment:
        return ArrayBooleanSegment(value=self.value)


type SerializableSegment = Annotated[
    (
        NoneSegment
        | StringSegment
        | SecretSegment
        | FloatSegment
        | IntegerSegment
        | ObjectSegment
        | FileSegment
        | BooleanSegment
        | ArrayAnySegment
        | ArrayStringSegment
        | ArrayNumberSegment
        | ArrayObjectSegment
        | ArrayFileSegment
        | ArrayBooleanSegment
    ),
    Field(discriminator="value_type"),
]
