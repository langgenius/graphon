from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Discriminator, Tag, field_validator

from graphon.file.models import File

from .types import SegmentType

SHORT_TOKEN_MAX_LENGTH = 8


def _obfuscated_token(token: str) -> str:
    if not token:
        return token
    if len(token) <= SHORT_TOKEN_MAX_LENGTH:
        return "*" * 20
    return token[:6] + "*" * 12 + token[-2:]


class Segment(BaseModel):
    """Segment is runtime type used during the execution of workflow.

    Note: this class is abstract, you should use subclasses of this class instead.
    """

    model_config = ConfigDict(frozen=True)

    value_type: SegmentType
    value: Any

    @field_validator("value_type")
    @classmethod
    def validate_value_type(cls, value: SegmentType) -> SegmentType:
        """Validate that the provided value is equal to the default value
        of the `'value_type'` field. If the value is different, a ValueError is
        raised.

        Returns:
            The validated `value_type` when it matches the segment class default.

        Raises:
            ValueError: If `value` differs from the segment class default `value_type`.

        """
        if value != cls.model_fields["value_type"].default:
            msg = "Cannot modify 'value_type'"
            raise ValueError(msg)
        return value

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
    value_type: SegmentType = SegmentType.NONE
    value: None = None

    def to_segment(self) -> NoneSegment:
        return NoneSegment()

    @property
    def text(self) -> str:
        return ""

    @property
    def log(self) -> str:
        return ""

    @property
    def markdown(self) -> str:
        return ""


class StringSegment(Segment):
    value_type: SegmentType = SegmentType.STRING
    value: str

    def to_segment(self) -> StringSegment:
        return StringSegment(value=self.value)


class SecretSegment(StringSegment):
    value_type: SegmentType = SegmentType.SECRET

    def to_segment(self) -> SecretSegment:
        return SecretSegment(value=self.value)

    @property
    def log(self) -> str:
        return _obfuscated_token(self.value)


class FloatSegment(Segment):
    value_type: SegmentType = SegmentType.FLOAT
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
    value_type: SegmentType = SegmentType.INTEGER
    value: int

    def to_segment(self) -> IntegerSegment:
        return IntegerSegment(value=self.value)


class ObjectSegment(Segment):
    value_type: SegmentType = SegmentType.OBJECT
    value: Mapping[str, Any]

    def to_segment(self) -> ObjectSegment:
        return ObjectSegment(value=self.value)

    @property
    def text(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False)

    @property
    def log(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False, indent=2)

    @property
    def markdown(self) -> str:
        return json.dumps(self.model_dump()["value"], ensure_ascii=False, indent=2)


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
    value_type: SegmentType = SegmentType.FILE
    value: File

    def to_segment(self) -> FileSegment:
        return FileSegment(value=self.value)

    @property
    def markdown(self) -> str:
        return self.value.markdown

    @property
    def log(self) -> str:
        return ""

    @property
    def text(self) -> str:
        return ""


class BooleanSegment(Segment):
    value_type: SegmentType = SegmentType.BOOLEAN
    value: bool

    def to_segment(self) -> BooleanSegment:
        return BooleanSegment(value=self.value)


class ArrayAnySegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_ANY
    value: Sequence[Any]

    def to_segment(self) -> ArrayAnySegment:
        return ArrayAnySegment(value=self.value)


class ArrayStringSegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_STRING
    value: Sequence[str]

    def to_segment(self) -> ArrayStringSegment:
        return ArrayStringSegment(value=self.value)

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.value, ensure_ascii=False)


class ArrayNumberSegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_NUMBER
    value: Sequence[float | int]

    def to_segment(self) -> ArrayNumberSegment:
        return ArrayNumberSegment(value=self.value)


class ArrayObjectSegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_OBJECT
    value: Sequence[Mapping[str, Any]]

    def to_segment(self) -> ArrayObjectSegment:
        return ArrayObjectSegment(value=self.value)

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.value, ensure_ascii=False)


class ArrayFileSegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_FILE
    value: Sequence[File]

    def to_segment(self) -> ArrayFileSegment:
        return ArrayFileSegment(value=self.value)

    @property
    def markdown(self) -> str:
        return "\n".join([item.markdown for item in self.value])

    @property
    def log(self) -> str:
        return ""

    @property
    def text(self) -> str:
        return ""


class ArrayBooleanSegment(ArraySegment):
    value_type: SegmentType = SegmentType.ARRAY_BOOLEAN
    value: Sequence[bool]

    def to_segment(self) -> ArrayBooleanSegment:
        return ArrayBooleanSegment(value=self.value)


def get_segment_discriminator(v: Any) -> SegmentType | None:
    if isinstance(v, Segment):
        return v.value_type
    if isinstance(v, dict):
        value_type = v.get("value_type")
        if value_type is None:
            return None
        try:
            seg_type = SegmentType(value_type)
        except ValueError:
            return None
        return seg_type
    # return None if the discriminator value isn't found
    return None


type SerializableSegment = Annotated[
    (
        Annotated[NoneSegment, Tag(SegmentType.NONE)]
        | Annotated[StringSegment, Tag(SegmentType.STRING)]
        | Annotated[SecretSegment, Tag(SegmentType.SECRET)]
        | Annotated[FloatSegment, Tag(SegmentType.FLOAT)]
        | Annotated[IntegerSegment, Tag(SegmentType.INTEGER)]
        | Annotated[ObjectSegment, Tag(SegmentType.OBJECT)]
        | Annotated[FileSegment, Tag(SegmentType.FILE)]
        | Annotated[BooleanSegment, Tag(SegmentType.BOOLEAN)]
        | Annotated[ArrayAnySegment, Tag(SegmentType.ARRAY_ANY)]
        | Annotated[ArrayStringSegment, Tag(SegmentType.ARRAY_STRING)]
        | Annotated[ArrayNumberSegment, Tag(SegmentType.ARRAY_NUMBER)]
        | Annotated[ArrayObjectSegment, Tag(SegmentType.ARRAY_OBJECT)]
        | Annotated[ArrayFileSegment, Tag(SegmentType.ARRAY_FILE)]
        | Annotated[ArrayBooleanSegment, Tag(SegmentType.ARRAY_BOOLEAN)]
    ),
    Discriminator(get_segment_discriminator),
]
