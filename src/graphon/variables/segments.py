import json
import sys
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphon.file.models import File

from .types import SegmentType

SHORT_TOKEN_MAX_LENGTH = 8


def _mask_token(token: str) -> str:
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


class NoneSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

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


class StringSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

    value_type: Literal[SegmentType.STRING] = SegmentType.STRING
    value: str


class SecretSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

    value_type: Literal[SegmentType.SECRET] = SegmentType.SECRET
    value: str

    @property
    def log(self) -> str:
        return _mask_token(self.value)


class FloatSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

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


class IntegerSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

    value_type: Literal[SegmentType.INTEGER] = SegmentType.INTEGER
    value: int


class ObjectSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

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


class ArraySegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

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
    model_config = ConfigDict(revalidate_instances="always")

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


class BooleanSegment(Segment):
    model_config = ConfigDict(revalidate_instances="always")

    value_type: Literal[SegmentType.BOOLEAN] = SegmentType.BOOLEAN
    value: bool


class ArrayAnySegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_ANY] = SegmentType.ARRAY_ANY
    value: Sequence[Any]


class ArrayStringSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_STRING] = SegmentType.ARRAY_STRING
    value: Sequence[str]

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.value, ensure_ascii=False)


class ArrayNumberSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_NUMBER] = SegmentType.ARRAY_NUMBER
    value: Sequence[float | int]


class ArrayObjectSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_OBJECT] = SegmentType.ARRAY_OBJECT
    value: Sequence[Mapping[str, Any]]

    @property
    def text(self) -> str:
        # Return empty string for empty arrays instead of "[]"
        if not self.value:
            return ""
        return json.dumps(self.value, ensure_ascii=False)


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


class ArrayBooleanSegment(ArraySegment):
    value_type: Literal[SegmentType.ARRAY_BOOLEAN] = SegmentType.ARRAY_BOOLEAN
    value: Sequence[bool]


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
