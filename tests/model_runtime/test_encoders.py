import dataclasses
import datetime
from collections import deque
from collections.abc import Iterator
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from pathlib import Path, PurePath
from re import compile as re_compile
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel, ConfigDict
from pydantic.networks import AnyUrl, NameEmail
from pydantic.types import SecretBytes, SecretStr
from pydantic_core import Url
from pydantic_extra_types.color import Color

from graphon.model_runtime.utils.encoders import (
    decimal_encoder,
    generate_encoders_by_class_tuples,
    isoformat,
    jsonable_encoder,
    model_dump,
)


class MockEnum(Enum):
    A = "a"
    B = "b"


class MockPydanticModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str
    age: int


@dataclasses.dataclass
class MockDataclass:
    name: str
    value: Any


class MockWithDict:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return iter(self.data.items())


class MockWithVars:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestEncoders:
    def test_model_dump(self):
        model = MockPydanticModel(name="test", age=20)
        result = model_dump(model)
        assert result == {"name": "test", "age": 20}

    def test_isoformat(self):
        d = datetime.date(2023, 1, 1)
        assert isoformat(d) == "2023-01-01"
        t = datetime.time(12, 0, 0)
        assert isoformat(t) == "12:00:00"

    def test_decimal_encoder(self):
        assert decimal_encoder(Decimal("1.0")) == pytest.approx(1.0)
        assert decimal_encoder(Decimal(1)) == 1
        assert decimal_encoder(Decimal("1.5")) == pytest.approx(1.5)
        assert decimal_encoder(Decimal(0)) == 0
        assert decimal_encoder(Decimal(-1)) == -1

    def test_generate_encoders_by_class_tuples(self):
        type_map = {int: str, float: str, str: int}
        result = generate_encoders_by_class_tuples(type_map)
        assert result[str] == (int, float)
        assert result[int] == (str,)

    def test_jsonable_encoder_basic_types(self):
        assert jsonable_encoder("string") == "string"
        assert jsonable_encoder(123) == 123
        assert jsonable_encoder(1.23) == pytest.approx(1.23)
        assert jsonable_encoder(None) is None

    def test_jsonable_encoder_pydantic(self):
        model = MockPydanticModel(name="test", age=20)
        assert jsonable_encoder(model) == {"name": "test", "age": 20}

    def test_jsonable_encoder_pydantic_root(self):
        model = MagicMock(spec=BaseModel)
        model.model_dump.return_value = {"__root__": [1, 2, 3]}
        assert jsonable_encoder(model) == [1, 2, 3]

    def test_jsonable_encoder_dataclass(self):
        obj = MockDataclass(name="test", value=1)
        assert jsonable_encoder(obj) == {"name": "test", "value": 1}
        with pytest.raises(ValueError, match="__dict__ attribute"):
            jsonable_encoder(MockDataclass)

    def test_jsonable_encoder_enum(self):
        assert jsonable_encoder(MockEnum.A) == "a"

    def test_jsonable_encoder_path(self):
        assert jsonable_encoder(Path("/tmp/test")) == "/tmp/test"
        assert jsonable_encoder(PurePath("/tmp/test")) == "/tmp/test"

    def test_jsonable_encoder_decimal(self):
        assert jsonable_encoder(Decimal("1.23")) == "1.23"
        assert jsonable_encoder(Decimal("1.000")) == "1.000"

    def test_jsonable_encoder_dict(self):
        d = {"a": 1, "b": [2, 3], "_private": "hidden"}
        assert jsonable_encoder(d) == {"a": 1, "b": [2, 3], "_private": "hidden"}
        assert jsonable_encoder(d, excluded_key_prefixes=("_",)) == {
            "a": 1,
            "b": [2, 3],
        }

        d_with_none = {"a": 1, "b": None}
        assert jsonable_encoder(d_with_none, exclude_none=True) == {"a": 1}
        assert jsonable_encoder(d_with_none, exclude_none=False) == {"a": 1, "b": None}

    def test_jsonable_encoder_collections(self):
        assert jsonable_encoder([1, 2]) == [1, 2]
        assert jsonable_encoder((1, 2)) == [1, 2]
        assert jsonable_encoder({1, 2}) == [1, 2]
        assert jsonable_encoder(frozenset([1, 2])) == [1, 2]
        assert jsonable_encoder(deque([1, 2])) == [1, 2]

        def gen() -> Iterator[int]:
            yield 1
            yield 2

        assert jsonable_encoder(gen()) == [1, 2]

    def test_jsonable_encoder_custom_encoder(self):
        custom = {int: lambda x: str(x + 1)}
        assert jsonable_encoder(1, custom_encoder=custom) == "2"

        class SubInt(int):
            pass

        assert jsonable_encoder(SubInt(1), custom_encoder=custom) == "2"

    def test_jsonable_encoder_special_types(self):
        assert jsonable_encoder(b"bytes") == "bytes"
        assert jsonable_encoder(Color("red")) == "red"

        dt = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        assert jsonable_encoder(dt) == dt.isoformat()

        date = datetime.date(2023, 1, 1)
        assert jsonable_encoder(date) == date.isoformat()

        time = datetime.time(12, 0, 0)
        assert jsonable_encoder(time) == time.isoformat()

        td = datetime.timedelta(seconds=60)
        assert jsonable_encoder(td) == pytest.approx(60.0)

        assert jsonable_encoder(IPv4Address("127.0.0.1")) == "127.0.0.1"
        assert jsonable_encoder(IPv4Interface("127.0.0.1/24")) == "127.0.0.1/24"
        assert jsonable_encoder(IPv4Network("127.0.0.0/24")) == "127.0.0.0/24"
        assert jsonable_encoder(IPv6Address("::1")) == "::1"
        assert jsonable_encoder(IPv6Interface("::1/128")) == "::1/128"
        assert jsonable_encoder(IPv6Network("::/128")) == "::/128"

        assert (
            jsonable_encoder(NameEmail(name="test", email="test@example.com"))
            == "test <test@example.com>"
        )

        assert jsonable_encoder(re_compile(r"abc")) == "abc"

        res_bytes = jsonable_encoder(SecretBytes(b"secret"))
        assert "**********" in res_bytes

        res_str = jsonable_encoder(SecretStr("secret"))
        assert res_str == "**********"

        u = UUID("12345678-1234-5678-1234-567812345678")
        assert jsonable_encoder(u) == str(u)

        url = AnyUrl("https://example.com")
        assert jsonable_encoder(url) == "https://example.com/"

        purl = Url("https://example.com")
        assert jsonable_encoder(purl) == "https://example.com/"

    def test_jsonable_encoder_fallback(self):
        obj_dict = MockWithDict({"a": 1})
        assert jsonable_encoder(obj_dict) == {"a": 1}

        obj_vars = MockWithVars(x=10, y=20)
        assert jsonable_encoder(obj_vars) == {"x": 10, "y": 20}

        class ReallyUnserializable:
            __slots__ = ["__weakref__"]

            def __iter__(self) -> Iterator[Any]:
                msg = "not iterable"
                raise TypeError(msg)

        with pytest.raises(ValueError, match="not iterable") as exc:
            jsonable_encoder(ReallyUnserializable())
        assert "not iterable" in str(exc.value)

    def test_jsonable_encoder_nested(self):
        obj = {
            "model": MockPydanticModel(name="test", age=20),
            "items": [MockEnum.A, Decimal("1.23")],
        }
        assert jsonable_encoder(obj) == {
            "model": {"name": "test", "age": 20},
            "items": ["a", "1.23"],
        }
