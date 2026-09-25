import datetime
import enum
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated
from typing import Literal
from typing import Optional
from typing import Union
from unittest.mock import AsyncMock
from unittest.mock import Mock
from uuid import UUID

import pytest
from amgi_types import MessageScope
from asyncfast import AsyncFast
from asyncfast import InvalidChannelDefinitionError
from asyncfast import Message
from asyncfast_avro import AvroPayload
from asyncfast_avro._avro import avro_dumps
from asyncfast_avro._avro import avro_loads
from asyncfast_avro._avro import avro_schema
from asyncfast_avro._avro import GenerateAvroSchema
from pydantic import BaseModel
from pydantic import TypeAdapter
from pydantic import WithJsonSchema
from tests_asyncfast_avro.conftest import MessageBenchmark


class Colour(enum.Enum):
    RED = "red"
    GREEN = "green"


class Item(BaseModel):
    sku: str
    qty: int = 1


class Order(BaseModel):
    id: str
    items: list[Item]


@dataclass
class Node:
    name: str
    child: Optional["Node"] = None


def message_scope(payload: bytes) -> MessageScope:
    return {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [],
        "payload": payload,
    }


def test_avro_schema_model() -> None:
    assert avro_schema(TypeAdapter(Order)) == {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Item",
                        "fields": [
                            {"name": "sku", "type": "string"},
                            {"name": "qty", "type": "long", "default": 1},
                        ],
                    },
                },
            },
        ],
    }


def test_avro_schema_builtin() -> None:
    assert avro_schema(TypeAdapter(int)) == "long"


def test_avro_schema_list() -> None:
    assert avro_schema(TypeAdapter(list[str])) == {"type": "array", "items": "string"}


def test_avro_schema_dataclass() -> None:
    @dataclass
    class Basket:
        total: float

    assert avro_schema(TypeAdapter(Basket)) == {
        "type": "record",
        "name": "Basket",
        "fields": [{"name": "total", "type": "double"}],
    }


def test_avro_schema_optional() -> None:
    class Nullable(BaseModel):
        name: Optional[str] = None

    assert avro_schema(TypeAdapter(Nullable)) == {
        "type": "record",
        "name": "Nullable",
        "fields": [{"name": "name", "type": ["null", "string"], "default": None}],
    }


def test_avro_schema_union() -> None:
    assert avro_schema(TypeAdapter(Union[int, str])) == ["long", "string"]


def test_avro_schema_logical_types() -> None:
    class Times(BaseModel):
        id: UUID
        when: datetime.datetime
        day: datetime.date
        at: datetime.time
        price: Decimal

    assert avro_schema(TypeAdapter(Times)) == {
        "type": "record",
        "name": "Times",
        "fields": [
            {"name": "id", "type": {"type": "string", "logicalType": "uuid"}},
            {
                "name": "when",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            },
            {"name": "day", "type": {"type": "int", "logicalType": "date"}},
            {"name": "at", "type": {"type": "long", "logicalType": "time-micros"}},
            {"name": "price", "type": "string"},
        ],
    }


def test_avro_schema_enum() -> None:
    class Painted(BaseModel):
        colour: Colour

    assert avro_schema(TypeAdapter(Painted)) == {
        "type": "record",
        "name": "Painted",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red", "green"]},
            }
        ],
    }


def test_avro_schema_map() -> None:
    assert avro_schema(TypeAdapter(dict[str, int])) == {"type": "map", "values": "long"}


def test_avro_map_non_string_keys() -> None:
    # Avro map keys are always strings, pydantic validates them back to ints
    type_adapter = TypeAdapter(dict[int, int])

    assert avro_loads(type_adapter, avro_dumps(type_adapter, {1: 2})) == {"1": 2}


def test_avro_schema_unsupported() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        avro_schema(TypeAdapter(object))


def test_avro_schema_all_of() -> None:
    # pydantic wraps schemas in single element allOf lists on some versions
    generator = GenerateAvroSchema()

    assert generator.generate({"allOf": [{"type": "string"}]}) == "string"


def test_avro_schema_named_type_reused() -> None:
    class Pair(BaseModel):
        left: Item
        right: Item

    assert avro_schema(TypeAdapter(Pair))["fields"][1]["type"] == "Item"  # type: ignore[call-overload,index]


def test_avro_round_trip() -> None:
    order = {"id": "1", "items": [{"sku": "a", "qty": 2}]}
    assert (
        avro_loads(TypeAdapter(Order), avro_dumps(TypeAdapter(Order), order)) == order
    )


async def test_avro_payload_received() -> None:
    app = AsyncFast()
    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(order: Annotated[Order, AvroPayload()]) -> None:
        test_mock(order)

    payload = avro_dumps(
        TypeAdapter(Order), {"id": "1", "items": [{"sku": "a", "qty": 2}]}
    )
    await app(message_scope(payload), AsyncMock(), AsyncMock())

    test_mock.assert_called_once_with(Order(id="1", items=[Item(sku="a", qty=2)]))


async def test_avro_payload_missing() -> None:
    app = AsyncFast()
    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(order: Annotated[Optional[Order], AvroPayload()]) -> None:
        test_mock(order)

    message: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [],
    }
    await app(message, AsyncMock(), AsyncMock())

    test_mock.assert_called_once_with(None)


def test_avro_message_sent(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        order: Annotated[Order, AvroPayload()]

    response = Response(order=Order(id="1", items=[Item(sku="a", qty=2)]))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [],
        "payload": avro_dumps(
            TypeAdapter(Order), {"id": "1", "items": [{"sku": "a", "qty": 2}]}
        ),
    }


def test_avro_message_round_trip() -> None:
    class Everything(BaseModel):
        id: UUID
        when: datetime.datetime
        day: datetime.date
        price: Decimal
        colour: Colour
        name: Optional[str] = None
        tags: dict[str, str]

    everything = Everything(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        when=datetime.datetime(2026, 8, 23, tzinfo=datetime.timezone.utc),
        day=datetime.date(2026, 8, 23),
        price=Decimal("1.50"),
        colour=Colour.RED,
        tags={"k": "v"},
    )

    @dataclass
    class Response(Message, address="response_channel"):
        everything: Annotated[Everything, AvroPayload()]

    payload = dict(Response(everything=everything))["payload"]

    assert (
        Everything.model_validate(avro_loads(TypeAdapter(Everything), payload))
        == everything
    )


def test_asyncapi_avro_payload() -> None:
    app = AsyncFast()

    @app.channel("orders")
    async def on_order(order: Annotated[Order, AvroPayload()]) -> None:
        pass  # pragma: no cover

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OnOrder": {
                "address": "orders",
                "messages": {
                    "OnOrderMessage": {"$ref": "#/components/messages/OnOrderMessage"}
                },
            }
        },
        "components": {
            "messages": {
                "OnOrderMessage": {
                    "payload": {
                        "schemaFormat": "application/vnd.apache.avro;version=1.9.0",
                        "schema": avro_schema(TypeAdapter(Order)),
                    }
                }
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnOrder": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnOrder"},
            }
        },
    }


def test_asyncapi_avro_send_message() -> None:
    app = AsyncFast()

    @dataclass
    class OrderMessage(Message, address="orders"):
        order: Annotated[Order, AvroPayload()]

    @app.channel("hello")
    async def on_hello() -> AsyncGenerator[OrderMessage, None]:
        yield OrderMessage(order=Order(id="1", items=[]))  # pragma: no cover

    assert app.asyncapi()["components"]["messages"]["OrderMessage"] == {
        "payload": {
            "schemaFormat": "application/vnd.apache.avro;version=1.9.0",
            "schema": avro_schema(TypeAdapter(Order)),
        }
    }


def test_avro_schema_enum_reused() -> None:
    class Painted(BaseModel):
        front: Colour
        back: Colour

    schema = avro_schema(TypeAdapter(Painted))

    assert schema["fields"][0]["type"] == {  # type: ignore[call-overload,index]
        "type": "enum",
        "name": "Colour",
        "symbols": ["red", "green"],
    }
    # a named type is defined once, then referenced by name
    assert schema["fields"][1]["type"] == "Colour"  # type: ignore[call-overload,index]


def test_avro_schema_recursive() -> None:
    assert avro_schema(TypeAdapter(Node)) == {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "child", "type": ["null", "Node"], "default": None},
        ],
    }


def test_avro_schema_tuple() -> None:
    assert avro_schema(TypeAdapter(tuple[int, str])) == {
        "type": "array",
        "items": ["long", "string"],
    }


def test_avro_schema_bytes() -> None:
    assert avro_schema(TypeAdapter(bytes)) == "bytes"


def test_avro_schema_int_enum() -> None:
    class Level(enum.IntEnum):
        LOW = 1
        HIGH = 2

    # Avro enum symbols must be names, so integer values fall back to the value type
    assert avro_schema(TypeAdapter(Level)) == "long"


def test_avro_round_trip_types() -> None:
    class Everything(BaseModel):
        text: str
        count: int
        ratio: float
        flag: bool
        raw: bytes
        pair: tuple[int, str]
        levels: set[int]
        elapsed: datetime.timedelta

    everything = Everything(
        text="a",
        count=1,
        ratio=1.5,
        flag=True,
        raw=b"xy",
        pair=(1, "b"),
        levels={1, 2},
        elapsed=datetime.timedelta(hours=1),
    )

    payload = avro_dumps(TypeAdapter(Everything), Everything.model_dump(everything))

    assert (
        Everything.model_validate(avro_loads(TypeAdapter(Everything), payload))
        == everything
    )


def test_avro_schema_const() -> None:
    class Fixed(BaseModel):
        colour: Literal["red"]

    assert avro_schema(TypeAdapter(Fixed)) == {
        "type": "record",
        "name": "Fixed",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red"]},
            }
        ],
    }


def test_avro_schema_const_default() -> None:
    class Fixed(BaseModel):
        colour: Literal["red"] = "red"

    assert avro_schema(TypeAdapter(Fixed)) == {
        "type": "record",
        "name": "Fixed",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red"]},
            }
        ],
    }


def test_avro_schema_untyped_collection() -> None:
    # An empty tuple has no items to describe, so cannot be encoded
    with pytest.raises(InvalidChannelDefinitionError, match="untyped collection"):
        avro_schema(TypeAdapter(tuple[()]))


def test_avro_schema_untyped_mapping() -> None:
    with pytest.raises(InvalidChannelDefinitionError, match="untyped mapping"):
        avro_schema(TypeAdapter(dict))


def test_avro_schema_unnamed_record() -> None:
    type_adapter: TypeAdapter[Annotated[int, WithJsonSchema]] = TypeAdapter(
        Annotated[
            int,
            WithJsonSchema(
                {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}},
                    "title": None,
                }
            ),
        ]
    )

    with pytest.raises(InvalidChannelDefinitionError, match="unnamed record"):
        avro_schema(type_adapter)


def test_avro_schema_enum_default() -> None:
    class Painted(BaseModel):
        colour: Colour = Colour.RED

    assert avro_schema(TypeAdapter(Painted)) == {
        "type": "record",
        "name": "Painted",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red", "green"]},
            }
        ],
    }
