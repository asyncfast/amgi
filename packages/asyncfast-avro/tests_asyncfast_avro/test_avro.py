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
from asyncfast_avro._avro import _avro_dumps
from asyncfast_avro._avro import _avro_loads
from asyncfast_avro._avro import _avro_schema
from asyncfast_avro._avro import _GenerateAvroSchema
from pydantic import BaseModel
from pydantic import create_model
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
    assert _avro_schema(TypeAdapter(Order)) == {
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
    assert _avro_schema(TypeAdapter(int)) == "long"


def test_avro_schema_list() -> None:
    assert _avro_schema(TypeAdapter(list[str])) == {"type": "array", "items": "string"}


def test_avro_schema_dataclass() -> None:
    @dataclass
    class Basket:
        total: float

    assert _avro_schema(TypeAdapter(Basket)) == {
        "type": "record",
        "name": "Basket",
        "fields": [{"name": "total", "type": "double"}],
    }


def test_avro_schema_optional() -> None:
    class Nullable(BaseModel):
        name: Optional[str] = None

    assert _avro_schema(TypeAdapter(Nullable)) == {
        "type": "record",
        "name": "Nullable",
        "fields": [{"name": "name", "type": ["null", "string"], "default": None}],
    }


def test_avro_schema_union() -> None:
    assert _avro_schema(TypeAdapter(Union[int, str])) == ["long", "string"]


def test_avro_schema_logical_types() -> None:
    class Times(BaseModel):
        id: UUID
        when: datetime.datetime
        day: datetime.date
        at: datetime.time
        price: Decimal

    assert _avro_schema(TypeAdapter(Times)) == {
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

    assert _avro_schema(TypeAdapter(Painted)) == {
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
    assert _avro_schema(TypeAdapter(dict[str, int])) == {
        "type": "map",
        "values": "long",
    }


def test_avro_map_non_string_keys() -> None:
    # Avro map keys are always strings, pydantic validates them back to ints
    type_adapter = TypeAdapter(dict[int, int])

    assert _avro_loads(type_adapter, _avro_dumps(type_adapter, {1: 2})) == {"1": 2}


def test_avro_schema_unsupported() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        _avro_schema(TypeAdapter(object))


def test_avro_schema_all_of() -> None:
    # pydantic wraps schemas in single element allOf lists on some versions
    generator = _GenerateAvroSchema()

    assert generator.generate({"allOf": [{"type": "string"}]}) == "string"


def test_avro_schema_named_type_reused() -> None:
    class Pair(BaseModel):
        left: Item
        right: Item

    assert _avro_schema(TypeAdapter(Pair))["fields"][1]["type"] == "Item"  # type: ignore[call-overload,index]


def test_avro_round_trip() -> None:
    order = {"id": "1", "items": [{"sku": "a", "qty": 2}]}
    assert (
        _avro_loads(TypeAdapter(Order), _avro_dumps(TypeAdapter(Order), order)) == order
    )


async def test_avro_payload_received() -> None:
    app = AsyncFast()
    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(order: Annotated[Order, AvroPayload()]) -> None:
        test_mock(order)

    payload = _avro_dumps(
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
        "payload": _avro_dumps(
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
        Everything.model_validate(_avro_loads(TypeAdapter(Everything), payload))
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
                        "schema": _avro_schema(TypeAdapter(Order)),
                    },
                    "contentType": "application/vnd.apache.avro;version=1.9.0",
                }
            }
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
            "schema": _avro_schema(TypeAdapter(Order)),
        },
        "contentType": "application/vnd.apache.avro;version=1.9.0",
    }


def test_avro_schema_enum_reused() -> None:
    class Painted(BaseModel):
        front: Colour
        back: Colour

    schema = _avro_schema(TypeAdapter(Painted))

    assert schema["fields"][0]["type"] == {  # type: ignore[call-overload,index]
        "type": "enum",
        "name": "Colour",
        "symbols": ["red", "green"],
    }
    # a named type is defined once, then referenced by name
    assert schema["fields"][1]["type"] == "Colour"  # type: ignore[call-overload,index]


def test_avro_schema_recursive() -> None:
    assert _avro_schema(TypeAdapter(Node)) == {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "child", "type": ["null", "Node"], "default": None},
        ],
    }


def test_avro_schema_tuple() -> None:
    assert _avro_schema(TypeAdapter(tuple[int, str])) == {
        "type": "array",
        "items": ["long", "string"],
    }


def test_avro_schema_bytes() -> None:
    assert _avro_schema(TypeAdapter(bytes)) == "bytes"


def test_avro_schema_int_enum() -> None:
    class Level(enum.IntEnum):
        LOW = 1
        HIGH = 2

    # Avro enum symbols must be names, so integer values fall back to the value type
    assert _avro_schema(TypeAdapter(Level)) == "long"


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

    payload = _avro_dumps(TypeAdapter(Everything), Everything.model_dump(everything))

    assert (
        Everything.model_validate(_avro_loads(TypeAdapter(Everything), payload))
        == everything
    )


def test_avro_schema_const() -> None:
    class Fixed(BaseModel):
        colour: Literal["red"]

    assert _avro_schema(TypeAdapter(Fixed)) == {
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

    assert _avro_schema(TypeAdapter(Fixed)) == {
        "type": "record",
        "name": "Fixed",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red"]},
                "default": "red",
            }
        ],
    }


def test_avro_schema_untyped_collection() -> None:
    # An empty tuple has no items to describe, so cannot be encoded
    with pytest.raises(InvalidChannelDefinitionError, match="untyped collection"):
        _avro_schema(TypeAdapter(tuple[()]))


def test_avro_schema_untyped_mapping() -> None:
    with pytest.raises(InvalidChannelDefinitionError, match="untyped mapping"):
        _avro_schema(TypeAdapter(dict))


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
        _avro_schema(type_adapter)


def test_avro_schema_enum_default() -> None:
    class Painted(BaseModel):
        colour: Colour = Colour.RED

    assert _avro_schema(TypeAdapter(Painted)) == {
        "type": "record",
        "name": "Painted",
        "fields": [
            {
                "name": "colour",
                "type": {"type": "enum", "name": "Colour", "symbols": ["red", "green"]},
                "default": "red",
            }
        ],
    }


def test_avro_schema_union_default() -> None:
    # the union is reordered so the default validates against its first branch
    class Discounted(BaseModel):
        rate: Optional[int] = 5

    assert _avro_schema(TypeAdapter(Discounted)) == {
        "type": "record",
        "name": "Discounted",
        "fields": [{"name": "rate", "type": ["long", "null"], "default": 5}],
    }


def test_avro_schema_enum_name_collision() -> None:
    class Front(BaseModel):
        colour: Literal["red"]

    class Back(BaseModel):
        colour: Literal["blue"]

    class Car(BaseModel):
        front: Front
        back: Back

    # both fields produce an enum named Colour, so publishing the schema would silently
    # encode one of them with the other's symbols
    with pytest.raises(InvalidChannelDefinitionError, match="Colour"):
        _avro_schema(TypeAdapter(Car))


def test_avro_schema_record_name_collision() -> None:
    class Pair(BaseModel):
        first: Item
        second: create_model("Item", qty=(bool, ...))  # type: ignore[valid-type]

    # the second Item is named the same as the module level Item, but has different fields
    with pytest.raises(InvalidChannelDefinitionError, match="Item"):
        _avro_schema(TypeAdapter(Pair))


def test_avro_schema_identical_definitions_deduped() -> None:
    def item_model() -> type[BaseModel]:
        return create_model("Item", sku=(str, ...), qty=(int, 1))

    class Pair(BaseModel):
        first: item_model()  # type: ignore[valid-type]
        second: item_model()  # type: ignore[valid-type]

    # two definitions with the same name and fields are defined once, then referenced
    assert _avro_schema(TypeAdapter(Pair))["fields"][1]["type"] == "Item"  # type: ignore[call-overload,index]


def test_avro_schema_nested_record_reused() -> None:
    # Outer is reused after the Item defined within it has been named
    def outer_model() -> type[BaseModel]:
        return create_model("Outer", item=(Item, ...))

    class Pair(BaseModel):
        first: outer_model()  # type: ignore[valid-type]
        second: outer_model()  # type: ignore[valid-type]

    schema = _avro_schema(TypeAdapter(Pair))

    assert schema["fields"][0]["type"]["name"] == "Outer"  # type: ignore[call-overload,index]
    assert schema["fields"][1]["type"] == "Outer"  # type: ignore[call-overload,index]


def test_avro_schema_enum_without_encodable_values() -> None:
    class Pair(enum.Enum):
        LOW = (1, 2)
        HIGH = (3, 4)

    with pytest.raises(InvalidChannelDefinitionError, match="Pair"):
        _avro_schema(TypeAdapter(Pair))


def test_avro_schema_boolean_enum_is_not_symbols() -> None:
    class Flag(enum.Enum):
        YES = True
        NO = False

    type_adapter = TypeAdapter(Flag)

    assert _avro_schema(type_adapter) == "boolean"
    assert _avro_loads(type_adapter, _avro_dumps(type_adapter, Flag.YES)) is True


def test_avro_schema_enum_with_unsupported_value() -> None:
    class Mixed(enum.Enum):
        NAME = "name"
        PAIR = (1, 2)

    with pytest.raises(InvalidChannelDefinitionError, match="Mixed"):
        _avro_schema(TypeAdapter(Mixed))


def test_avro_schema_repeated_enum_with_default() -> None:
    class Shade(BaseModel):
        first: Colour = Colour.RED
        second: Colour = Colour.RED

    type_adapter = TypeAdapter(Shade)
    schema = _avro_schema(type_adapter)

    assert schema["fields"][1]["type"] == "Colour"  # type: ignore[call-overload,index]
    assert schema["fields"][1]["default"] == schema["fields"][0]["default"]  # type: ignore[call-overload,index]
    payload = _avro_dumps(type_adapter, type_adapter.dump_python(Shade()))
    assert _avro_loads(type_adapter, payload) == {
        "first": Colour.RED.value,
        "second": Colour.RED.value,
    }


def test_avro_round_trip_naive_datetime() -> None:
    # naive datetimes are encoded as UTC, so they produce the same payload as their UTC
    # equivalent wherever they are encoded
    type_adapter = TypeAdapter(datetime.datetime)
    naive = datetime.datetime(2026, 8, 23, 12, 0)
    utc = naive.replace(tzinfo=datetime.timezone.utc)

    assert _avro_dumps(type_adapter, naive) == _avro_dumps(type_adapter, utc)
    assert _avro_loads(type_adapter, _avro_dumps(type_adapter, naive)) == utc


def test_avro_round_trip_nested_naive_datetime() -> None:
    class Schedule(BaseModel):
        starts: list[datetime.datetime]
        by_name: dict[str, datetime.datetime]

    type_adapter = TypeAdapter(Schedule)
    naive = datetime.datetime(2026, 8, 23, 12, 0)
    utc = naive.replace(tzinfo=datetime.timezone.utc)
    value = {"starts": [naive], "by_name": {"first": naive}}

    payload = _avro_dumps(type_adapter, value)

    assert _avro_loads(type_adapter, payload) == {
        "starts": [utc],
        "by_name": {"first": utc},
    }


def test_avro_load_empty_payload() -> None:
    with pytest.raises(ValueError, match="Invalid Avro payload"):
        _avro_loads(TypeAdapter(Order), b"")


def test_avro_load_truncated_payload() -> None:
    type_adapter = TypeAdapter(Order)
    payload = _avro_dumps(type_adapter, {"id": "1", "items": [{"sku": "a", "qty": 2}]})

    with pytest.raises(ValueError, match="Invalid Avro payload"):
        _avro_loads(type_adapter, payload[: len(payload) // 2])


def test_avro_load_trailing_bytes() -> None:
    type_adapter = TypeAdapter(Order)
    payload = _avro_dumps(type_adapter, {"id": "1", "items": [{"sku": "a", "qty": 2}]})

    with pytest.raises(ValueError, match="Invalid Avro payload"):
        _avro_loads(type_adapter, payload + b"trailing")


def test_avro_dump_invalid_value() -> None:
    with pytest.raises(ValueError, match="Invalid Avro value"):
        _avro_dumps(TypeAdapter(Colour), "blue")


def test_asyncapi_json_payload_has_no_content_type() -> None:
    app = AsyncFast()

    @app.channel("orders")
    async def on_order(order: Order) -> None:
        pass  # pragma: no cover

    message = app.asyncapi()["components"]["messages"]["OnOrderMessage"]

    assert "contentType" not in message
