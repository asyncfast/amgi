from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from asyncfast import Header
from asyncfast import Message
from asyncfast.bindings import KafkaKey
from pydantic import BaseModel


def test_message_payload() -> None:
    class Data(BaseModel):
        id: str

    @dataclass
    class Response(Message, address="response_channel"):
        data: Data

    response = Response(data=Data(id="test"))

    assert dict(response) == {
        "address": "response_channel",
        "headers": [],
        "payload": b'{"id":"test"}',
    }


def test_message_header() -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[int, Header()]

    response = Response(id=100)

    assert dict(response) == {
        "address": "response_channel",
        "headers": [(b"id", b"100")],
    }


def test_message_parameter() -> None:
    @dataclass
    class Response(Message, address="register.{user_id}"):
        user_id: str

    response = Response(user_id="ec5e9f87-c896-4fb1-b028-8352ef654e05")

    assert dict(response) == {
        "address": "register.ec5e9f87-c896-4fb1-b028-8352ef654e05",
        "headers": [],
    }


def test_message_header_string() -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[UUID, Header()]

    response = Response(id=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert dict(response) == {
        "address": "response_channel",
        "headers": [(b"id", b"ec5e9f87-c896-4fb1-b028-8352ef654e05")],
    }


def test_message_binding_kafka_key() -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[UUID, KafkaKey()]

    response = Response(key=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert dict(response) == {
        "address": "response_channel",
        "bindings": {"kafka": {"key": b"ec5e9f87-c896-4fb1-b028-8352ef654e05"}},
        "headers": [],
    }
