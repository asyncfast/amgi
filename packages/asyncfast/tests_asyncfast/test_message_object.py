from dataclasses import dataclass
from typing import Annotated
from typing import Any
from typing import Callable
from typing import cast
from uuid import UUID

import pytest
from asyncfast import Header
from asyncfast import Message
from asyncfast.bindings import KafkaKey
from pydantic import BaseModel
from pytest_benchmark.fixture import BenchmarkFixture

MessageBenchmark = Callable[[Message], dict[str, Any]]


@pytest.fixture
def message_benchmark(benchmark: BenchmarkFixture) -> MessageBenchmark:
    def _message_benchmark(
        message: Message,
    ) -> dict[str, Any]:
        result = benchmark(lambda: dict(message))
        return cast(dict[str, Any], result)

    return _message_benchmark


def test_message_payload(message_benchmark: MessageBenchmark) -> None:
    class Data(BaseModel):
        id: str

    @dataclass
    class Response(Message, address="response_channel"):
        data: Data

    response = Response(data=Data(id="test"))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [],
        "payload": b'{"id":"test"}',
    }


def test_message_header(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[int, Header()]

    response = Response(id=100)

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [(b"id", b"100")],
    }


def test_message_header_alias(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[int, Header(alias="Id")]

    response = Response(id=100)

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [(b"Id", b"100")],
    }


def test_message_header_underscore(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        request_id: Annotated[str, Header()]

    response = Response(request_id="46345148-fafe-11f0-af7a-975c1791ef56")

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [(b"request-id", b"46345148-fafe-11f0-af7a-975c1791ef56")],
    }


def test_message_parameter(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="register.{user_id}"):
        user_id: str

    response = Response(user_id="ec5e9f87-c896-4fb1-b028-8352ef654e05")

    assert message_benchmark(response) == {
        "address": "register.ec5e9f87-c896-4fb1-b028-8352ef654e05",
        "headers": [],
    }


def test_message_header_string(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[UUID, Header()]

    response = Response(id=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [(b"id", b"ec5e9f87-c896-4fb1-b028-8352ef654e05")],
    }


def test_message_header_bytes(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        id: Annotated[bytes, Header()]

    response = Response(id=b"1234")

    assert message_benchmark(response) == {
        "address": "response_channel",
        "headers": [(b"id", b"1234")],
    }


def test_message_binding_kafka_key(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[UUID, KafkaKey()]

    response = Response(key=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {"kafka": {"key": b"ec5e9f87-c896-4fb1-b028-8352ef654e05"}},
        "headers": [],
    }


def test_message_unsupported_key() -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[UUID, KafkaKey()]

    response = Response(key=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    with pytest.raises(KeyError):
        response["other_key"]


def test_message_no_address(message_benchmark: MessageBenchmark) -> None:
    class Data(BaseModel):
        id: str

    @dataclass
    class Response(Message):
        data: Data

    response = Response(data=Data(id="test"))

    assert message_benchmark(response) == {
        "address": None,
        "headers": [],
        "payload": b'{"id":"test"}',
    }


def test_message_len() -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[UUID, KafkaKey()]

    response = Response(key=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert len(response) == 3
