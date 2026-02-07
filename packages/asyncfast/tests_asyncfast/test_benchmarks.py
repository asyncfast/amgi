import asyncio
from dataclasses import dataclass
from typing import Annotated
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Optional
from typing import Sequence
from uuid import UUID

import pytest
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import MessageSender
from asyncfast import Payload
from asyncfast.bindings import KafkaKey
from pydantic import BaseModel
from pytest_benchmark.fixture import BenchmarkFixture

AppBenchmark = Callable[
    [AMGIApplication, MessageScope, MessageReceiveEvent],
    None,
]


@pytest.fixture
def app_benchmark(benchmark: BenchmarkFixture) -> AppBenchmark:
    async def _send(message: AMGISendEvent) -> None:
        pass

    def _app_benchmark(
        app: AMGIApplication,
        message_scope: MessageScope,
        message_receive_event: MessageReceiveEvent,
    ) -> None:
        loop = asyncio.new_event_loop()

        async def _receive() -> MessageReceiveEvent:
            return message_receive_event

        benchmark(
            lambda: loop.run_until_complete(
                app(
                    message_scope,
                    _receive,
                    _send,
                )
            )
        )

    return _app_benchmark


def test_message_payload(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "payload": b'{"id":1}',
        },
    )


def test_message_payload_optional(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload | None) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_payload_sync(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    @app.channel("topic")
    def topic_handler(payload: MessagePayload) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "payload": b'{"id":1}',
        },
    )


def test_message_header_string(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(etag: Annotated[str, Header(alias="ETag")]) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [(b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
        },
    )


def test_message_header_integer(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(id: Annotated[int, Header()]) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [(b"id", b"10")],
        },
    )


def test_message_header_underscore_to_hyphen(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(
        idempotency_key: Annotated[UUID, Header()],
    ) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [(b"idempotency-key", b"8e03978e-40d5-43e8-bc93-6894a57f9324")],
        },
    )


def test_message_headers_multiple(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(
        id: Annotated[int, Header()],
        etag: Annotated[str, Header()],
    ) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [
                (b"id", b"10"),
                (b"etag", b"33a64df551425fcc55e4d42a148795d9f25f89d4"),
            ],
        },
    )


@pytest.mark.parametrize(
    "headers",
    [[(b"id", b"33a64df551425fcc55e4d42a148795d9f25f89d4")], []],
)
def test_message_header_optional(
    headers: Sequence[tuple[bytes, bytes]], app_benchmark: AppBenchmark
) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(id: Annotated[Optional[str], Header()] = None) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": headers,
        },
    )


@pytest.mark.parametrize(
    "headers",
    [
        [(b"id", b"1")],
        [(b"id", b"1"), (b"example", b"value")],
    ],
)
def test_message_header_default(
    headers: Sequence[tuple[bytes, bytes]], app_benchmark: AppBenchmark
) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(
        id: Annotated[int, Header()], example: Annotated[str, Header()] = "default"
    ) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": headers,
        },
    )


def test_message_sending_dict(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_payload_dataclass(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @dataclass
    class MessagePayload:
        id: int

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "payload": b'{"id":1}',
        },
    )


def test_message_payload_simple(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(payload: Annotated[int, Payload()]) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "payload": b"123",
        },
    )


def test_message_payload_address_parameter(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("order.{user_id}")
    async def order_handler(user_id: str) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "order.1234",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_sending_message(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @dataclass
    class SendMessage(Message, address="send_topic"):
        payload: int
        id: Annotated[int, Header()]

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[SendMessage, None]:
        yield SendMessage(payload=10, id=10)

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_address_parameter(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @dataclass
    class SendMessage(Message, address="send.{name}"):
        name: str
        payload: int

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[SendMessage, None]:
        yield SendMessage(
            name="test",
            payload=10,
        )

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_nack(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler() -> None:
        raise Exception("test")

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )


def test_message_binding_kafka_key(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(key: Annotated[int, KafkaKey()]) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "bindings": {"kafka": {"key": b"1234"}},
        },
    )


@pytest.mark.parametrize("bindings", ({}, {"kafka": {"key": b"1234"}}))
def test_message_binding_default_kafka_key(
    bindings: dict[str, Any], app_benchmark: AppBenchmark
) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(key: Annotated[Optional[int], KafkaKey()] = None) -> None:
        pass

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
            "bindings": bindings,
        },
    )


def test_message_sender(app_benchmark: AppBenchmark) -> None:
    app = AsyncFast()

    @dataclass
    class SendMessage(Message, address="send_topic"):
        payload: int
        id: Annotated[int, Header()]

    @app.channel("topic")
    async def topic_handler(message_sender: MessageSender[SendMessage]) -> None:
        await message_sender.send(SendMessage(payload=10, id=10))

    app_benchmark(
        app,
        {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
        },
        {
            "type": "message.receive",
            "id": "id-1",
            "headers": [],
        },
    )
