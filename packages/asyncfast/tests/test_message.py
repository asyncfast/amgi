from dataclasses import dataclass
from typing import Annotated
from typing import Any
from typing import AsyncGenerator
from typing import Iterable
from typing import Optional
from typing import Tuple
from unittest.mock import _Call
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock
from uuid import UUID

import pytest
from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Payload
from pydantic import BaseModel
from types_acgi import MessageScope


async def test_message_payload() -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        test_mock(payload)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [],
        "payload": b'{"id":1}',
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(MessagePayload(id=1))


async def test_message_header_string() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(etag: Annotated[str, Header()]) -> None:
        test_mock(etag)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [(b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with("33a64df551425fcc55e4d42a148795d9f25f89d4")


async def test_message_header_integer() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(id: Annotated[int, Header()]) -> None:
        test_mock(id)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [(b"Id", b"10")],
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(10)


async def test_message_header_underscore_to_hyphen() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(
        idempotency_key: Annotated[UUID, Header()],
    ) -> None:
        test_mock(idempotency_key)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [(b"Idempotency-Key", b"8e03978e-40d5-43e8-bc93-6894a57f9324")],
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(UUID("8e03978e-40d5-43e8-bc93-6894a57f9324"))


async def test_message_headers_multiple() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(
        id: Annotated[int, Header()],
        etag: Annotated[str, Header()],
    ) -> None:
        test_mock(id, etag)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [
            (b"Id", b"10"),
            (b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4"),
        ],
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(10, "33a64df551425fcc55e4d42a148795d9f25f89d4")


@pytest.mark.parametrize(
    ["headers", "expected_call"],
    [
        (
            [(b"Id", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
            call("33a64df551425fcc55e4d42a148795d9f25f89d4"),
        ),
        ([], call(None)),
    ],
)
async def test_message_header_optional(
    headers: Iterable[Tuple[bytes, bytes]], expected_call: _Call
) -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(id: Annotated[Optional[str], Header()] = None) -> None:
        test_mock(id)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": headers,
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    assert test_mock.mock_calls == [expected_call]


@pytest.mark.parametrize(
    ["headers", "expected_call"],
    [
        (
            [(b"Id", b"1")],
            call(1, "default"),
        ),
        (
            [(b"Id", b"1"), (b"Example", b"value")],
            call(1, "value"),
        ),
    ],
)
async def test_message_header_default(
    headers: Iterable[Tuple[bytes, bytes]], expected_call: _Call
) -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(
        id: Annotated[int, Header()], example: Annotated[str, Header()] = "default"
    ) -> None:
        test_mock(id, example)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": headers,
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    assert test_mock.mock_calls == [expected_call]


async def test_message_sending() -> None:
    app = AsyncFast()

    message_mock = Mock(
        address="send_topic",
        payload=b'{"key": "KEY-001"}',
        headers=[(b"Id", b"10")],
    )
    send_mock = AsyncMock()

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[Any, None]:
        yield message_mock

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [],
        "payload": b'{"id":10}',
    }
    await app(
        message_scope,
        AsyncMock(),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "send_topic",
            "headers": [(b"Id", b"10")],
            "payload": b'{"key": "KEY-001"}',
        }
    )


async def test_message_payload_dataclass() -> None:
    app = AsyncFast()

    @dataclass
    class MessagePayload:
        id: int

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        test_mock(payload)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [],
        "payload": b'{"id":1}',
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(MessagePayload(id=1))


async def test_message_payload_simple() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: Annotated[int, Payload()]) -> None:
        test_mock(payload)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
        "headers": [],
        "payload": b"123",
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(123)


async def test_message_payload_address_parameter() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("order.{user_id}")
    async def order_handler(user_id: str) -> None:
        test_mock(user_id)

    message_scope: MessageScope = {
        "type": "message",
        "acgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "order.1234",
        "headers": [],
    }
    await app(
        message_scope,
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with("1234")
