from collections.abc import AsyncGenerator
from collections.abc import Generator
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated
from typing import Any
from typing import Optional
from unittest.mock import _Call
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock
from uuid import UUID

import pytest
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import Payload
from pydantic import BaseModel


class IsStrMatcher:
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, str)


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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
        "payload": b'{"id":1}',
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(MessagePayload(id=1))


async def test_message_payload_sync() -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("topic")
    def topic_handler(payload: MessagePayload) -> None:
        test_mock(payload)

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
        "payload": b'{"id":1}',
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [(b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [(b"Id", b"10")],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [(b"Idempotency-Key", b"8e03978e-40d5-43e8-bc93-6894a57f9324")],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [
            (b"Id", b"10"),
            (b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4"),
        ],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
    headers: Iterable[tuple[bytes, bytes]], expected_call: _Call
) -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(id: Annotated[Optional[str], Header()] = None) -> None:
        test_mock(id)

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": headers,
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
    headers: Iterable[tuple[bytes, bytes]], expected_call: _Call
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": headers,
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        AsyncMock(),
    )

    assert test_mock.mock_calls == [expected_call]


async def test_message_sending_dict() -> None:
    app = AsyncFast()

    send_mock = AsyncMock()

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send_topic",
                    "headers": [(b"Id", b"10")],
                    "payload": b'{"key": "KEY-001"}',
                }
            )
        ]
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
        "payload": b'{"id":1}',
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
        "payload": b"123",
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
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
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "order.1234",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        AsyncMock(),
    )

    test_mock.assert_called_once_with("1234")


async def test_message_sending_message() -> None:
    app = AsyncFast()

    send_mock = AsyncMock()

    @dataclass
    class SendMessage(Message, address="send_topic"):
        payload: int
        id: Annotated[int, Header()]

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[SendMessage, None]:
        yield SendMessage(payload=10, id=10)

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send_topic",
                    "headers": [(b"id", b"10")],
                    "payload": b"10",
                }
            )
        ]
    )


async def test_message_address_parameter() -> None:
    app = AsyncFast()

    send_mock = AsyncMock()

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

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send.test",
                    "headers": [],
                    "payload": b"10",
                }
            )
        ]
    )


async def test_message_ack() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler() -> None:
        test_mock()

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    send_mock = AsyncMock()
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    test_mock.assert_called_once()
    send_mock.assert_awaited_once_with({"type": "message.ack", "id": "id-1"})


async def test_message_nack() -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler() -> None:
        raise Exception("test")

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    send_mock = AsyncMock()
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {"type": "message.nack", "id": "id-1", "message": "test"}
    )


async def test_message_sending_dict_error() -> None:
    app = AsyncFast()

    test_mock = Mock()

    exception = Exception("test")

    def send_mock(event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            raise exception

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        try:
            yield {
                "address": "send_topic",
                "payload": b'{"key": "KEY-001"}',
                "headers": [(b"Id", b"10")],
            }
        except Exception as e:
            test_mock(e)

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    send_mock = AsyncMock(side_effect=send_mock)
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    test_mock.assert_called_once_with(exception)


async def test_message_sending_dict_post_error() -> None:
    app = AsyncFast()

    exception = Exception("test")

    def send_mock(event: AMGISendEvent) -> None:
        if event["type"] == "message.send" and event["address"] == "error":
            raise exception

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        try:
            yield {
                "address": "error",
                "payload": b"1",
                "headers": [],
            }
        except Exception:
            yield {
                "address": "not_error",
                "payload": b"1",
                "headers": [],
            }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    send_mock = AsyncMock(side_effect=send_mock)
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "error",
                    "headers": [],
                    "payload": b"1",
                }
            ),
            call(
                {
                    "type": "message.send",
                    "address": "not_error",
                    "headers": [],
                    "payload": b"1",
                }
            ),
            call({"type": "message.ack", "id": "id-1"}),
        ]
    )


async def test_message_sending_dict_post_error_sync() -> None:
    app = AsyncFast()

    def send_mock(event: AMGISendEvent) -> None:
        if event["type"] == "message.send" and event["address"] == "error":
            raise Exception("test")

    @app.channel("topic")
    def topic_handler() -> Generator[dict[str, Any], None, None]:
        try:
            yield {
                "address": "error",
                "payload": b"1",
                "headers": [],
            }
        except Exception:
            yield {
                "address": "not_error",
                "payload": b"1",
                "headers": [],
            }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
    }
    send_mock = AsyncMock(side_effect=send_mock)
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "error",
                    "headers": [],
                    "payload": b"1",
                }
            ),
            call(
                {
                    "type": "message.send",
                    "address": "not_error",
                    "headers": [],
                    "payload": b"1",
                }
            ),
            call({"type": "message.ack", "id": "id-1"}),
        ]
    )


async def test_message_invalid_payload_nack() -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(id: int) -> None:
        pass

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
        "address": "topic",
    }
    message_receive_event: MessageReceiveEvent = {
        "type": "message.receive",
        "id": "id-1",
        "headers": [],
        "payload": b"not_an_int",
    }
    send_mock = AsyncMock()
    await app(
        message_scope,
        AsyncMock(side_effect=[message_receive_event]),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {"type": "message.nack", "id": "id-1", "message": IsStrMatcher()}
    )
