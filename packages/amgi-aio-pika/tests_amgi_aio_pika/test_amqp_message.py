from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from amgi_aio_pika import _encode_headers
from amgi_aio_pika import _Send
from amgi_aio_pika import MessageSend
from amgi_types import MessageSendEvent


async def test_send_nack() -> None:
    message = SimpleNamespace(ack=AsyncMock(), nack=AsyncMock())
    message_send = AsyncMock()

    await _Send(message, message_send)({"type": "message.nack", "message": "reject"})

    message.nack.assert_awaited_once_with()
    message.ack.assert_not_awaited()
    message_send.assert_not_awaited()


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend("amqp://guest:guest@localhost/")

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send({"type": "message.send", "address": "queue", "headers": []})


async def test_message_send_uses_aio_pika_bindings_and_exchange() -> None:
    exchange = SimpleNamespace(publish=AsyncMock())
    channel = SimpleNamespace(
        default_exchange=SimpleNamespace(publish=AsyncMock()),
        get_exchange=AsyncMock(return_value=exchange),
    )
    message_send = MessageSend("amqp://guest:guest@localhost/")
    message_send._channel = channel

    event = cast(
        MessageSendEvent,
        {
            "type": "message.send",
            "address": "fallback",
            "headers": [(b"name", b"value"), (b"attempt", 3)],
            "bindings": {
                "aio_pika": {
                    "exchange": "events",
                    "routing_key": "custom",
                    "content_type": "application/json",
                    "message_id": "message-id",
                }
            },
        },
    )

    await message_send(event)

    channel.get_exchange.assert_awaited_once_with("events", ensure=False)
    exchange.publish.assert_awaited_once()
    (message,) = exchange.publish.await_args.args
    assert exchange.publish.await_args.kwargs == {"routing_key": "custom"}
    assert message.body == b""
    assert message.headers == {"name": "value", "attempt": 3}
    assert message.content_type == "application/json"
    assert message.message_id == "message-id"
    channel.default_exchange.publish.assert_not_awaited()


async def test_message_send_closes_initialized_channel_and_connection() -> None:
    channel = SimpleNamespace(close=AsyncMock())
    connection = SimpleNamespace(close=AsyncMock())
    message_send = MessageSend("amqp://guest:guest@localhost/")
    message_send._channel = channel
    message_send._connection = connection

    await message_send.__aexit__(None, None, None)

    channel.close.assert_awaited_once_with()
    connection.close.assert_awaited_once_with()


def test_encode_headers_handles_empty_and_mixed_values() -> None:
    assert _encode_headers(None) == []
    assert _encode_headers({}) == []
    assert _encode_headers(
        {
            "string": "value",
            cast(Any, b"bytes-key"): b"bytes-value",
            "missing": None,
            "number": 3,
        }
    ) == [
        (b"string", b"value"),
        (b"bytes-key", b"bytes-value"),
        (b"missing", b""),
        (b"number", b"3"),
    ]
