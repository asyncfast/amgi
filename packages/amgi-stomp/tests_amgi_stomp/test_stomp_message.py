from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from amgi_stomp import _decode_headers
from amgi_stomp import _encode_headers
from amgi_stomp import _Send
from amgi_stomp import MessageSend
from stompman import AckableMessageFrame
from stompman import Client


async def test_send_ack() -> None:
    frame = SimpleNamespace(ack=AsyncMock(), nack=AsyncMock())
    message_send = AsyncMock()

    await _Send(cast(AckableMessageFrame, frame), message_send)({"type": "message.ack"})

    frame.ack.assert_awaited_once_with()
    frame.nack.assert_not_awaited()
    message_send.assert_not_awaited()


async def test_send_nack() -> None:
    frame = SimpleNamespace(ack=AsyncMock(), nack=AsyncMock())
    message_send = AsyncMock()

    await _Send(cast(AckableMessageFrame, frame), message_send)(
        {"type": "message.nack", "message": "reject"}
    )

    frame.nack.assert_awaited_once_with()
    frame.ack.assert_not_awaited()
    message_send.assert_not_awaited()


async def test_send_message_send() -> None:
    frame = SimpleNamespace(ack=AsyncMock(), nack=AsyncMock())
    message_send = AsyncMock()

    await _Send(cast(AckableMessageFrame, frame), message_send)(
        {"type": "message.send", "address": "destination", "headers": []}
    )

    message_send.assert_awaited_once_with(
        {"type": "message.send", "address": "destination", "headers": []}
    )
    frame.ack.assert_not_awaited()
    frame.nack.assert_not_awaited()


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend("localhost", 61613, "guest", "guest")

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send(
            {"type": "message.send", "address": "destination", "headers": []}
        )


async def test_message_send_closes_client() -> None:
    message_send = MessageSend("localhost", 61613, "guest", "guest")
    client_exit = AsyncMock()
    message_send._client = cast(Client, SimpleNamespace(__aexit__=client_exit))

    await message_send.__aexit__(None, None, None)

    client_exit.assert_awaited_once_with(None, None, None)


def test_encode_headers_handles_empty_and_values() -> None:
    assert _encode_headers(None) == []
    assert _encode_headers({}) == []
    assert _encode_headers({"name": "value"}) == [(b"name", b"value")]


def test_decode_headers_handles_empty_and_values() -> None:
    assert _decode_headers([]) == {}
    assert _decode_headers([(b"name", b"value")]) == {"name": "value"}
