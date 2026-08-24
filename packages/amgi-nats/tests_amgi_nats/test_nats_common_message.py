import pytest
from amgi_nats._common import _decode_headers
from amgi_nats._common import _encode_headers
from amgi_nats._common import MessageSend


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend("nats://localhost:4222")

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send(
            {"type": "message.send", "address": "subject", "headers": []}
        )


def test_encode_headers_handles_empty_and_values() -> None:
    assert _encode_headers(None) == []
    assert _encode_headers({}) == []
    assert _encode_headers({"name": "value"}) == [(b"name", b"value")]


def test_decode_headers_handles_empty_and_values() -> None:
    assert _decode_headers([]) == {}
    assert _decode_headers([(b"name", b"value")]) == {"name": "value"}
