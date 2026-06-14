from typing import Any

import pytest
from amgi_types import MessageSendEvent
from pytest_amgi import Message


@pytest.mark.parametrize(
    ["message_send_event", "message"],
    [
        (
            {
                "type": "message.send",
                "address": "address",
                "headers": [],
            },
            Message(address="address"),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b"test",
                "headers": [],
            },
            Message(
                address="address",
                payload=b"test",
            ),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b"test",
                "headers": [(b"key", b"value")],
            },
            Message(
                address="address",
                payload=b"test",
                headers={"key": "value"},
            ),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b"test",
                "headers": [(b"key", b"value")],
            },
            Message(
                address="address",
                payload=b"test",
                headers={"key": "value"},
            ),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b'{"id":1}',
                "headers": [],
            },
            Message(
                address="address",
                json={"id": 1},
            ),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b'{"id": 1}',
                "headers": [],
            },
            Message(
                address="address",
                json={"id": 1},
            ),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "headers": [],
                "bindings": {"kafka": {"key": b"key"}},
            },
            Message(address="address", bindings={"kafka": {"key": b"key"}}),
        ),
    ],
)
def test_message_equality(
    message_send_event: MessageSendEvent, message: Message
) -> None:
    assert message_send_event == message


@pytest.mark.parametrize(
    ["message_send_event", "message"],
    [
        (
            {
                "type": "message.ack",
                "address": "address",
                "headers": [],
            },
            Message(address="address"),
        ),
        (
            {
                "type": "message.send",
                "address": "other-address",
                "headers": [],
            },
            Message(address="address"),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b"other",
                "headers": [],
            },
            Message(address="address", payload=b"test"),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b'{"id": 2}',
                "headers": [],
            },
            Message(address="address", json={"id": 1}),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "payload": b"test",
                "headers": [(b"key", b"other")],
            },
            Message(address="address", payload=b"test", headers={"key": "value"}),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "bindings": {"kafka": {"key": b"other"}},
            },
            Message(address="address", bindings={"kafka": {"key": b"key"}}),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "headers": [],
                "extra": "unexpected",
            },
            Message(address="address"),
        ),
        (
            {
                "type": "message.send",
                "address": "address",
                "headers": object(),
            },
            Message(address="address"),
        ),
        (
            None,
            Message(address="address"),
        ),
    ],
)
def test_message_inequality(message_send_event: Any, message: Message) -> None:
    assert message_send_event != message
