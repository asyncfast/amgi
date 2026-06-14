from typing import Any
from typing import Awaitable
from typing import Callable

import pytest
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from amgi_types import Scope
from pytest_amgi import AMGIProducerFactory
from pytest_amgi import Message

AMGIMessageApplication = Callable[
    [MessageScope, AMGIReceiveCallable, AMGISendCallable],
    Awaitable[None],
]


def app_factory(
    message_app: AMGIMessageApplication, *, state: dict[str, Any] | None = None
) -> AMGIApplication:

    async def app_with_lifespan(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            event = await receive()
            if state is not None:
                for key, value in state.items():
                    scope["state"][key] = value
            assert event == {"type": "lifespan.startup"}
            await send({"type": "lifespan.startup.complete"})
            event = await receive()
            assert event == {"type": "lifespan.shutdown"}
            await send({"type": "lifespan.shutdown.complete"})
        elif scope["type"] == "message":
            await message_app(scope, receive, send)

    return app_with_lifespan


async def test_producer_send(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        assert scope == {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "headers": [(b"trace-id", b"trace-1")],
            "payload": b"1",
            "state": {},
            "type": "message",
        }
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send(
        "address",
        payload="1",
        headers={"trace-id": "trace-1"},
    )

    result.assert_acked()


async def test_producer_send_bindings(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        expected_scope: MessageScope = {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [],
            "state": {},
            "type": "message",
        }
        assert scope == expected_scope

    producer = await amgi_producer(app_factory(app))

    await producer.send("address", bindings={"kafka": {"key": b"key"}})


async def test_producer_send_extensions(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        expected_scope: MessageScope = {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "extensions": {"kafka.produce.transaction": {}},
            "headers": [],
            "state": {},
            "type": "message",
        }
        assert scope == expected_scope

    producer = await amgi_producer(app_factory(app))

    await producer.send("address", extensions={"kafka.produce.transaction": {}})


async def test_producer_send_json(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        assert scope == {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "headers": [],
            "payload": b'{"id": 1}',
            "state": {},
            "type": "message",
        }
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send("address", json={"id": 1})

    result.assert_acked()


async def test_producer_send_scope(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        expected_scope: MessageScope = {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "headers": [],
            "state": {"item": "test"},
            "type": "message",
        }
        assert scope == expected_scope
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app, state={"item": "test"}))

    result = await producer.send("address")

    result.assert_acked()


async def test_app_nack(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        await send({"type": "message.nack", "message": "failure"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send("address")

    result.assert_nacked(match="failure")


async def test_app_message_send(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        assert scope == {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "headers": [],
            "payload": b'{"id": 1}',
            "state": {},
            "type": "message",
        }
        await send(
            {
                "type": "message.send",
                "address": "send_address",
                "headers": [],
            }
        )
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send("address", json={"id": 1})

    result.assert_acked()
    result.assert_has_message_send("send_address")


async def test_app_message_sends(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        assert scope == {
            "address": "address",
            "amgi": {"spec_version": "2.0", "version": "2.0"},
            "headers": [],
            "payload": b'{"id": 1}',
            "state": {},
            "type": "message",
        }
        await send(
            {
                "type": "message.send",
                "address": "send_address1",
                "headers": [],
            }
        )
        await send(
            {
                "type": "message.send",
                "address": "send_address2",
                "headers": [],
            }
        )
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send("address", json={"id": 1})

    result.assert_acked()
    result.assert_has_message_sends(
        [Message("send_address1"), Message("send_address2")]
    )


async def test_app_calls_receive_error(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        await receive()

    producer = await amgi_producer(app_factory(app))

    with pytest.raises(RuntimeError):
        await producer.send("address")


async def test_result_nack_message_none(amgi_producer: AMGIProducerFactory) -> None:
    async def app(
        scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        await send({"type": "message.ack"})

    producer = await amgi_producer(app_factory(app))

    result = await producer.send("address")

    assert result.nack_message is None


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
