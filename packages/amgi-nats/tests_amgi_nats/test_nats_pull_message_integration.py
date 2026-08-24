from collections.abc import AsyncGenerator
from typing import Generator
from uuid import uuid4

import nats
import pytest
from amgi_nats.pull import _run_cli
from amgi_nats.pull import MessageSend
from amgi_nats.pull import run
from amgi_nats.pull import Server
from nats.errors import ConnectionClosedError
from nats.errors import TimeoutError
from nats.js.errors import NoStreamResponseError
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.nats import NatsContainer


@pytest.fixture(scope="module")
def nats_container() -> Generator[NatsContainer, None, None]:
    with NatsContainer(
        image="ghcr.io/asyncfast/nats:2.10", jetstream=True
    ) as nats_container:
        yield nats_container


@pytest.fixture
def subject() -> str:
    return f"receive.{uuid4()}"


@pytest.fixture
async def send_subject(
    nats_container: NatsContainer,
) -> AsyncGenerator[str, None]:
    subject = f"send.{uuid4()}"
    connection = await nats.connect(nats_container.nats_uri())
    await connection.jetstream().add_stream(
        name=subject.replace(".", "_"), subjects=[subject]
    )
    yield subject
    await connection.close()


@pytest.fixture
async def app(
    nats_container: NatsContainer, subject: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(app, subject, servers=nats_container.nats_uri())
    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    await connection.jetstream().publish(subject, b"value")

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["address"] == subject
        assert scope["amgi"] == {"version": "2.0", "spec_version": "2.0"}
        assert scope["headers"] == []
        assert scope["payload"] == b"value"
        assert scope["bindings"] == {"nats": {"reply": ""}}
        assert scope["state"] == {}
        await send({"type": "message.ack"})

    await connection.close()


@pytest.mark.integration
async def test_message_send(
    app: MockApp, subject: str, send_subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [(b"key", b"value")],
                "payload": b"test",
            }
        )
        await send({"type": "message.ack"})

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b"test"
    assert message.headers == {"key": "value"}

    await connection.close()


@pytest.mark.integration
async def test_message_nack(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    jetstream = connection.jetstream()
    await jetstream.publish(subject, b"value")

    async with app.call() as (scope, receive, send):
        await send({"type": "message.nack", "message": "failure"})

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["payload"] == b"value"
        await send({"type": "message.ack"})

    await connection.close()


@pytest.mark.integration
async def test_message_send_uses_headers(
    app: MockApp, subject: str, send_subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [(b"name", b"value")],
                "payload": b"payload",
            }
        )
        await send({"type": "message.ack"})

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b"payload"
    assert message.headers == {"name": "value"}

    await connection.close()


@pytest.mark.integration
async def test_message_send_rejects_reply_binding(
    app: MockApp, subject: str, send_subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        with pytest.raises(ValueError, match="do not support the nats reply binding"):
            await send(
                {
                    "type": "message.send",
                    "address": send_subject,
                    "headers": [],
                    "bindings": {"nats": {"reply": f"reply.{uuid4()}"}},
                }
            )
        await send({"type": "message.ack"})

    await connection.close()


@pytest.mark.integration
async def test_message_send_without_stream(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        with pytest.raises(NoStreamResponseError):
            await send(
                {
                    "type": "message.send",
                    "address": f"unstreamed.{uuid4()}",
                    "headers": [],
                }
            )
        await send({"type": "message.ack"})

    await connection.close()


@pytest.mark.integration
async def test_message_send_defaults_empty_payload(
    app: MockApp, subject: str, send_subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [],
            }
        )
        await send({"type": "message.ack"})

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b""
    assert message.headers is None

    await connection.close()


@pytest.mark.integration
async def test_send_ignores_non_message_send_events(
    app: MockApp, subject: str, send_subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.jetstream().publish(subject, b"")

    async with app.call() as (scope, receive, send):
        await send({"type": "message.ack"})

    with pytest.raises(TimeoutError):
        await subscription.next_msg(timeout=0.5)

    await connection.close()


@pytest.mark.integration
async def test_message_send_closes_initialized_connection(
    nats_container: NatsContainer,
) -> None:
    message_send = MessageSend(nats_container.nats_uri())

    async with message_send:
        pass

    with pytest.raises(ConnectionClosedError):
        await message_send({"type": "message.send", "address": "closed", "headers": []})


@pytest.mark.integration
async def test_lifespan(subject: str, nats_container: NatsContainer) -> None:
    app = MockApp()
    server = Server(app, subject, servers=nats_container.nats_uri())

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        connection = await nats.connect(nats_container.nats_uri())
        await connection.jetstream().publish(subject, b"")

        async with app.call() as (scope, receive, send):
            assert scope["type"] == "message"
            assert scope["address"] == subject
            assert scope["headers"] == []
            assert scope["payload"] == b""
            assert scope["bindings"] == {"nats": {"reply": ""}}
            assert scope["amgi"] == {"version": "2.0", "spec_version": "2.0"}
            assert scope["state"] == {"item": state_item}
            await send({"type": "message.ack"})

        await connection.close()


@pytest.mark.integration
def test_run(subject: str, nats_container: NatsContainer) -> None:
    assert_run_can_terminate(run, subject, servers=nats_container.nats_uri())


@pytest.mark.integration
def test_run_cli(subject: str, nats_container: NatsContainer) -> None:
    assert_run_can_terminate(_run_cli, [subject], servers=nats_container.nats_uri())


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    await connection.jetstream().publish(subject, b"test")

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
        await send({"type": "message.ack"})

    await connection.close()
