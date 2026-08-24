from collections.abc import AsyncGenerator
from typing import Generator
from uuid import uuid4

import nats
import pytest
from amgi_nats.push import _run_cli
from amgi_nats.push import MessageSend
from amgi_nats.push import run
from amgi_nats.push import Server
from nats.errors import ConnectionClosedError
from nats.errors import TimeoutError
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.nats import NatsContainer


@pytest.fixture(scope="module")
def nats_container() -> Generator[NatsContainer, None, None]:
    with NatsContainer(image="ghcr.io/asyncfast/nats:2.10") as nats_container:
        yield nats_container


@pytest.fixture
def subject() -> str:
    return f"receive.{uuid4()}"


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
    await connection.publish(subject, b"value")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": subject,
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "type": "message",
            "headers": [],
            "payload": b"value",
            "bindings": {"nats": {"reply": ""}},
            "state": {},
        }

    await connection.close()


@pytest.mark.integration
async def test_message_send(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    send_subject = f"send.{uuid4()}"

    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.publish(subject, b"")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [(b"key", b"value")],
                "payload": b"test",
            }
        )

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b"test"
    assert message.headers == {"key": "value"}

    await connection.close()


@pytest.mark.integration
async def test_message_send_uses_nats_bindings_and_headers(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    send_subject = f"send.{uuid4()}"
    reply_subject = f"reply.{uuid4()}"

    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.publish(subject, b"")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [(b"name", b"value")],
                "payload": b"payload",
                "bindings": {"nats": {"reply": reply_subject}},
            }
        )

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b"payload"
    assert message.reply == reply_subject
    assert message.headers == {"name": "value"}

    await connection.close()


@pytest.mark.integration
async def test_message_send_defaults_empty_payload_and_reply(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    send_subject = f"send.{uuid4()}"

    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(send_subject)

    await connection.publish(subject, b"")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [],
            }
        )

    message = await subscription.next_msg(timeout=5)
    assert message.subject == send_subject
    assert message.data == b""
    assert message.reply == ""
    assert message.headers is None

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
async def test_send_ignores_non_message_send_events(
    app: MockApp, subject: str, nats_container: NatsContainer
) -> None:
    connection = await nats.connect(nats_container.nats_uri())
    subscription = await connection.subscribe(">")

    await connection.publish(subject, b"")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        await send({"type": "message.ack"})

    message = await subscription.next_msg(timeout=5)
    assert message.subject == subject

    with pytest.raises(TimeoutError):
        await subscription.next_msg(timeout=0.5)

    await connection.close()


@pytest.mark.integration
async def test_lifespan(subject: str, nats_container: NatsContainer) -> None:
    app = MockApp()
    server = Server(app, subject, servers=nats_container.nats_uri())

    connection = await nats.connect(nats_container.nats_uri())

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await connection.publish(subject, b"")
        await connection.flush()

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": subject,
                "headers": [],
                "payload": b"",
                "bindings": {"nats": {"reply": ""}},
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "message",
                "state": {"item": state_item},
            }

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
    await connection.publish(subject, b"test")
    await connection.flush()

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()

    await connection.close()
