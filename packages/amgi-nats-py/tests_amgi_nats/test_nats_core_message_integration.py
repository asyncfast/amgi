from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from amgi_nats_py.core import _run_cli
from amgi_nats_py.core import run
from amgi_nats_py.core import Server
from nats.aio.client import Client
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.nats import NatsContainer


@pytest.fixture
def subject() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
async def app(
    nats_container: NatsContainer, subject: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(app, subject, servers=nats_container.nats_uri())

    async with app.lifespan(server=server):
        yield app


async def test_message(app: MockApp, subject: str, client: Client) -> None:
    await client.publish(subject, b"value", headers={"test": "test"})

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": subject,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
            "state": {},
        }

        message_receive = await receive()
        assert message_receive["type"] == "message.receive"
        assert message_receive == {
            "headers": [(b"test", b"test")],
            "id": f"",
            "payload": b"value",
            "type": "message.receive",
        }

        await send(
            {
                "type": "message.ack",
                "id": "",
            }
        )


async def test_message_send(app: MockApp, subject: str, client: Client) -> None:
    send_subject = f"send-{uuid4()}"
    await client.publish(subject, b"")
    subscription = await client.subscribe(send_subject)

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_subject,
                "headers": [(b"test", b"test")],
                "payload": b"test",
            }
        )

        msg = await anext(aiter(subscription.messages))

        assert msg.subject == send_subject
        assert msg.data == b"test"
        assert msg.headers == {"test": "test"}

    await subscription.unsubscribe()


async def test_lifespan(
    nats_container: NatsContainer, subject: str, client: Client
) -> None:
    app = MockApp()

    server = Server(app, subject, servers=nats_container.nats_uri())

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await client.publish(subject, b"")

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": subject,
                "amgi": {"spec_version": "1.0", "version": "1.0"},
                "type": "message",
                "state": {"item": state_item},
            }


def test_run(client: Client, nats_container: NatsContainer) -> None:
    assert_run_can_terminate(
        run, f"receive-{uuid4()}", servers=nats_container.nats_uri()
    )


def test_run_cli(client: Client, nats_container: NatsContainer) -> None:
    assert_run_can_terminate(
        _run_cli, [f"receive-{uuid4()}"], servers=[nats_container.nats_uri()]
    )
