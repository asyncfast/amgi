import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Generator
from uuid import uuid4

import nats
import pytest
from amgi_nats_py import Server
from nats.aio.client import Client
from test_utils import MockApp
from testcontainers.nats import NatsContainer


@pytest.fixture(scope="module")
def nats_container() -> Generator[NatsContainer, None, None]:
    with NatsContainer() as nats_container:
        yield nats_container


@pytest.fixture
def subject() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
async def client(nats_container: NatsContainer) -> Client:
    return await nats.connect(nats_container.nats_uri())


@pytest.fixture
async def app(
    nats_container: NatsContainer, subject: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(app, subject, servers=nats_container.nats_uri())
    loop = asyncio.get_running_loop()
    serve_task = loop.create_task(server.serve())

    async with app.lifespan():
        yield app
        server.stop()

    await serve_task


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
