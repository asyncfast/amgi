import asyncio
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from amgi_paho_mqtt import Server
from test_utils import MockApp
from testcontainers.mqtt import MosquittoContainer


@pytest.fixture
def topic() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture(scope="session")
async def mosquitto_container() -> AsyncGenerator[MosquittoContainer, None]:
    with MosquittoContainer() as mosquitto_container:
        yield mosquitto_container


@pytest.fixture
async def app(
    topic: str, mosquitto_container: MosquittoContainer
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        topic,
        mosquitto_container.get_container_host_ip(),
        mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
        str(uuid4()),
    )
    loop = asyncio.get_running_loop()
    serve_task = loop.create_task(server.serve())
    async with app.call() as (scope, receive, send):
        assert scope == {
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "lifespan",
            "state": {},
        }
        lifespan_startup = await receive()
        assert lifespan_startup == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})
        yield app
        server.stop()
        lifespan_shutdown = await receive()
        assert lifespan_shutdown == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.complete"})

    await serve_task


async def test_message(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    mosquitto_container.publish_message(topic, "value")

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": topic,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
        }

        message_receive = await receive()
        assert message_receive == {
            "headers": [],
            "id": "0",
            "payload": b"value",
            "type": "message.receive",
        }
