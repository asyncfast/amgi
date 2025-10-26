import asyncio
from collections.abc import AsyncGenerator
from threading import Event
from typing import Any
from uuid import uuid4

import pytest
from amgi_paho_mqtt import Server
from paho.mqtt.client import Client
from paho.mqtt.client import MQTTMessage
from paho.mqtt.enums import CallbackAPIVersion
from test_utils import MockApp
from testcontainers.mqtt import MosquittoContainer


@pytest.fixture
def topic() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture(scope="session")
async def mosquitto_container() -> AsyncGenerator[MosquittoContainer, None]:
    with MosquittoContainer() as mosquitto_container:
        yield mosquitto_container
        print(mosquitto_container)


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
    mosquitto_container.publish_message(topic, "test")

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
            "payload": b"test",
            "type": "message.receive",
        }


async def test_message_send(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    send_topic = f"send-{uuid4()}"

    subscribe_event = Event()
    message: MQTTMessage
    message_event = Event()

    client = Client(CallbackAPIVersion.VERSION2)

    client.on_connect = lambda *_: client.subscribe(send_topic)
    client.on_subscribe = lambda *_: subscribe_event.set()

    @client.message_callback()
    def _message_callback(
        _client: Client, _userdata: Any, _message: MQTTMessage
    ) -> None:
        nonlocal message
        message = _message
        message_event.set()

    client.loop_start()

    client.connect(
        mosquitto_container.get_container_host_ip(),
        mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
        60,
    )

    await asyncio.to_thread(subscribe_event.wait)

    mosquitto_container.publish_message(topic, "")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_topic,
                "headers": [],
                "payload": b"test",
            }
        )

        await asyncio.to_thread(message_event.wait)
        assert message.topic == send_topic
        assert message.payload == b"test"

    client.disconnect()
