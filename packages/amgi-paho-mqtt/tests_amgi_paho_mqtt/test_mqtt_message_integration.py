import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from threading import Event
from typing import Any
from uuid import uuid4

import pytest
from amgi_paho_mqtt import PublishError
from amgi_paho_mqtt import run
from amgi_paho_mqtt import Server
from paho.mqtt.client import Client
from paho.mqtt.client import MQTTMessage
from paho.mqtt.client import MQTTv5
from paho.mqtt.enums import CallbackAPIVersion
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.mqtt import MosquittoContainer


@pytest.fixture
def topic() -> str:
    return f"receive/{uuid4()}"


@pytest.fixture(scope="module")
async def mosquitto_container() -> AsyncGenerator[MosquittoContainer, None]:
    mosquitto_container = MosquittoContainer(
        image="ghcr.io/asyncfast/eclipse-mosquitto:2.0.22"
    ).with_volume_mapping(
        Path(__file__).parent / "mqtt.acl",
        "/mosquitto/config/mqtt.acl",
    )
    try:
        mosquitto_container.start(str(Path(__file__).parent / "mosquitto.conf"))
        yield mosquitto_container
    finally:
        mosquitto_container.stop()


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
        MQTTv5,
    )
    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    mosquitto_container.publish_message(topic, "test")

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": topic,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
            "state": {},
        }

        message_receive = await receive()
        assert message_receive == {
            "headers": [],
            "id": "0",
            "payload": b"test",
            "type": "message.receive",
        }


@pytest.mark.integration
async def test_message_send(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    send_topic = f"send/{uuid4()}"

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


@pytest.mark.integration
async def test_lifespan(topic: str, mosquitto_container: MosquittoContainer) -> None:
    app = MockApp()
    server = Server(
        app,
        topic,
        mosquitto_container.get_container_host_ip(),
        mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
        str(uuid4()),
    )

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        mosquitto_container.publish_message(topic, "")

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": topic,
                "amgi": {"spec_version": "1.0", "version": "1.0"},
                "type": "message",
                "state": {"item": state_item},
            }


@pytest.mark.integration
async def test_message_send_deny(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    mosquitto_container.publish_message(topic, "")

    async with app.call() as (scope, receive, send):
        with pytest.raises(PublishError, match="Not authorized"):
            await send(
                {
                    "type": "message.send",
                    "address": f"deny/{uuid4()}",
                    "headers": [],
                    "payload": b"test",
                    "bindings": {"mqtt": {"qos": 1}},
                }
            )


@pytest.mark.integration
def test_run(topic: str, mosquitto_container: MosquittoContainer) -> None:
    assert_run_can_terminate(
        run,
        topic,
        host=mosquitto_container.get_container_host_ip(),
        port=mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
    )
