import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from threading import Event
from typing import Any
from typing import Generator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from amgi_paho_mqtt import _run_cli
from amgi_paho_mqtt import MessageSend
from amgi_paho_mqtt import PublishError
from amgi_paho_mqtt import run
from amgi_paho_mqtt import Server
from paho.mqtt.client import Client
from paho.mqtt.client import ConnectFlags
from paho.mqtt.client import MQTTMessage
from paho.mqtt.client import MQTTv5
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.mqtt import MosquittoContainer


@pytest.fixture
def topic() -> str:
    return f"receive/{uuid4()}"


@pytest.fixture(scope="module")
def mosquitto_container() -> Generator[MosquittoContainer, None, None]:
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
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "type": "message",
            "headers": [],
            "payload": b"test",
            "state": {},
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
                "headers": [],
                "payload": b"",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "message",
                "state": {"item": state_item},
            }


def _mosquitto_address(mosquitto_container: MosquittoContainer) -> tuple[str, int]:
    return (
        mosquitto_container.get_container_host_ip(),
        mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
    )


@pytest.mark.integration
async def test_injected_message_send(
    mosquitto_container: MosquittoContainer,
) -> None:
    host, port = _mosquitto_address(mosquitto_container)
    receive_topic = f"receive/{uuid4()}"
    send_topic = f"send/{uuid4()}"

    subscribe_event = Event()
    received_message: MQTTMessage
    message_event = Event()

    client = Client(CallbackAPIVersion.VERSION2)

    def _on_connect(
        _client: Client,
        _userdata: Any,
        _connect_flags: ConnectFlags,
        _reason_code: ReasonCode,
        _properties: Properties | None,
    ) -> None:
        client.subscribe(send_topic)

    client.on_connect = _on_connect
    client.on_subscribe = lambda *_: subscribe_event.set()

    @client.message_callback()
    def _message_callback(
        _client: Client, _userdata: Any, _message: MQTTMessage
    ) -> None:
        nonlocal received_message
        received_message = _message
        message_event.set()

    client.loop_start()
    client.connect(host, port, 60)

    app = MockApp()
    server = Server(
        app,
        receive_topic,
        host,
        port,
        str(uuid4()),
        message_send=MessageSend(host, port, protocol=MQTTv5),
    )

    async with app.lifespan(server=server):
        await asyncio.to_thread(subscribe_event.wait)

        mosquitto_container.publish_message(receive_topic, "")

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [],
                    "payload": b"injected",
                }
            )

        await asyncio.to_thread(message_event.wait)
        assert received_message.topic == send_topic
        assert received_message.payload == b"injected"

    client.disconnect()


@pytest.mark.integration
async def test_injected_message_send_manager(
    mosquitto_container: MosquittoContainer,
) -> None:
    host, port = _mosquitto_address(mosquitto_container)
    receive_topic = f"receive/{uuid4()}"

    entered: list[bool] = []
    exited: list[bool] = []
    message_send = AsyncMock()

    class _MessageSendManager:
        async def __aenter__(self) -> Any:
            entered.append(True)
            return message_send

        async def __aexit__(self, *exc_info: Any) -> None:
            exited.append(True)

    app = MockApp()
    server = Server(
        app,
        receive_topic,
        host,
        port,
        str(uuid4()),
        message_send=_MessageSendManager(),
    )

    async with app.lifespan(server=server):
        mosquitto_container.publish_message(receive_topic, "")

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": "send/any",
                    "headers": [],
                    "payload": b"test",
                }
            )

    assert entered == [True]
    assert exited == [True]
    message_send.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "send/any",
            "headers": [],
            "payload": b"test",
        }
    )


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


@pytest.mark.integration
def test_run_cli(topic: str, mosquitto_container: MosquittoContainer) -> None:
    assert_run_can_terminate(
        _run_cli,
        topic,
        host=mosquitto_container.get_container_host_ip(),
        port=mosquitto_container.get_exposed_port(mosquitto_container.MQTT_PORT),
    )


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, topic: str, mosquitto_container: MosquittoContainer
) -> None:
    mosquitto_container.publish_message(topic, "test")

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
