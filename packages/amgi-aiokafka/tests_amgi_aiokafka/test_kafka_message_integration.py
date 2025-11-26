from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from amgi_aiokafka import Server
from amgi_types import MessageAckEvent
from test_utils import MockApp
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
async def kafka_container() -> AsyncGenerator[KafkaContainer, None]:
    with KafkaContainer() as kafka_container:
        yield kafka_container


@pytest.fixture
def bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()  # type: ignore


@pytest.fixture
def topic() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
async def app(bootstrap_server: str, topic: str) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=None,
    )

    async with app.lifespan(server=server):
        yield app


async def test_message(bootstrap_server: str, app: MockApp, topic: str) -> None:

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(topic, b"value", b"key", headers=[("test", b"test")])
    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": topic,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
            "state": {},
        }

        message_receive = await receive()
        assert message_receive["type"] == "message.receive"
        assert message_receive == {
            "headers": [(b"test", b"test")],
            "id": f"{topic}:0:0",
            "more_messages": False,
            "payload": b"value",
            "bindings": {"kafka": {"key": b"key"}},
            "type": "message.receive",
        }

        message_ack_event: MessageAckEvent = {
            "type": "message.ack",
            "id": message_receive["id"],
        }
        await send(message_ack_event)

    await producer.stop()


async def test_message_send(bootstrap_server: str, app: MockApp, topic: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(topic, b"")
    send_topic = f"send-{uuid4()}"

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server
    ) as consumer:
        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [(b"test", b"test")],
                    "payload": b"test",
                }
            )

        message = await consumer.getone()
        assert message.topic == send_topic
        assert message.value == b"test"
        assert message.headers == (("test", b"test"),)

    await producer.stop()


async def test_message_send_kafka_key(
    bootstrap_server: str, app: MockApp, topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(topic, b"")
    send_topic = f"send-{uuid4()}"

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server
    ) as consumer:
        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [],
                    "bindings": {"kafka": {"key": b"test"}},
                }
            )

        message = await consumer.getone()
        assert message.topic == send_topic
        assert message.key == b"test"

    await producer.stop()


async def test_lifespan(bootstrap_server: str, topic: str) -> None:
    app = MockApp()
    server = Server(
        app,
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=None,
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await producer.send_and_wait(
            topic,
            b"",
        )
        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": topic,
                "amgi": {"spec_version": "1.0", "version": "1.0"},
                "type": "message",
                "state": {"item": state_item},
            }
