from collections.abc import AsyncGenerator
from typing import Generator
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin import NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from amgi_aiokafka import _run_cli
from amgi_aiokafka import run
from amgi_aiokafka import Server
from amgi_types import MessageAckEvent
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    with KafkaContainer(image="ghcr.io/asyncfast/cp-kafka:7.6.0") as kafka_container:
        yield kafka_container


@pytest.fixture
def bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()  # type: ignore


async def create_topic(bootstrap_server: str, topic: str) -> str:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_server)
    await admin.start()

    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:  # pragma: no cover
        pass
    finally:
        await admin.close()

    return topic


@pytest.fixture
async def receive_topic(bootstrap_server: str) -> str:
    return await create_topic(bootstrap_server, f"receive-{uuid4()}")


@pytest.fixture
async def send_topic(bootstrap_server: str) -> str:
    return await create_topic(bootstrap_server, f"send-{uuid4()}")


@pytest.fixture
async def app(
    bootstrap_server: str, receive_topic: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        receive_topic,
        bootstrap_servers=bootstrap_server,
        group_id=str(uuid4()),
        auto_offset_reset="earliest",
    )

    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(bootstrap_server: str, app: MockApp, receive_topic: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(
        receive_topic, b"value", b"key", headers=[("test", b"test")]
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": receive_topic,
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [(b"test", b"test")],
            "payload": b"value",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "type": "message",
            "state": {},
        }

        message_ack_event: MessageAckEvent = {
            "type": "message.ack",
        }
        await send(message_ack_event)

    await producer.stop()


@pytest.mark.integration
async def test_message_send(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server, auto_offset_reset="earliest"
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


@pytest.mark.integration
async def test_message_send_kafka_key(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server, auto_offset_reset="earliest"
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


@pytest.mark.integration
async def test_lifespan(bootstrap_server: str, receive_topic: str) -> None:
    app = MockApp()
    server = Server(
        app,
        receive_topic,
        bootstrap_servers=bootstrap_server,
        group_id=None,
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await producer.send_and_wait(
            receive_topic,
            b"",
        )
        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": receive_topic,
                "bindings": {"kafka": {"key": None}},
                "headers": [],
                "payload": b"",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "message",
                "state": {"item": state_item},
            }


@pytest.mark.integration
def test_run(bootstrap_server: str, receive_topic: str) -> None:
    assert_run_can_terminate(run, receive_topic, bootstrap_servers=bootstrap_server)


@pytest.mark.integration
def test_run_cli(bootstrap_server: str, receive_topic: str) -> None:
    assert_run_can_terminate(
        _run_cli, [receive_topic], bootstrap_servers=bootstrap_server
    )


@pytest.mark.integration
async def test_message_receive_not_callable(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")
    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
