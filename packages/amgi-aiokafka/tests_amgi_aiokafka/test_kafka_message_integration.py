import asyncio
from asyncio import Event
from asyncio import Queue
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from amgi_aiokafka import Server
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import Scope
from testcontainers.kafka import KafkaContainer


class MockApp:
    def __init__(self) -> None:
        self._call_queue = Queue[
            tuple[Scope, AMGIReceiveCallable, AMGISendCallable, Event]
        ]()

    @asynccontextmanager
    async def call(
        self,
    ) -> AsyncGenerator[tuple[Scope, AMGIReceiveCallable, AMGISendCallable], None]:
        scope, receive, send, return_event = await self._call_queue.get()
        try:
            yield scope, receive, send
        finally:
            return_event.set()

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        return_event = Event()
        self._call_queue.put_nowait((scope, receive, send, return_event))
        await return_event.wait()


@pytest.fixture(scope="session")
async def kafka_container() -> AsyncGenerator[KafkaContainer, None]:
    with KafkaContainer() as kafka_container:
        yield kafka_container


@pytest.fixture
def bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()  # type: ignore


@pytest.fixture
async def app() -> MockApp:
    return MockApp()


@pytest.fixture
def topic() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
def state_item() -> str:
    return f"state-{uuid4()}"


@pytest.fixture
async def server(
    bootstrap_server: str, app: MockApp, topic: str, state_item: str
) -> AsyncGenerator[Server, None]:
    server = Server(
        app,
        topic,
        bootstrap_servers=bootstrap_server,
        group_id=None,
    )
    loop = asyncio.get_running_loop()
    serve_task = loop.create_task(server.serve())
    async with app.call() as (scope, receive, send):
        assert scope == {
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "lifespan",
            "state": {},
        }
        scope["state"]["item"] = state_item
        lifespan_startup = await receive()
        assert lifespan_startup == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})
        yield server
        server.stop()
        lifespan_shutdown = await receive()
        assert lifespan_shutdown == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.complete"})

    await serve_task


async def test_message(
    bootstrap_server: str, server: Server, app: MockApp, topic: str, state_item: str
) -> None:

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(topic, b"value", b"key", headers=[("test", b"test")])
    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": topic,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
            "state": {"item": state_item},
        }

        message_receive = await receive()
        assert message_receive == {
            "headers": [(b"test", b"test")],
            "id": f"{topic}:0:0",
            "more_messages": False,
            "payload": b"value",
            "bindings": {"kafka": {"key": b"key"}},
            "type": "message.receive",
        }

    await producer.stop()


async def test_message_send(
    bootstrap_server: str, server: Server, app: MockApp, topic: str
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
    bootstrap_server: str, server: Server, app: MockApp, topic: str
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
