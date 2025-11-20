import asyncio
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from aio_pika import connect_robust
from aio_pika import Message
from amgi_aiopika_amqp import Server
from test_utils import MockApp
from testcontainers.rabbitmq import RabbitMqContainer


@pytest.fixture(scope="session")
def rabbitmq_container() -> AsyncGenerator[RabbitMqContainer, None]:
    with RabbitMqContainer("rabbitmq:3.9.10") as rabbitmq:
        yield rabbitmq


@pytest.fixture
def amqp_url(rabbitmq_container: RabbitMqContainer) -> str:
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    return f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
def queue_name() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture()
async def app(
    amqp_url: str, queue_name: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(app, queue_name, url=amqp_url)
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
        for name in dir(server):
            try:
                attr = getattr(server, name)
            except Exception:
                continue
            if attr is None:
                continue
            close = getattr(attr, "close", None)
            if callable(close):
                try:
                    maybe_coro = close()
                except TypeError:
                    continue
                if asyncio.iscoroutine(maybe_coro):
                    try:
                        await maybe_coro
                    except Exception:
                        pass
        lifespan_shutdown = await receive()
        assert lifespan_shutdown == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.complete"})
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass


async def test_message(app: MockApp, amqp_url: str, queue_name: str) -> None:
    """Test receiving messages through AMQP."""
    for attempt in range(5):
        try:
            connection = await connect_robust(amqp_url)
            break
        except ConnectionResetError:
            if attempt == 4:
                raise
            await asyncio.sleep(0.5 * (2 ** attempt))
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            Message(body=b"value", headers={"test": "test"}),
            routing_key=queue_name,
        )

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": queue_name,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
        }

        message_receive = await receive()
        assert message_receive["type"] == "message.receive"
        assert message_receive["payload"] == b"value"
        assert message_receive["headers"] == [(b"test", b"test")]

        await send({"type": "message.ack", "id": message_receive["id"]})


async def test_message_send(app: MockApp, amqp_url: str, queue_name: str) -> None:
    """Test sending messages through AMQP."""
    send_queue_name = f"send-{uuid4()}"

    for attempt in range(5):
        try:
            connection = await connect_robust(amqp_url)
            break
        except ConnectionResetError:
            if attempt == 4:
                raise
            await asyncio.sleep(0.5 * (2 ** attempt))

    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        send_queue = await channel.declare_queue(send_queue_name, durable=True)

        await channel.default_exchange.publish(
            Message(body=b"trigger"),
            routing_key=queue_name,
        )

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_queue_name,
                    "headers": [(b"test", b"test")],
                    "payload": b"test",
                }
            )

            incoming_message = await send_queue.get(timeout=5)
            assert incoming_message.body == b"test"
            assert incoming_message.headers == {"test": "test"}
            await incoming_message.ack()


async def test_message_nack(
    app: MockApp, amqp_url: str, queue_name: str
) -> None:
    """Test negative acknowledgement of messages."""
    for attempt in range(5):
        try:
            connection = await connect_robust(amqp_url)
            break
        except ConnectionResetError:
            if attempt == 4:
                raise
            await asyncio.sleep(0.5 * (2 ** attempt))
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            Message(body=b"value", headers={"test": "test"}),
            routing_key=queue_name,
        )

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": queue_name,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
        }

        message_receive = await receive()
        assert message_receive["type"] == "message.receive"
        assert message_receive["payload"] == b"value"
        assert message_receive["headers"] == [(b"test", b"test")]

        await send({
            "type": "message.nack",
            "id": message_receive["id"],
            "message": "",
            "requeue": True,
        })

        message_receive_requeued = None
        for attempt in range(10):
            try:
                message_receive_requeued = await asyncio.wait_for(receive(), timeout=1)
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(0.2)

        assert message_receive_requeued is not None, "Message was not re-delivered to the application after NACK/requeue"
        assert message_receive_requeued["type"] == "message.receive"
        assert message_receive_requeued["payload"] == b"value"
        assert message_receive_requeued["headers"] == [(b"test", b"test")]

        await send({
            "type": "message.ack",
            "id": message_receive_requeued["id"],
        })
