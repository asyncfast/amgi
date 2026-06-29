from collections.abc import AsyncGenerator
from typing import Generator
from uuid import uuid4

import aio_pika
import pytest
from amgi_aio_pika import _run_cli
from amgi_aio_pika import run
from amgi_aio_pika import Server
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]


@pytest.fixture(scope="module")
def rabbitmq_container() -> Generator[RabbitMqContainer, None, None]:
    with RabbitMqContainer(image="rabbitmq:4.0.5-alpine") as container:
        yield container


@pytest.fixture
def queue_name() -> str:
    return f"queue-{uuid4()}"


@pytest.fixture
def rabbitmq_url(rabbitmq_container: RabbitMqContainer) -> str:
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(rabbitmq_container.port)
    username = rabbitmq_container.username
    password = rabbitmq_container.password
    return f"amqp://{username}:{password}@{host}:{port}/"


@pytest.fixture
async def app(rabbitmq_url: str, queue_name: str) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(app, queue_name, url=rabbitmq_url)
    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(app: MockApp, queue_name: str, rabbitmq_url: str) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"value"),
            routing_key=queue_name,
        )

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["address"] == queue_name
        assert scope["payload"] == b"value"
        assert "amqp" in scope.get("bindings", {})
        assert scope["bindings"]["amqp"]["routing_key"] == queue_name
        assert scope["state"] == {}
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_message_send(app: MockApp, queue_name: str, rabbitmq_url: str) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"trigger"),
            routing_key=queue_name,
        )

        send_queue_name = f"send-{uuid4()}"
        send_queue = await channel.declare_queue(send_queue_name, durable=True)

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_queue_name,
                    "headers": [(b"key", b"val")],
                    "payload": b"test_payload",
                    "bindings": {
                        "amqp": {
                            "content_type": "text/plain",
                        }
                    },
                }
            )
            await send({"type": "message.ack"})

        msg = await send_queue.get()
        assert msg is not None
        assert msg.body == b"test_payload"
        assert msg.headers == {"key": "val"}
        assert msg.content_type == "text/plain"
        await msg.ack()


@pytest.mark.integration
async def test_lifespan(rabbitmq_url: str, queue_name: str) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b""),
            routing_key=queue_name,
        )

    app = MockApp()
    server = Server(app, queue_name, url=rabbitmq_url)
    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        async with app.call() as (scope, receive, send):
            assert scope["type"] == "message"
            assert scope["state"] == {"item": state_item}
            await send({"type": "message.ack"})


@pytest.mark.integration
def test_run(rabbitmq_url: str, queue_name: str) -> None:
    assert_run_can_terminate(run, queue_name, url=rabbitmq_url)


@pytest.mark.integration
def test_run_cli(rabbitmq_url: str, queue_name: str) -> None:
    assert_run_can_terminate(_run_cli, [queue_name], url=rabbitmq_url)


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, queue_name: str, rabbitmq_url: str
) -> None:
    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b""),
            routing_key=queue_name,
        )

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
