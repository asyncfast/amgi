import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Generator
from uuid import uuid4

import pulsar.asyncio
import pytest
from amgi_pulsar import _run_cli
from amgi_pulsar import MessageSend
from amgi_pulsar import run
from amgi_pulsar import Server
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.core.generic import DockerContainer


@pytest.fixture(scope="module")
def pulsar_container() -> Generator[DockerContainer, None, None]:
    container = DockerContainer(image="apachepulsar/pulsar:4.0.5")
    container.with_command("bin/pulsar standalone")
    container.with_exposed_ports(6650, 8080)
    with container:
        wait_until_ready(container)
        yield container


def service_url(container: DockerContainer) -> str:
    return f"pulsar://{container.get_container_host_ip()}:{container.get_exposed_port(6650)}"


def wait_until_ready(container: DockerContainer) -> None:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            client = pulsar.Client(
                service_url(container),
                operation_timeout_seconds=2,
                connection_timeout_ms=2000,
            )
            try:
                client.create_producer("readiness-probe").close()
            finally:
                client.close()
            return
        except (OSError, pulsar.PulsarException):
            time.sleep(2)
    raise RuntimeError("Pulsar container failed to become ready")  # pragma: no cover


@pytest.fixture
def topic() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
def subscription_name() -> str:
    return f"amgi-{uuid4()}"


@pytest.fixture
async def app(
    pulsar_container: DockerContainer, topic: str, subscription_name: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        topic,
        service_url=service_url(pulsar_container),
        subscription_name=subscription_name,
    )
    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    await producer.send(b"value")
    await client.close()

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["address"] == topic
        assert scope["amgi"] == {"version": "2.0", "spec_version": "2.0"}
        assert scope["headers"] == []
        assert scope["payload"] == b"value"
        assert scope["bindings"] == {"pulsar": {"key": ""}}
        assert scope["state"] == {}
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_message_uses_properties_and_key(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    await producer.send(b"payload", properties={"name": "value"}, partition_key="key")
    await client.close()

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["headers"] == [(b"name", b"value")]
        assert scope["bindings"] == {"pulsar": {"key": "key"}}
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_message_send(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    send_topic = f"send-{uuid4()}"

    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    reader = await client.create_reader(send_topic, pulsar.MessageId.earliest)

    await producer.send(b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_topic,
                "headers": [(b"key", b"value")],
                "payload": b"test",
            }
        )
        await send({"type": "message.ack"})

    message = await reader.read_next()
    assert message.data() == b"test"
    assert message.properties() == {"key": "value"}

    await reader.close()
    await client.close()


@pytest.mark.integration
async def test_message_send_uses_key_binding(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    send_topic = f"send-{uuid4()}"

    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    reader = await client.create_reader(send_topic, pulsar.MessageId.earliest)

    await producer.send(b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_topic,
                "headers": [],
                "payload": b"payload",
                "bindings": {"pulsar": {"key": "ordering-key"}},
            }
        )
        await send({"type": "message.ack"})

    message = await reader.read_next()
    assert message.data() == b"payload"
    assert message.partition_key() == "ordering-key"

    await reader.close()
    await client.close()


@pytest.mark.integration
async def test_message_send_defaults_empty_payload(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    send_topic = f"send-{uuid4()}"

    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    reader = await client.create_reader(send_topic, pulsar.MessageId.earliest)

    await producer.send(b"")

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_topic,
                "headers": [],
            }
        )
        await send({"type": "message.ack"})

    message = await reader.read_next()
    assert message.data() == b""
    assert message.properties() == {}

    await reader.close()
    await client.close()


@pytest.mark.integration
async def test_send_ignores_non_message_send_events(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    send_topic = f"send-{uuid4()}"

    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    reader = await client.create_reader(send_topic, pulsar.MessageId.earliest)

    await producer.send(b"")

    async with app.call() as (scope, receive, send):
        await send({"type": "message.ack"})

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(reader.read_next(), timeout=1)

    await reader.close()
    await client.close()


@pytest.mark.integration
async def test_message_nack(
    pulsar_container: DockerContainer, topic: str, subscription_name: str
) -> None:
    app = MockApp()
    server = Server(
        app,
        topic,
        service_url=service_url(pulsar_container),
        subscription_name=subscription_name,
        negative_ack_redelivery_delay_ms=100,
    )
    async with app.lifespan(server=server):
        client = pulsar.asyncio.Client(service_url(pulsar_container))
        producer = await client.create_producer(topic)
        await producer.send(b"value")

        async with app.call() as (scope, receive, send):
            await send({"type": "message.nack", "message": "failure"})

        async with app.call() as (scope, receive, send):
            assert scope["type"] == "message"
            assert scope["payload"] == b"value"
            await send({"type": "message.ack"})

        await client.close()


@pytest.mark.integration
async def test_message_send_closes_initialized_client(
    pulsar_container: DockerContainer,
) -> None:
    message_send = MessageSend(service_url(pulsar_container))
    send_topic = f"send-{uuid4()}"

    async with message_send:
        await message_send(
            {"type": "message.send", "address": send_topic, "headers": []}
        )

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send(
            {"type": "message.send", "address": send_topic, "headers": []}
        )


@pytest.mark.integration
async def test_lifespan(
    pulsar_container: DockerContainer, topic: str, subscription_name: str
) -> None:
    app = MockApp()
    server = Server(
        app,
        topic,
        service_url=service_url(pulsar_container),
        subscription_name=subscription_name,
    )

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        client = pulsar.asyncio.Client(service_url(pulsar_container))
        producer = await client.create_producer(topic)
        await producer.send(b"")

        async with app.call() as (scope, receive, send):
            assert scope["type"] == "message"
            assert scope["address"] == topic
            assert scope["payload"] == b""
            assert scope["bindings"] == {"pulsar": {"key": ""}}
            assert scope["amgi"] == {"version": "2.0", "spec_version": "2.0"}
            assert scope["state"] == {"item": state_item}
            await send({"type": "message.ack"})

        await client.close()


@pytest.mark.integration
def test_run(
    pulsar_container: DockerContainer, topic: str, subscription_name: str
) -> None:
    assert_run_can_terminate(
        run,
        topic,
        service_url=service_url(pulsar_container),
        subscription_name=subscription_name,
    )


@pytest.mark.integration
def test_run_cli(
    pulsar_container: DockerContainer, topic: str, subscription_name: str
) -> None:
    assert_run_can_terminate(
        _run_cli,
        [topic],
        service_url=service_url(pulsar_container),
        subscription_name=subscription_name,
    )


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, topic: str, pulsar_container: DockerContainer
) -> None:
    client = pulsar.asyncio.Client(service_url(pulsar_container))
    producer = await client.create_producer(topic)
    await producer.send(b"test")
    await client.close()

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
        await send({"type": "message.ack"})
