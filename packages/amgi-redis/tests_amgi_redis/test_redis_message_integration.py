from collections.abc import AsyncGenerator
from typing import Any
from typing import Generator
from uuid import uuid4

import pytest
from amgi_redis import _run_cli
from amgi_redis import run
from amgi_redis import Server
from redis.asyncio.client import PubSub
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.redis import AsyncRedisContainer


@pytest.fixture(scope="module")
def redis_container() -> Generator[AsyncRedisContainer, None, None]:
    with AsyncRedisContainer(image="ghcr.io/asyncfast/redis:8.2.2") as redis_container:
        yield redis_container


@pytest.fixture
def channel() -> str:
    return f"receive-{uuid4()}"


@pytest.fixture
async def app(
    redis_container: AsyncRedisContainer, channel: str
) -> AsyncGenerator[MockApp, None]:

    app = MockApp()

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(redis_container.port)
    server = Server(app, channel, url=f"redis://{host}:{port}")
    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(
    app: MockApp, channel: str, redis_container: AsyncRedisContainer
) -> None:
    client = await redis_container.get_async_client()

    await client.publish(channel, "value")

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": channel,
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "headers": [],
            "payload": b"value",
            "state": {},
            "type": "message",
        }


async def _get_message(pubsub: PubSub) -> Any:
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None and message["type"] == "message":
            return message


@pytest.mark.integration
async def test_message_send(
    app: MockApp, channel: str, redis_container: AsyncRedisContainer
) -> None:
    client = await redis_container.get_async_client()

    await client.publish(channel, "value")
    send_channel = f"send-{uuid4()}"

    async with client.pubsub() as pubsub:
        await pubsub.subscribe(send_channel)

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_channel,
                    "headers": [],
                    "payload": b"test",
                }
            )

        message = await _get_message(pubsub)

        assert message == {
            "type": "message",
            "pattern": None,
            "channel": send_channel.encode(),
            "data": b"test",
        }


@pytest.mark.integration
async def test_lifespan(redis_container: AsyncRedisContainer, channel: str) -> None:
    client = await redis_container.get_async_client()

    app = MockApp()

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(redis_container.port)
    server = Server(app, channel, url=f"redis://{host}:{port}")

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await client.publish(channel, "")

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": channel,
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "headers": [],
                "payload": b"",
                "state": {"item": state_item},
                "type": "message",
            }


@pytest.mark.integration
def test_run(redis_container: AsyncRedisContainer, channel: str) -> None:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(redis_container.port)

    assert_run_can_terminate(run, channel, url=f"redis://{host}:{port}")


@pytest.mark.integration
def test_run_cli(redis_container: AsyncRedisContainer, channel: str) -> None:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(redis_container.port)

    assert_run_can_terminate(_run_cli, [channel], url=f"redis://{host}:{port}")


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, channel: str, redis_container: AsyncRedisContainer
) -> None:
    client = await redis_container.get_async_client()

    await client.publish(channel, "")

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
