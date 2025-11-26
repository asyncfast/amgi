from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import pytest
from amgi_redis import Server
from redis.asyncio.client import PubSub
from test_utils import MockApp
from testcontainers.redis import AsyncRedisContainer


@pytest.fixture(scope="module")
async def redis_container() -> AsyncGenerator[AsyncRedisContainer, None]:
    with AsyncRedisContainer(image="redis:8.2.2") as redis_container:
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


async def test_message(
    app: MockApp, channel: str, redis_container: AsyncRedisContainer
) -> None:
    client = await redis_container.get_async_client()

    await client.publish(channel, "value")

    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": channel,
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "type": "message",
            "state": {},
        }

        message_receive = await receive()
        assert message_receive == {
            "headers": [],
            "id": "",
            "more_messages": False,
            "payload": b"value",
            "type": "message.receive",
        }


async def _get_message(pubsub: PubSub) -> Any:
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None and message["type"] == "message":
            return message


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
                "amgi": {"spec_version": "1.0", "version": "1.0"},
                "type": "message",
                "state": {"item": state_item},
            }
