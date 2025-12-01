import asyncio
from asyncio import Task
from typing import Any

from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from redis.asyncio import from_url
from redis.asyncio.client import PubSub
from redis.asyncio.client import Redis


def run(app: AMGIApplication, *channels: str, url: str = "redis://localhost") -> None:
    server = Server(app, *channels, url=url)
    server_serve(server)


def _run_cli(
    app: AMGIApplication, channels: list[str], url: str = "redis://localhost"
) -> None:
    run(app, *channels, url=url)


class _Receive:
    def __init__(self, message: dict[str, Any]) -> None:
        self._message = message

    async def __call__(self) -> MessageReceiveEvent:
        return {
            "type": "message.receive",
            "id": "",
            "headers": [],
            "payload": self._message["data"],
            "more_messages": False,
        }


class _Send:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def __call__(self, message: AMGISendEvent) -> None:
        if message["type"] == "message.send":
            await self._redis.publish(message["address"], message["payload"])


class Server:
    def __init__(self, app: AMGIApplication, *channels: str, url: str):
        self._app = app
        self._channels = channels
        self._url = url

        self._stoppable = Stoppable()
        self._tasks = set[Task[None]]()

    async def serve(self) -> None:
        redis = from_url(self._url)
        async with redis.pubsub() as pubsub:
            await pubsub.subscribe(*self._channels)
            async with Lifespan(self._app) as state:
                await self._main_loop(redis, pubsub, state)
                await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _main_loop(
        self, redis: Redis, pubsub: PubSub, state: dict[str, Any]
    ) -> None:
        loop = asyncio.get_event_loop()
        async for message in self._stoppable.call(
            pubsub.get_message, ignore_subscribe_messages=True, timeout=None
        ):
            if message is not None:
                task = loop.create_task(self._handle_message(message, redis, state))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def _handle_message(
        self, message: dict[str, Any], redis: Redis, state: dict[str, Any]
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": message["channel"].decode(),
            "state": state.copy(),
        }
        await self._app(scope, _Receive(message), _Send(redis))

    def stop(self) -> None:
        self._stoppable.stop()
