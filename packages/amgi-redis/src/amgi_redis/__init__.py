import asyncio
import sys
from asyncio import Task
from collections.abc import Awaitable
from collections.abc import Callable
from types import TracebackType
from typing import Any
from typing import AsyncContextManager

from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from redis.asyncio import from_url
from redis.asyncio.client import PubSub
from redis.asyncio.client import Redis

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


def run(
    app: AMGIApplication,
    *channels: str,
    url: str = "redis://localhost",
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(app, *channels, url=url, message_send=message_send)
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
    def __init__(self, message_send: _MessageSendT) -> None:
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await self._message_send(event)


class MessageSend:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def __aenter__(self) -> Self:
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        await self._redis.publish(event["address"], event["payload"])

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._redis.aclose()


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *channels: str,
        url: str,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._channels = channels
        self._redis = from_url(url)
        self._message_send = message_send or MessageSend(self._redis)

        self._stoppable = Stoppable()
        self._tasks = set[Task[None]]()

    async def serve(self) -> None:
        async with self._redis.pubsub() as pubsub, self._message_send as message_send:
            await pubsub.subscribe(*self._channels)
            async with Lifespan(self._app) as state:
                await self._main_loop(message_send, pubsub, state)
                await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _main_loop(
        self, message_send: _MessageSendT, pubsub: PubSub, state: dict[str, Any]
    ) -> None:
        loop = asyncio.get_event_loop()
        async for message in self._stoppable.call(
            pubsub.get_message, ignore_subscribe_messages=True, timeout=None
        ):
            if message is not None:
                task = loop.create_task(
                    self._handle_message(message, message_send, state)
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def _handle_message(
        self,
        message: dict[str, Any],
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": message["channel"].decode(),
            "state": state.copy(),
        }
        await self._app(scope, _Receive(message), _Send(message_send))

    def stop(self) -> None:
        self._stoppable.stop()
