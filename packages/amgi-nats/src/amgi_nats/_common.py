import asyncio
import sys
from abc import ABC
from abc import abstractmethod
from asyncio import Task
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Generic
from typing import TypeVar

import nats
from amgi_common import Lifespan
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from nats.aio.client import Client
from nats.aio.msg import Msg

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]
_SendT = Callable[[AMGISendEvent], Awaitable[None]]
_SubscriptionT = TypeVar("_SubscriptionT")


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


def _encode_headers(headers: dict[str, str] | None) -> list[tuple[bytes, bytes]]:
    if not headers:
        return []
    return [(key.encode(), value.encode()) for key, value in headers.items()]


def _decode_headers(
    headers: Sequence[tuple[bytes, bytes]],
) -> dict[str, str]:
    return {key.decode(): value.decode() for key, value in headers}


class MessageSend:
    def __init__(self, servers: str | list[str]) -> None:
        self._servers = servers
        self._connection: Client | None = None

    async def __aenter__(self) -> Self:
        self._connection = await nats.connect(self._servers)
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        if self._connection is None:
            raise RuntimeError("MessageSend not initialized")

        await self._publish(self._connection, event)

    async def _publish(self, connection: Client, event: MessageSendEvent) -> None:
        bindings = event.get("bindings", {}).get("nats", {})
        reply = bindings.get("reply", "")

        await connection.publish(
            event["address"],
            event.get("payload") or b"",
            reply=reply,
            headers=_decode_headers(event["headers"]),
        )
        await connection.flush()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._connection is not None:
            await self._connection.close()


class _Server(ABC, Generic[_SubscriptionT]):
    _message_send_type: type[MessageSend] = MessageSend

    def __init__(
        self,
        app: AMGIApplication,
        *subjects: str,
        servers: str | list[str],
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._subjects = subjects
        self._servers = servers
        self._message_send = message_send or self._message_send_type(servers)
        self._stoppable = Stoppable()
        self._tasks = set[Task[None]]()

    async def serve(self) -> None:
        connection = await nats.connect(self._servers)
        async with connection, self._message_send as message_send:
            subscriptions = await self._subscribe(connection)
            async with Lifespan(self._app) as state:
                await asyncio.gather(
                    *(
                        self._subject_loop(subscription, message_send, state)
                        for subscription in subscriptions
                    )
                )
                await asyncio.gather(*self._tasks, return_exceptions=True)

    @abstractmethod
    async def _subscribe(self, connection: Client) -> Sequence[_SubscriptionT]:
        """Subscribe to each of the configured subjects."""

    @abstractmethod
    async def _next_message(self, subscription: _SubscriptionT) -> Msg:
        """Wait for the next message on a subscription."""

    @abstractmethod
    def _send(self, message: Msg, message_send: _MessageSendT) -> _SendT:
        """Build the send callable passed to the application."""

    def _reply(self, message: Msg) -> str:
        return ""

    async def _subject_loop(
        self,
        subscription: _SubscriptionT,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        loop = asyncio.get_running_loop()
        async for message in self._stoppable.call(self._next_message, subscription):
            task = loop.create_task(self._handle_message(message, message_send, state))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _handle_message(
        self,
        message: Msg,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": message.subject,
            "headers": _encode_headers(message.headers),
            "payload": message.data,
            "bindings": {"nats": {"reply": self._reply(message)}},
            "state": state.copy(),
        }
        await self._app(scope, _receive, self._send(message, message_send))

    def stop(self) -> None:
        self._stoppable.stop()
