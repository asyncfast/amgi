import asyncio
import sys
from collections.abc import Awaitable
from collections.abc import Callable
from types import TracebackType
from typing import Any
from typing import AsyncContextManager

import aio_pika
from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractRobustChannel
from aio_pika.abc import AbstractRobustConnection

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


def run(
    app: AMGIApplication,
    *queues: str,
    url: str = "amqp://guest:guest@localhost/",
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(app, *queues, url=url, message_send=message_send)
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    queues: list[str],
    url: str = "amqp://guest:guest@localhost/",
) -> None:
    run(app, *queues, url=url)


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


class _Send:
    def __init__(
        self,
        message: AbstractIncomingMessage,
        message_send: _MessageSendT,
    ) -> None:
        self._message = message
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._message.ack()
        elif event["type"] == "message.nack":
            await self._message.nack()
        elif event["type"] == "message.send":
            await self._message_send(event)


class MessageSend:
    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None

    async def __aenter__(self) -> Self:
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        if self._channel is None:
            raise RuntimeError("MessageSend not initialized")

        headers: dict[str, Any] = {
            key.decode(): value.decode() if isinstance(value, bytes) else value
            for key, value in event.get("headers", ())
        }

        amqp_bindings = (
            event.get("bindings", {}).get("amqp", {})
            or event.get("bindings", {}).get("aio_pika", {})
        )
        exchange_name = amqp_bindings.get("exchange", "")
        routing_key = amqp_bindings.get("routing_key", event["address"])

        message = aio_pika.Message(
            body=event.get("payload") or b"",
            headers=headers,
            content_type=amqp_bindings.get("content_type"),
            content_encoding=amqp_bindings.get("content_encoding"),
            delivery_mode=amqp_bindings.get("delivery_mode"),
            priority=amqp_bindings.get("priority"),
            correlation_id=amqp_bindings.get("correlation_id"),
            reply_to=amqp_bindings.get("reply_to"),
            expiration=amqp_bindings.get("expiration"),
            message_id=amqp_bindings.get("message_id"),
            timestamp=amqp_bindings.get("timestamp"),
            type=amqp_bindings.get("type"),
            user_id=amqp_bindings.get("user_id"),
            app_id=amqp_bindings.get("app_id"),
        )

        if exchange_name:
            exchange = await self._channel.get_exchange(exchange_name, ensure=False)
        else:
            exchange = self._channel.default_exchange

        await exchange.publish(message, routing_key=routing_key)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._channel is not None:
            await self._channel.close()
        if self._connection is not None:
            await self._connection.close()


def _encode_headers(headers: dict[str, Any] | None) -> list[tuple[bytes, bytes]]:
    if not headers:
        return []
    encoded = []
    for key, value in headers.items():
        key_bytes = key.encode() if isinstance(key, str) else bytes(key)
        if isinstance(value, str):
            val_bytes = value.encode()
        elif isinstance(value, bytes):
            val_bytes = value
        elif value is None:
            val_bytes = b""
        else:
            val_bytes = str(value).encode()
        encoded.append((key_bytes, val_bytes))
    return encoded


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *queues: str,
        url: str,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._queues = queues
        self._url = url
        self._message_send = message_send or MessageSend(self._url)
        self._stoppable = Stoppable()
        self._tasks = set[asyncio.Task[None]]()

    async def serve(self) -> None:
        connection = await aio_pika.connect_robust(self._url)
        async with connection, self._message_send as message_send:
            channel = await connection.channel()
            # Declare queues so they exist
            declared_queues = []
            for queue_name in self._queues:
                queue = await channel.declare_queue(queue_name, durable=True)
                declared_queues.append(queue)

            async with Lifespan(self._app) as state:
                await asyncio.gather(
                    *(
                        self._queue_loop(queue, message_send, state)
                        for queue in declared_queues
                    )
                )
                await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _queue_loop(
        self,
        queue: AbstractQueue,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        loop = asyncio.get_running_loop()
        async for message in self._stoppable.call(
            queue.get, timeout=1, fail=False
        ):
            if message is not None:
                task = loop.create_task(
                    self._handle_message(message, message_send, state)
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def _handle_message(
        self,
        message: AbstractIncomingMessage,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": message.routing_key or "",
            "headers": _encode_headers(message.headers),
            "payload": message.body,
            "bindings": {
                "amqp": {
                    "exchange": message.exchange or "",
                    "routing_key": message.routing_key or "",
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                    "reply_to": message.reply_to,
                    "content_type": message.content_type,
                    "content_encoding": message.content_encoding,
                    "delivery_mode": message.delivery_mode,
                    "priority": message.priority,
                    "expiration": message.expiration,
                    "timestamp": message.timestamp,
                    "type": message.type,
                    "user_id": message.user_id,
                    "app_id": message.app_id,
                }
            },
            "state": state.copy(),
        }
        await self._app(scope, _receive, _Send(message, message_send))

    def stop(self) -> None:
        self._stoppable.stop()
