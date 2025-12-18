import asyncio
from asyncio import Event
from asyncio import Task
from typing import Optional

import aio_pika
from aio_pika import connect_robust
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractRobustChannel
from aio_pika.abc import AbstractRobustConnection
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope


def run(
    app: AMGIApplication,
    queue: str,
    url: str = "amqp://guest:guest@localhost/",
    durable: bool = True,
) -> None:
    asyncio.run(_run(app, queue, url, durable))


async def _run(app: AMGIApplication, queue: str, url: str, durable: bool) -> None:
    server = Server(app, queue, url, durable)
    await server.serve()


def _run_cli(
    app: AMGIApplication,
    queues: list[str],
    url: str = "amqp://guest:guest@localhost/",
    durable: bool = True,
) -> None:
    run(app, queues[0], url, durable)


class _MessageReceive:
    def __init__(self, message: IncomingMessage) -> None:
        self._message = message

    async def __call__(self) -> MessageReceiveEvent:
        return {
            "type": "message.receive",
            "id": str(self._message.delivery_tag),
            "headers": [
                (key.encode(), value.encode())
                for key, value in (self._message.headers or {}).items()
            ],
            "payload": self._message.body,
        }


class _MessageSend:
    def __init__(self, channel: AbstractRobustChannel) -> None:
        self._channel = channel

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await self._channel.default_exchange.publish(
                aio_pika.Message(
                    body=event.get("payload", b""),
                    headers={
                        key.decode(): value.decode()
                        for key, value in event.get("headers", [])
                    },
                ),
                routing_key=event["address"],
            )


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        queue: str,
        url: str,
        durable: bool = True,
    ) -> None:
        self._app = app
        self._queue = queue
        self._url = url
        self._durable = durable
        self._stop_event = Event()
        self._tasks: set[Task[None]] = set()
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractRobustChannel] = None

    async def _handle_message(self, message: IncomingMessage) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": self._queue,
        }

        try:
            await self._app(
                scope, _MessageReceive(message), _MessageSend(self._channel)
            )
            await message.ack()
        except Exception:
            await message.nack(requeue=True)

    async def serve(self) -> None:
        self._connection = await connect_robust(self._url)
        self._channel = await self._connection.channel()

        queue = await self._channel.declare_queue(self._queue, durable=self._durable)

        async with Lifespan(self._app) as state:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if self._stop_event.is_set():
                        break

                    task = asyncio.create_task(self._handle_message(message))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

            await asyncio.gather(*self._tasks)

        await self._channel.close()
        await self._connection.close()

    def stop(self) -> None:
        self._stop_event.set()
