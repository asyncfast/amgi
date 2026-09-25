import asyncio
import sys
from asyncio import Task
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import AsyncContextManager

import pulsar.asyncio
from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = ["MessageSend", "Server", "run"]

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


def run(
    app: AMGIApplication,
    *topics: str,
    service_url: str = "pulsar://localhost:6650",
    subscription_name: str = "amgi",
    negative_ack_redelivery_delay_ms: int = 60000,
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(
        app,
        *topics,
        service_url=service_url,
        subscription_name=subscription_name,
        negative_ack_redelivery_delay_ms=negative_ack_redelivery_delay_ms,
        message_send=message_send,
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    topics: list[str],
    service_url: str = "pulsar://localhost:6650",
    subscription_name: str = "amgi",
) -> None:
    run(app, *topics, service_url=service_url, subscription_name=subscription_name)


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


def _encode_headers(properties: dict[str, str] | None) -> list[tuple[bytes, bytes]]:
    if not properties:
        return []
    return [(key.encode(), value.encode()) for key, value in properties.items()]


def _decode_headers(
    headers: Sequence[tuple[bytes, bytes]],
) -> dict[str, str]:
    return {key.decode(): value.decode() for key, value in headers}


class MessageSend:
    def __init__(self, service_url: str) -> None:
        self._service_url = service_url
        self._client: pulsar.asyncio.Client | None = None
        self._producers: dict[str, pulsar.asyncio.Producer] = {}
        self._producers_lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        self._client = pulsar.asyncio.Client(self._service_url)
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        producer = await self._get_producer(event["address"])
        key = event.get("bindings", {}).get("pulsar", {}).get("key")

        await producer.send(
            event.get("payload") or b"",
            properties=_decode_headers(event["headers"]),
            partition_key=key,
        )

    async def _get_producer(self, topic: str) -> pulsar.asyncio.Producer:
        producer = self._producers.get(topic)
        if producer is not None:
            return producer

        async with self._producers_lock:
            producer = self._producers.get(topic)
            if producer is None:
                producer = await self._require_client().create_producer(topic)
                self._producers[topic] = producer
        return producer

    def _require_client(self) -> pulsar.asyncio.Client:
        if self._client is None:
            raise RuntimeError("MessageSend not initialized")
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            self._producers.clear()
            client, self._client = self._client, None
            await client.close()


class _Send:
    def __init__(
        self,
        consumer: pulsar.asyncio.Consumer,
        message: pulsar.Message,
        message_send: _MessageSendT,
    ) -> None:
        self._consumer = consumer
        self._message = message
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._consumer.acknowledge(self._message)
        elif event["type"] == "message.nack":
            await self._consumer.negative_acknowledge(self._message)
        elif event["type"] == "message.send":
            await self._message_send(event)


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *topics: str,
        service_url: str,
        subscription_name: str,
        negative_ack_redelivery_delay_ms: int = 60000,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._topics = topics
        self._service_url = service_url
        self._subscription_name = subscription_name
        self._negative_ack_redelivery_delay_ms = negative_ack_redelivery_delay_ms
        self._message_send = message_send or MessageSend(service_url)
        self._stoppable = Stoppable()
        self._tasks = set[Task[None]]()

    async def serve(self) -> None:
        client = pulsar.asyncio.Client(self._service_url)
        try:
            async with self._message_send as message_send:
                consumers = [
                    await self._subscribe(client, topic) for topic in self._topics
                ]
                async with Lifespan(self._app) as state:
                    await asyncio.gather(
                        *(
                            self._topic_loop(consumer, topic, message_send, state)
                            for consumer, topic in zip(consumers, self._topics)
                        )
                    )
                    await asyncio.gather(*self._tasks, return_exceptions=True)
        finally:
            await client.close()

    async def _subscribe(
        self, client: pulsar.asyncio.Client, topic: str
    ) -> pulsar.asyncio.Consumer:
        return await client.subscribe(
            topic,
            self._subscription_name,
            negative_ack_redelivery_delay_ms=self._negative_ack_redelivery_delay_ms,
        )

    async def _topic_loop(
        self,
        consumer: pulsar.asyncio.Consumer,
        topic: str,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        loop = asyncio.get_running_loop()
        async for message in self._stoppable.call(consumer.receive):
            task = loop.create_task(
                self._handle_message(consumer, topic, message, message_send, state)
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _handle_message(
        self,
        consumer: pulsar.asyncio.Consumer,
        topic: str,
        message: pulsar.Message,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": topic,
            "headers": _encode_headers(message.properties()),
            "payload": message.data(),
            "bindings": {"pulsar": {"key": message.partition_key()}},
            "state": state.copy(),
        }
        await self._app(scope, _receive, _Send(consumer, message, message_send))

    def stop(self) -> None:
        self._stoppable.stop()
