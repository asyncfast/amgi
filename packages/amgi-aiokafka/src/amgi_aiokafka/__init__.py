import asyncio
import logging
import sys
from asyncio import Lock
from collections import deque
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Literal

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRecord
from aiokafka import TopicPartition
from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


logger = logging.getLogger("amgi-aiokafka.error")


AutoOffsetReset = Literal["earliest", "latest", "none"]


def run(
    app: AMGIApplication,
    *topics: str,
    bootstrap_servers: str | list[str] = "localhost",
    group_id: str | None = None,
    auto_offset_reset: AutoOffsetReset = "latest",
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(
        app,
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        message_send=message_send,
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    topics: list[str],
    bootstrap_servers: list[str] | None = None,
    group_id: str | None = None,
    auto_offset_reset: AutoOffsetReset = "latest",
) -> None:
    run(
        app,
        *topics,
        bootstrap_servers=bootstrap_servers or ["localhost"],
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
    )


class _Receive:
    def __init__(
        self,
        records: Iterable[ConsumerRecord],
    ) -> None:
        self._deque = deque(records)

    async def __call__(self) -> MessageReceiveEvent:
        record = self._deque.popleft()
        encoded_headers = [(key.encode(), value) for key, value in record.headers]

        return {
            "type": "message.receive",
            "id": f"{record.topic}:{record.partition}:{record.offset}",
            "headers": encoded_headers,
            "payload": record.value,
            "bindings": {"kafka": {"key": record.key}},
            "more_messages": len(self._deque) != 0,
        }


class _Send:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        message_receive_ids: dict[str, dict[TopicPartition, int]],
        ackable_consumer: bool,
        message_send: _MessageSendT,
    ) -> None:
        self._consumer = consumer
        self._message_send = message_send
        self._message_receive_ids = message_receive_ids
        self._ackable_consumer = ackable_consumer

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack" and self._ackable_consumer:
            offsets = self._message_receive_ids.pop(event["id"])
            await self._consumer.commit(offsets)
        if event["type"] == "message.send":
            await self._message_send(event)


class MessageSend:
    def __init__(self, bootstrap_servers: str | list[str]) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer = None
        self._producer_lock = Lock()

    async def __aenter__(self) -> Self:
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        producer = await self._get_producer()
        encoded_headers = [(key.decode(), value) for key, value in event["headers"]]

        key = event.get("bindings", {}).get("kafka", {}).get("key")
        await producer.send(
            event["address"],
            headers=encoded_headers,
            value=event.get("payload"),
            key=key,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            async with self._producer_lock:
                if self._producer is None:
                    producer = AIOKafkaProducer(
                        bootstrap_servers=self._bootstrap_servers
                    )
                    await producer.start()
                    self._producer = producer
        return self._producer


class Server:
    _consumer: AIOKafkaConsumer

    def __init__(
        self,
        app: AMGIApplication,
        *topics: str,
        bootstrap_servers: str | list[str],
        group_id: str | None,
        auto_offset_reset: AutoOffsetReset = "latest",
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._message_send = message_send or MessageSend(bootstrap_servers)
        self._ackable_consumer = self._group_id is not None
        self._stoppable = Stoppable()

    async def serve(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
        )
        async with self._consumer, self._message_send as message_send:
            async with Lifespan(self._app) as state:
                await self._main_loop(state, message_send)

    async def _main_loop(
        self, state: dict[str, Any], message_send: _MessageSendT
    ) -> None:
        async for messages in self._stoppable.call(
            self._consumer.getmany, timeout_ms=1000
        ):
            await asyncio.gather(
                *[
                    self._handle_partition_records(
                        topic_partition, records, message_send, state
                    )
                    for topic_partition, records in messages.items()
                ]
            )

    async def _handle_partition_records(
        self,
        topic_partition: TopicPartition,
        records: list[ConsumerRecord],
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        if records:
            scope: MessageScope = {
                "type": "message",
                "amgi": {"version": "1.0", "spec_version": "1.0"},
                "address": topic_partition.topic,
                "state": state.copy(),
            }

            message_receive_ids = {
                f"{record.topic}:{record.partition}:{record.offset}": {
                    TopicPartition(record.topic, record.partition): record.offset + 1
                }
                for record in records
            }

            await self._app(
                scope,
                _Receive(records),
                _Send(
                    self._consumer,
                    message_receive_ids,
                    self._ackable_consumer,
                    message_send,
                ),
            )

    def stop(self) -> None:
        self._stoppable.stop()
