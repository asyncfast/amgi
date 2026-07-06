import asyncio
import logging
import sys
from asyncio import Lock
from asyncio import Task
from collections.abc import Awaitable
from collections.abc import Callable
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Literal
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRebalanceListener
from aiokafka import ConsumerRecord
from aiokafka import TopicPartition
from aiokafka.errors import CommitFailedError
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


if TYPE_CHECKING:  # pragma: no cover

    class _ConsumerRebalanceListener:
        pass

else:
    _ConsumerRebalanceListener = ConsumerRebalanceListener

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


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


class _Send:
    def __init__(
        self,
        message_send: _MessageSendT,
    ) -> None:
        self._message_send = message_send
        self.acked = False

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.acked = True
        if event["type"] == "message.send":
            await self._message_send(event)


class MessageSend:
    def __init__(self, bootstrap_servers: str | list[str]) -> None:
        self._bootstrap_servers = bootstrap_servers

    async def __aenter__(self) -> Self:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._producer.start()
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        encoded_headers = [(key.decode(), value) for key, value in event["headers"]]

        key = event.get("bindings", {}).get("kafka", {}).get("key")
        await self._producer.send_and_wait(
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
        await self._producer.stop()


class _CancelListener(_ConsumerRebalanceListener):
    def __init__(
        self,
        partition_lock: Lock,
        partition_tasks: dict[TopicPartition, Task[None]],
        assigned_partitions: set[TopicPartition],
    ) -> None:
        self._partition_lock = partition_lock
        self._partition_tasks = partition_tasks
        self._assigned_partitions = assigned_partitions

    async def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        async with self._partition_lock:
            self._assigned_partitions.difference_update(revoked)
            revoked_tasks = [
                task
                for topic_partition in revoked
                if (task := self._partition_tasks.get(topic_partition)) is not None
            ]
        for revoked_task in revoked_tasks:
            revoked_task.cancel()
        results = await asyncio.gather(*revoked_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, asyncio.CancelledError):
                continue
            if isinstance(result, Exception):
                logger.exception("Partition task failed during revoke", exc_info=result)
            elif isinstance(result, BaseException):
                raise result

    async def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        async with self._partition_lock:
            self._assigned_partitions.update(assigned)


class Server:
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
        self._stoppable = Stoppable()
        self._partition_lock = Lock()
        self._partition_tasks: dict[TopicPartition, Task[None]] = {}
        self._assigned_partitions = set[TopicPartition]()

    async def serve(self) -> None:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
        )

        consumer.subscribe(
            self._topics,
            listener=_CancelListener(
                self._partition_lock, self._partition_tasks, self._assigned_partitions
            ),
        )
        async with consumer, self._message_send as message_send:
            async with Lifespan(self._app) as state:
                await self._main_loop(consumer, state, message_send)

    async def _main_loop(
        self,
        consumer: AIOKafkaConsumer,
        state: dict[str, Any],
        message_send: _MessageSendT,
    ) -> None:
        async for messages in self._stoppable.call(consumer.getmany, timeout_ms=1000):
            async with self._partition_lock:
                partition_tasks = {
                    topic_partition: asyncio.create_task(
                        self._handle_partition_records(
                            consumer, topic_partition, records, message_send, state
                        )
                    )
                    for topic_partition, records in messages.items()
                    if topic_partition in self._assigned_partitions
                    or self._group_id is None
                }

                self._partition_tasks.update(partition_tasks)

            results = await asyncio.gather(
                *partition_tasks.values(), return_exceptions=True
            )

            async with self._partition_lock:
                self._partition_tasks.clear()

            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    continue
                if isinstance(result, Exception):
                    logger.exception("Partition task failed", exc_info=result)
                elif isinstance(result, BaseException):
                    raise result

    async def _handle_partition_records(
        self,
        consumer: AIOKafkaConsumer,
        topic_partition: TopicPartition,
        records: list[ConsumerRecord],
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        offset = None
        try:
            for record in records:
                if await self._handle_record(record, message_send, state):
                    offset = record.offset + 1
                else:
                    break
        finally:
            if self._group_id is not None and offset is not None:
                try:
                    await asyncio.shield(consumer.commit({topic_partition: offset}))
                except CommitFailedError:
                    logger.warning(
                        "Commit failed for %s[%s] while committing offset %s",
                        topic_partition.topic,
                        topic_partition.partition,
                        offset,
                    )

    async def _handle_record(
        self,
        record: ConsumerRecord,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> bool:
        encoded_headers = [(key.encode(), value) for key, value in record.headers]

        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": record.topic,
            "headers": encoded_headers,
            "payload": record.value,
            "bindings": {"kafka": {"key": record.key}},
            "state": state.copy(),
        }

        send = _Send(
            message_send,
        )
        await self._app(
            scope,
            _receive,
            send,
        )
        return send.acked

    def stop(self) -> None:
        self._stoppable.stop()
