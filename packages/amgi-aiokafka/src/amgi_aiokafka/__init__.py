import asyncio
import logging
import sys
from asyncio import Lock
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Literal
from typing import TYPE_CHECKING
from typing import TypeVar

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRecord
from aiokafka import TopicPartition
from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

if TYPE_CHECKING:

    class _ConsumerRebalanceListener: ...

else:
    from aiokafka import ConsumerRebalanceListener as _ConsumerRebalanceListener


T = TypeVar("T")

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


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


class _Send:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        record: ConsumerRecord,
        ackable_consumer: bool,
        message_send: _MessageSendT,
    ) -> None:
        self._consumer = consumer
        self._message_send = message_send
        self._record = record
        self._ackable_consumer = ackable_consumer
        self.acked = False

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.acked = True
            if self._ackable_consumer:
                topic_partition = TopicPartition(
                    self._record.topic, self._record.partition
                )
                offset = self._record.offset + 1
                await self._consumer.commit({topic_partition: offset})
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


class _HoldingLock:
    def __init__(self, lock: Lock):
        self._lock = lock
        self._holding = False

    def release(self) -> None:
        if self._holding:
            self._lock.release()
            self._holding = False

    async def acquire(self) -> None:
        await self._lock.acquire()
        self._holding = True


async def _iter_with_lock(
    iterable: AsyncIterable[T],
    lock: Lock,
) -> AsyncIterator[T]:
    iterator = iterable.__aiter__()
    holding_lock = _HoldingLock(lock)
    try:
        while True:
            holding_lock.release()
            await holding_lock.acquire()
            try:
                yield await iterator.__anext__()
            except StopAsyncIteration:
                break
    finally:
        holding_lock.release()


class _RebalanceListener(_ConsumerRebalanceListener):
    def __init__(self, rebalance_lock: Lock) -> None:
        self._rebalance_lock = rebalance_lock

    async def on_partitions_revoked(
        self,
        revoked: list[TopicPartition],
    ) -> None:
        async with self._rebalance_lock:
            pass

    async def on_partitions_assigned(
        self,
        assigned: list[TopicPartition],
    ) -> None:
        async with self._rebalance_lock:
            pass


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
        self._rebalance_lock = Lock()
        self._stoppable = Stoppable()

    async def serve(self) -> None:
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
        )
        self._consumer.subscribe(
            topics=list(self._topics),
            listener=_RebalanceListener(self._rebalance_lock),
        )
        async with self._consumer, self._message_send as message_send:
            async with Lifespan(self._app) as state:
                await self._main_loop(state, message_send)

    async def _main_loop(
        self, state: dict[str, Any], message_send: _MessageSendT
    ) -> None:
        async for messages in _iter_with_lock(
            self._stoppable.call(self._consumer.getmany, timeout_ms=1000),
            self._rebalance_lock,
        ):
            await asyncio.gather(
                *[
                    self._handle_partition_records(records, message_send, state)
                    for topic_partition, records in messages.items()
                ]
            )

    async def _handle_partition_records(
        self,
        records: list[ConsumerRecord],
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        for record in records:
            if not await self._handle_record(record, message_send, state):
                break

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
            self._consumer,
            record,
            self._ackable_consumer,
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
