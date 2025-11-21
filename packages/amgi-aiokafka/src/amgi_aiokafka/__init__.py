import logging
from asyncio import Lock
from collections import deque
from collections.abc import Awaitable
from collections.abc import Iterable
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

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


logger = logging.getLogger("amgi-aiokafka.error")


def run(
    app: AMGIApplication,
    *topics: Iterable[str],
    bootstrap_servers: Union[str, list[str]] = "localhost",
    group_id: Optional[str] = None,
) -> None:
    server = Server(
        app, *topics, bootstrap_servers=bootstrap_servers, group_id=group_id
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    topics: list[str],
    bootstrap_servers: Optional[list[str]] = None,
    group_id: Optional[str] = None,
) -> None:
    run(
        app,
        *topics,
        bootstrap_servers=bootstrap_servers or ["localhost"],
        group_id=group_id,
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
        message_send: Callable[[MessageSendEvent], Awaitable[None]],
        ackable_consumer: bool,
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


class Server:
    _consumer: AIOKafkaConsumer

    def __init__(
        self,
        app: AMGIApplication,
        *topics: Iterable[str],
        bootstrap_servers: Union[str, list[str]],
        group_id: Optional[str],
    ) -> None:
        self._app = app
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._ackable_consumer = self._group_id is not None
        self._producer: Optional[AIOKafkaProducer] = None
        self._producer_lock = Lock()
        self._stoppable = Stoppable()

    async def serve(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
        )
        async with self._consumer:
            async with Lifespan(self._app) as state:
                await self._main_loop(state)

        if self._producer is not None:
            await self._producer.stop()

    async def _main_loop(self, state: dict[str, Any]) -> None:
        async for messages in self._stoppable.call(
            self._consumer.getmany, timeout_ms=1000
        ):
            for topic_partition, records in messages.items():
                if records:
                    scope: MessageScope = {
                        "type": "message",
                        "amgi": {"version": "1.0", "spec_version": "1.0"},
                        "address": topic_partition.topic,
                        "state": state.copy(),
                    }

                    message_receive_ids = {
                        f"{record.topic}:{record.partition}:{record.offset}": {
                            TopicPartition(
                                record.topic, record.partition
                            ): record.offset
                            + 1
                        }
                        for record in records
                    }

                    await self._app(
                        scope,
                        _Receive(records),
                        _Send(
                            self._consumer,
                            message_receive_ids,
                            self._message_send,
                            self._ackable_consumer,
                        ),
                    )

    async def _get_producer(self) -> AIOKafkaProducer:
        async with self._producer_lock:
            if self._producer is None:
                producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
                await producer.start()
                self._producer = producer
        return self._producer

    async def _message_send(self, event: MessageSendEvent) -> None:
        producer = await self._get_producer()
        encoded_headers = [(key.decode(), value) for key, value in event["headers"]]

        key = event.get("bindings", {}).get("kafka", {}).get("key")
        await producer.send(
            event["address"],
            headers=encoded_headers,
            value=event.get("payload"),
            key=key,
        )

    def stop(self) -> None:
        self._stoppable.stop()
