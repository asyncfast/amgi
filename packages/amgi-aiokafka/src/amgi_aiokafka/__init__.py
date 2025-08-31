import asyncio
import logging
import signal
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Lock
from collections import deque
from typing import Awaitable
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRecord
from aiokafka import TopicPartition
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent


logger = logging.getLogger("amgi-aiokafka.error")


def run(
    app: AMGIApplication,
    *topics: Iterable[str],
    bootstrap_servers: Union[str, List[str]] = "localhost",
    group_id: Optional[str] = None,
) -> None:
    server = Server(
        app, *topics, bootstrap_servers=bootstrap_servers, group_id=group_id
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_serve(server, loop))


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


class _RecordsEvents:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        records: Iterable[ConsumerRecord],
        message_send: Callable[[MessageSendEvent], Awaitable[None]],
    ) -> None:
        self._deque = deque(records)
        self._consumer = consumer
        self._message_send = message_send
        self._message_receive_ids: dict[str, dict[TopicPartition, int]] = {}

    async def receive(self) -> MessageReceiveEvent:
        record = self._deque.popleft()
        message_receive_id = f"{record.topic}:{record.partition}:{record.offset}"
        self._message_receive_ids[message_receive_id] = {
            TopicPartition(record.topic, record.partition): record.offset + 1
        }

        encoded_headers = [(key.encode(), value) for key, value in record.headers]
        message_receive_event: MessageReceiveEvent = {
            "type": "message.receive",
            "id": message_receive_id,
            "headers": encoded_headers,
            "payload": record.value,
            "more_messages": len(self._deque) != 0,
        }
        return message_receive_event

    async def send(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
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
        bootstrap_servers: Union[str, List[str]],
        group_id: Optional[str],
    ) -> None:
        self._app = app
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._producer: Optional[AIOKafkaProducer] = None
        self._producer_lock = Lock()
        self._stop_event = Event()

    async def serve(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
        )
        async with self._consumer:
            async with Lifespan(self._app):
                await self._main_loop()

        if self._producer is not None:
            await self._producer.stop()

    async def _main_loop(self) -> None:
        loop = asyncio.get_running_loop()
        stop_task = loop.create_task(self._stop_event.wait())
        while True:
            getmany_task = loop.create_task(self._consumer.getmany(timeout_ms=1000))
            await asyncio.wait(
                (
                    getmany_task,
                    stop_task,
                ),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop_event.is_set():
                getmany_task.cancel()
                break
            messages = await getmany_task

            for topic_partition, records in messages.items():
                if records:
                    scope: MessageScope = {
                        "type": "message",
                        "amgi": {"version": "1.0", "spec_version": "1.0"},
                        "address": topic_partition.topic,
                    }

                    records_events = _RecordsEvents(
                        self._consumer, records, self._message_send
                    )

                    await self._app(
                        scope,
                        records_events.receive,
                        records_events.send,
                    )

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            async with self._producer_lock:
                producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
                await producer.start()
                self._producer = producer
        return self._producer

    async def _message_send(self, event: MessageSendEvent) -> None:
        producer = await self._get_producer()
        encoded_headers = [(key.decode(), value) for key, value in event["headers"]]
        await producer.send(
            event["address"], headers=encoded_headers, value=event.get("payload")
        )

    def stop(self) -> None:
        self._stop_event.set()


async def _serve(server: Server, loop: AbstractEventLoop) -> None:
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, server.stop)

    await server.serve()
