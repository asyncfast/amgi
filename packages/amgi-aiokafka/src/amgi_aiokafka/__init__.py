import asyncio
import logging
import signal
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Lock
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import ConsumerRecord
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope


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
        )
        async with self._consumer:
            async with Lifespan(self._app):
                await self.main_loop()

        if self._producer is not None:
            await self._producer.stop()

    async def main_loop(self) -> None:
        message: ConsumerRecord[bytes, bytes]
        loop = asyncio.get_running_loop()
        stop_task = loop.create_task(self._stop_event.wait())
        while True:
            get_one_task = loop.create_task(self._consumer.getone())
            await asyncio.wait(
                (
                    get_one_task,
                    stop_task,
                ),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop_event.is_set():
                get_one_task.cancel()
                break
            message = await get_one_task

            encoded_headers = [(key.encode(), value) for key, value in message.headers]
            scope: MessageScope = {
                "type": "message",
                "amgi": {"version": "1.0", "spec_version": "1.0"},
                "address": message.topic,
                "headers": encoded_headers,
                "payload": message.value,
            }
            await self._app(scope, self.receive, self.send)

    async def receive(self) -> AMGIReceiveEvent:
        raise NotImplementedError()

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            async with self._producer_lock:
                producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
                await producer.start()
                self._producer = producer
        return self._producer

    async def _send_message(
        self,
        topic: str,
        headers: Iterable[Tuple[bytes, bytes]],
        payload: Optional[bytes],
    ) -> None:
        producer = await self._get_producer()
        encoded_headers = [(key.decode(), value) for key, value in headers]
        await producer.send(topic, headers=encoded_headers, value=payload)

    async def send(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await self._send_message(
                event["address"], event["headers"], event.get("payload")
            )

    def stop(self) -> None:
        self._stop_event.set()


async def _serve(server: Server, loop: AbstractEventLoop) -> None:
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, server.stop)

    await server.serve()
