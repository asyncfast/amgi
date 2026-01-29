import asyncio
import base64
import itertools
import logging
import signal
import sys
from asyncio import Task
from collections import deque
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Literal

from amgi_aiokafka import MessageSend as AioKafkaMessageSend
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from typing_extensions import NotRequired
from typing_extensions import TypedDict

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


_logger = logging.getLogger("amgi-kafka-event-source-mapping.error")


_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


_RecordHeaders = list[dict[str, list[int]]]


@dataclass
class _RecordNack:
    topic: str
    partition: int
    offset: int
    message: str

    def __str__(self) -> str:
        return f"Failed to process record topic={self.topic}, partition={self.partition}, offset={self.offset}, message={self.message}"


class _KafkaRecord(TypedDict):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestampType: str
    key: NotRequired[str | None]
    value: NotRequired[str | None]
    headers: NotRequired[_RecordHeaders]


class _KafkaEventSourceMapping(TypedDict):
    eventSource: str
    eventSourceArn: NotRequired[str]
    bootstrapServers: str
    records: dict[str, list[_KafkaRecord]]


class _Send:
    def __init__(
        self, record_nacks: dict[str, _RecordNack], message_send: _MessageSendT
    ) -> None:
        self._message_send = message_send
        self.record_nacks = record_nacks

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.record_nacks.pop(event["id"])
        if event["type"] == "message.nack":
            self.record_nacks[event["id"]].message = event["message"]
        if event["type"] == "message.send":
            await self._message_send(event)


def _encode_record_headers(
    headers: _RecordHeaders,
) -> Iterable[tuple[bytes, bytes]]:
    for header in headers:
        for header_name, header_value in header.items():
            yield header_name.encode(), bytes(header_value)


def _record_id(message: _KafkaRecord) -> str:
    topic = message["topic"]
    partition = message["partition"]
    offset = message["offset"]

    return f"{topic}:{partition}:{offset}"


class _Receive:
    def __init__(self, records: Iterable[_KafkaRecord]) -> None:
        self._deque = deque(records)

    async def __call__(self) -> MessageReceiveEvent:
        message = self._deque.popleft()
        headers = message.get("headers", [])
        encoded_headers = list(_encode_record_headers(headers))

        value = message.get("value")
        key = message.get("key")

        record_id = _record_id(message)

        return {
            "type": "message.receive",
            "id": record_id,
            "headers": encoded_headers,
            "payload": None if value is None else base64.b64decode(value),
            "bindings": {
                "kafka": {"key": None if key is None else base64.b64decode(key)}
            },
            "more_messages": len(self._deque) != 0,
        }


class _MessageSender:
    def __init__(self) -> None:
        self._send_tasks: dict[str, Task[_MessageSendT]] = {}
        self._async_exit_stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        await self._async_exit_stack.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._async_exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _create_sender(self, bootstrap_servers: list[str]) -> _MessageSendT:
        return await self._async_exit_stack.enter_async_context(
            AioKafkaMessageSend(bootstrap_servers=bootstrap_servers)
        )

    async def get_message_send(self, bootstrap_servers: str) -> _MessageSendT:
        task = self._send_tasks.get(bootstrap_servers)
        if task is None:
            task = asyncio.create_task(
                self._create_sender(bootstrap_servers.split(","))
            )
            self._send_tasks[bootstrap_servers] = task

        try:
            sender = await task
            return sender
        except Exception:
            if self._send_tasks.get(bootstrap_servers) is task:
                del self._send_tasks[bootstrap_servers]
            raise


class _MessageSendWrapper:
    def __init__(self, message_send_manager: _MessageSendManagerT) -> None:
        self._message_send_manager = message_send_manager

    async def __aenter__(self) -> Self:
        self._message_send = await self._message_send_manager.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._message_send_manager.__aexit__(exc_type, exc_val, exc_tb)

    async def get_message_send(self, bootstrap_servers: str) -> _MessageSendT:
        return self._message_send


def _partition_records_topic(records: list[_KafkaRecord]) -> str:
    topics = {record["topic"] for record in records}
    assert len(topics) == 1, f"Mixed topics: {topics}"
    return next(iter(topics))


class NackError(Exception):
    def __init__(self, nacks: Sequence[_RecordNack]):
        self._nacks = nacks

    def __str__(self) -> str:
        return "\n".join(str(nack) for nack in self._nacks)


class KafkaEventSourceMappingHandler:
    def __init__(
        self,
        app: AMGIApplication,
        lifespan: bool = True,
        on_nack: Literal["log", "error"] = "log",
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._on_nack = on_nack
        self._lifespan = lifespan
        self._loop = asyncio.new_event_loop()
        self._message_send_manager = (
            _MessageSender()
            if message_send is None
            else _MessageSendWrapper(message_send)
        )
        self._message_sender: _MessageSender | _MessageSendWrapper | None = None
        self._lifespan_context: Lifespan | None = None
        self._state: dict[str, Any] = {}
        self._client_instantiated = False

        try:
            self._loop.add_signal_handler(signal.SIGTERM, self._sigterm_handler)
        except NotImplementedError:
            # Windows / non-main thread: no signal handlers via asyncio
            pass

    def __call__(self, event: _KafkaEventSourceMapping, context: Any) -> None:
        return self._loop.run_until_complete(self._call(event))

    async def _call(self, event: _KafkaEventSourceMapping) -> None:
        if not self._lifespan_context and self._lifespan:
            self._lifespan_context = Lifespan(self._app, self._state)
            await self._lifespan_context.__aenter__()
        if self._message_sender is None:
            self._message_sender = await self._message_send_manager.__aenter__()

        record_nacks = await asyncio.gather(
            *(
                self._call_source_batch(
                    event["bootstrapServers"],
                    _partition_records_topic(records),
                    records,
                    self._message_sender,
                )
                for records in event["records"].values()
            )
        )

        all_nacks = tuple(itertools.chain.from_iterable(record_nacks))
        if self._on_nack == "error" and all_nacks:
            raise NackError(all_nacks)
        for nack in all_nacks:
            _logger.error(str(nack))

    async def _call_source_batch(
        self,
        bootstrap_servers: str,
        topic: str,
        records: Iterable[_KafkaRecord],
        message_sender: _MessageSender | _MessageSendWrapper,
    ) -> Iterable[_RecordNack]:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": topic,
            "state": self._state.copy(),
            "extensions": {"message.ack.out_of_order": {}},
        }
        message_send = await message_sender.get_message_send(bootstrap_servers)
        record_nacks = {
            _record_id(record): _RecordNack(
                record["topic"],
                record["partition"],
                record["offset"],
                "Ack not received",
            )
            for record in records
        }
        send = _Send(record_nacks, message_send)
        await self._app(scope, _Receive(records), send)

        return send.record_nacks.values()

    def _sigterm_handler(self) -> None:
        self._loop.run_until_complete(self._shutdown())

    async def _shutdown(self) -> None:
        if self._lifespan_context:
            await self._lifespan_context.__aexit__(None, None, None)
        if self._message_sender:
            await self._message_send_manager.__aexit__(None, None, None)
