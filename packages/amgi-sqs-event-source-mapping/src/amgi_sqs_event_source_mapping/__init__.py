import asyncio
import base64
import hashlib
import itertools
import re
import signal
import sys
import warnings
from collections import defaultdict
from collections import deque
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Sequence
from functools import cached_property
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import Literal
from typing import TypedDict

import boto3
from amgi_common import Lifespan
from amgi_common import OperationBatcher
from amgi_common import OperationCacher
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

if sys.version_info >= (3, 11):
    from typing import NotRequired
    from typing import Self
else:
    from typing_extensions import NotRequired
    from typing_extensions import Self


_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


class _AttributeValue(TypedDict):
    stringValue: NotRequired[str]
    binaryValue: NotRequired[str]
    stringListValues: NotRequired[list[str]]
    binaryListValues: NotRequired[list[str]]
    dataType: str


class _Record(TypedDict):
    messageId: str
    receiptHandle: str
    body: str
    attributes: dict[str, str]
    messageAttributes: dict[str, _AttributeValue]
    md5OfBody: str
    eventSource: Literal["aws:sqs"]
    eventSourceARN: str
    awsRegion: str


class _SqsEventSourceMapping(TypedDict):
    Records: list[_Record]


class _ItemIdentifier(TypedDict):
    itemIdentifier: str


class _BatchItemFailures(TypedDict):
    batchItemFailures: list[_ItemIdentifier]


EVENT_SOURCE_ARN_PATTERN = re.compile(
    r"^arn:aws:sqs:[A-Za-z0-9-]+:\d+:(?P<queue>[A-Za-z.\-_]+)$"
)


def _encode_message_attributes(
    message_attributes: dict[str, Any],
) -> Iterable[tuple[bytes, bytes]]:
    for name, value in message_attributes.items():
        encoded_value = (
            base64.b64decode(value["binaryValue"])
            if value["dataType"] == "Binary"
            else value["stringValue"].encode()
        )
        yield name.encode(), encoded_value


class _Receive:
    def __init__(self, records: Iterable[_Record]) -> None:
        self._deque = deque(records)

    async def __call__(self) -> MessageReceiveEvent:
        message = self._deque.popleft()
        encoded_headers = list(
            _encode_message_attributes(message.get("messageAttributes", {}))
        )
        return {
            "type": "message.receive",
            "id": message["messageId"],
            "headers": encoded_headers,
            "payload": message["body"].encode(),
            "more_messages": len(self._deque) != 0,
        }


class _QueueUrlCache:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._operation_cacher = OperationCacher(self._get_queue_url)

    async def get_queue_url(self, queue_name: str) -> str:
        return await self._operation_cacher.get(queue_name)

    async def _get_queue_url(self, queue_name: str) -> str:
        queue_url_response = await asyncio.to_thread(
            self._client.get_queue_url, QueueName=queue_name
        )
        queue_url = queue_url_response["QueueUrl"]
        assert isinstance(queue_url, str)
        return queue_url


class SqsBatchFailureError(IOError):
    def __init__(self, sender_fault: bool, code: str, message: str):
        self.sender_fault = sender_fault
        self.code = code
        super().__init__(message)


def _generate_response_failures(
    response: dict[str, Any], count: int
) -> Generator[SqsBatchFailureError | None, None, None]:
    failed_map = {int(failed["Id"]): failed for failed in response.get("Failed", ())}
    for i in range(count):
        failed = failed_map.get(i)
        if failed:
            yield SqsBatchFailureError(
                failed["SenderFault"], failed["Code"], failed["Message"]
            )
        else:
            yield None


class _SendBatcher:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._operation_batcher = OperationBatcher(
            self.send_message_batch, lambda item: item[0], batch_size=10
        )

    async def send_message_batch(
        self,
        batch: Iterable[tuple[str, bytes | None, Iterable[tuple[bytes, bytes]]]],
    ) -> Sequence[None | Exception]:
        queue_urls, batch_payloads, batch_headers = zip(*batch)
        assert len(set(queue_urls)) == 1
        queue_url = queue_urls[0]
        send_message_batch_response = await asyncio.to_thread(
            self._client.send_message_batch,
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": str(i),
                    "MessageBody": ("" if payload is None else payload.decode()),
                    "MessageAttributes": {
                        name.decode(): {
                            "StringValue": value.decode(),
                            "DataType": "StringValue",
                        }
                        for name, value in headers
                    },
                }
                for i, (payload, headers) in enumerate(
                    zip(batch_payloads, batch_headers)
                )
            ],
        )

        return tuple(
            _generate_response_failures(
                send_message_batch_response, len(batch_payloads)
            )
        )

    async def send_message(
        self,
        queue_url: str,
        payload: bytes | None,
        headers: Iterable[tuple[bytes, bytes]],
    ) -> None:
        await self._operation_batcher.enqueue((queue_url, payload, headers))


class MessageSend:
    def __init__(
        self,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._client_instantiated = False

    async def __aenter__(self) -> Self:
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        queue_url = await self._queue_url_cache.get_queue_url(event["address"])
        await self._send_batcher.send_message(
            queue_url, event["payload"], event["headers"]
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._client_instantiated:
            self._client.close()

    @cached_property
    def _client(self) -> Any:
        client = boto3.client(
            "sqs",
            region_name=self._region_name,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        self._client_instantiated = True
        return client

    @cached_property
    def _queue_url_cache(self) -> _QueueUrlCache:
        return _QueueUrlCache(self._client)

    @cached_property
    def _send_batcher(self) -> _SendBatcher:
        return _SendBatcher(self._client)


class _Send:
    def __init__(self, message_ids: set[str], message_send: _MessageSendT) -> None:
        self.message_ids = message_ids
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.message_ids.discard(event["id"])
        if event["type"] == "message.send":
            await self._message_send(event)


class SqsEventSourceMappingHandler:
    def __init__(
        self,
        app: AMGIApplication,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        lifespan: bool = True,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._message_send: _MessageSendT | None = None
        self._message_send_context = message_send or MessageSend(
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._loop = asyncio.new_event_loop()
        self._lifespan = lifespan
        self._lifespan_context: Lifespan | None = None
        self._state: dict[str, Any] = {}
        try:
            self._loop.add_signal_handler(signal.SIGTERM, self._sigterm_handler)
        except NotImplementedError:
            # Windows / non-main thread: no signal handlers via asyncio
            pass

    def __call__(
        self, event: _SqsEventSourceMapping, context: Any
    ) -> _BatchItemFailures:
        return self._loop.run_until_complete(self._call(event))

    async def _call(self, event: _SqsEventSourceMapping) -> _BatchItemFailures:
        if not self._lifespan_context and self._lifespan:
            self._lifespan_context = Lifespan(self._app, self._state)
            await self._lifespan_context.__aenter__()
        if self._message_send is None:
            self._message_send = await self._message_send_context.__aenter__()
        event_source_arn_records = defaultdict(list)
        corrupted_message_ids = []
        for record in event["Records"]:
            if hashlib.md5(record["body"].encode()).hexdigest() == record["md5OfBody"]:
                event_source_arn_records[record["eventSourceARN"]].append(record)
            else:
                corrupted_message_ids.append(record["messageId"])

        unacked_message_ids = await asyncio.gather(
            *(
                self._call_source_batch(event_source_arn, records, self._message_send)
                for event_source_arn, records in event_source_arn_records.items()
            )
        )

        return {
            "batchItemFailures": [
                {"itemIdentifier": message_id}
                for message_id in itertools.chain(
                    *unacked_message_ids, corrupted_message_ids
                )
            ]
        }

    async def _call_source_batch(
        self,
        event_source_arn: str,
        records: Iterable[_Record],
        message_send: _MessageSendT,
    ) -> Iterable[str]:
        event_source_arn_match = EVENT_SOURCE_ARN_PATTERN.match(event_source_arn)
        message_ids = {record["messageId"] for record in records}
        if event_source_arn_match is None:
            return message_ids
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": event_source_arn_match["queue"],
            "state": self._state.copy(),
            "extensions": {"message.ack.out_of_order": {}},
        }

        records_send = _Send(message_ids, message_send)
        await self._app(scope, _Receive(records), records_send)
        return records_send.message_ids

    def _sigterm_handler(self) -> None:
        self._loop.run_until_complete(self._shutdown())

    async def _shutdown(self) -> None:
        if self._lifespan_context:
            await self._lifespan_context.__aexit__(None, None, None)
        if self._message_send is not None:
            await self._message_send_context.__aexit__(None, None, None)


def __getattr__(name: str) -> object:
    if name == "SqsHandler":
        warnings.warn(
            "SqsHandler is deprecated; use SqsEventSourceMappingHandler instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        globals()[name] = SqsEventSourceMappingHandler
        return SqsEventSourceMappingHandler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
