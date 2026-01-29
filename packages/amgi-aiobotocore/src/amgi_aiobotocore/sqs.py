import asyncio
import sys
from collections import deque
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Sequence
from types import TracebackType
from typing import Any
from typing import AsyncContextManager

from aiobotocore.session import get_session
from amgi_common import Lifespan
from amgi_common import OperationBatcher
from amgi_common import OperationCacher
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


def run(
    app: AMGIApplication,
    *queues: str,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(
        app,
        *queues,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        message_send=message_send,
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    queues: list[str],
    region_name: str | None = None,
    endpoint_url: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> None:
    run(
        app,
        *queues,
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def _encode_message_attributes(
    message_attributes: dict[str, Any],
) -> Iterable[tuple[bytes, bytes]]:
    for name, value in message_attributes.items():
        encoded_value = (
            value["StringValue"].encode()
            if value["DataType"] == "StringValue"
            else value["BinaryValue"]
        )
        yield name.encode(), encoded_value


class _Receive:
    def __init__(self, messages: Iterable[Any]) -> None:
        self._deque = deque(messages)

    async def __call__(self) -> MessageReceiveEvent:
        message = self._deque.popleft()
        encoded_headers = list(
            _encode_message_attributes(message.get("MessageAttributes", {}))
        )
        return {
            "type": "message.receive",
            "id": message["ReceiptHandle"],
            "headers": encoded_headers,
            "payload": message["Body"].encode(),
            "more_messages": len(self._deque) != 0,
        }


async def _get_queue_url(client: Any, queue_name: str) -> str:
    queue_url_response = await client.get_queue_url(QueueName=queue_name)
    queue_url = queue_url_response["QueueUrl"]
    assert isinstance(queue_url, str)
    return queue_url


class _QueueUrlCache:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._operation_cacher = OperationCacher[str, str](
            lambda queue_name: _get_queue_url(client, queue_name)
        )

    async def get_queue_url(self, queue_name: str) -> str:
        return await self._operation_cacher.get(queue_name)


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


class _DeleteBatcher:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._operation_batcher = OperationBatcher(
            self.delete_message_batch, lambda item: item[0], batch_size=10
        )

    async def delete_message_batch(
        self, batch: Iterable[tuple[str, str]]
    ) -> Sequence[None | Exception]:
        queue_urls, receipt_handles = zip(*batch)
        assert len(set(queue_urls)) == 1
        queue_url = queue_urls[0]

        delete_message_batch_response = await self._client.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": receipt_handle}
                for i, receipt_handle in enumerate(receipt_handles)
            ],
        )

        return tuple(
            _generate_response_failures(
                delete_message_batch_response, len(receipt_handles)
            )
        )

    async def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        await self._operation_batcher.enqueue((queue_url, receipt_handle))


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

        send_message_batch_response = await self._client.send_message_batch(
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
        session = get_session()

        self._client_context = session.create_client(
            "sqs",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    async def __aenter__(self) -> Self:
        self._client = await self._client_context.__aenter__()
        self._send_batcher = _SendBatcher(self._client)
        self._queue_url_cache = _QueueUrlCache(self._client)

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
        await self._client_context.__aexit__(exc_type, exc_val, exc_tb)


class _Send:
    def __init__(
        self,
        delete_batcher: _DeleteBatcher,
        queue_url: str,
        message_send: _MessageSendT,
    ) -> None:
        self._queue_url = queue_url
        self._delete_batcher = delete_batcher
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._delete_batcher.delete_message(
                self._queue_url,
                event["id"],
            )
        if event["type"] == "message.send":
            await self._message_send(event)


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *queues: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._queues = queues
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._message_send = message_send or MessageSend(
            region_name, endpoint_url, aws_access_key_id, aws_secret_access_key
        )

        self._stoppable = Stoppable()

    async def serve(self) -> None:
        session = get_session()

        async with (
            session.create_client(
                "sqs",
                region_name=self._region_name,
                endpoint_url=self._endpoint_url,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
            ) as client,
            self._message_send as message_send,
        ):
            delete_batcher = _DeleteBatcher(client)

            queue_urls = zip(
                await asyncio.gather(
                    *(_get_queue_url(client, queue) for queue in self._queues)
                ),
                self._queues,
            )
            async with Lifespan(self._app) as state:
                await asyncio.gather(
                    *(
                        self._queue_loop(
                            client,
                            queue_url,
                            queue,
                            delete_batcher,
                            message_send,
                            state,
                        )
                        for queue_url, queue in queue_urls
                    )
                )

    async def _queue_loop(
        self,
        client: Any,
        queue_url: str,
        queue_name: str,
        delete_batcher: _DeleteBatcher,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        async for messages_response in self._stoppable.call(
            client.receive_message,
            QueueUrl=queue_url,
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
        ):
            messages = messages_response.get("Messages", ())
            if messages:
                scope: MessageScope = {
                    "type": "message",
                    "amgi": {"version": "1.0", "spec_version": "1.0"},
                    "address": queue_name,
                    "state": state.copy(),
                    "extensions": {"message.ack.out_of_order": {}},
                }
                await self._app(
                    scope,
                    _Receive(messages),
                    _Send(delete_batcher, queue_url, message_send),
                )

    def stop(self) -> None:
        self._stoppable.stop()
