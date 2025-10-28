import asyncio
from collections import deque
from collections.abc import Iterable
from typing import Any
from typing import Optional

from aiobotocore.session import get_session
from amgi_common import Lifespan
from amgi_common import Stoppable
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope


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


class _Send:
    def __init__(self, client: Any, queue_url: str) -> None:
        self._client = client
        self._queue_url = queue_url

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=event["id"],
            )
        if event["type"] == "message.send":
            queue_url_response = await self._client.get_queue_url(
                QueueName=event["address"]
            )
            await self._client.send_message(
                QueueUrl=queue_url_response["QueueUrl"],
                MessageBody=(
                    "" if event["payload"] is None else event["payload"].decode()
                ),
                MessageAttributes={
                    name.decode(): {"BinaryValue": value, "DataType": "BinaryValue"}
                    for name, value in event["headers"]
                },
            )


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *queues: str,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        self._app = app
        self._queues = queues
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._stoppable = Stoppable()

    async def serve(self) -> None:
        session = get_session()

        async with session.create_client(
            "sqs",
            region_name=self._region_name,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        ) as client:
            queue_urls = zip(
                await asyncio.gather(
                    *(client.get_queue_url(QueueName=queue) for queue in self._queues)
                ),
                self._queues,
            )

            async with Lifespan(self._app) as state:
                await asyncio.gather(
                    *(
                        self._queue_loop(client, queue_url["QueueUrl"], queue, state)
                        for queue_url, queue in queue_urls
                    )
                )

    async def _queue_loop(
        self, client: Any, queue_url: str, queue_name: str, state: dict[str, Any]
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
                }
                await self._app(scope, _Receive(messages), _Send(client, queue_url))

    def stop(self) -> None:
        self._stoppable.stop()
