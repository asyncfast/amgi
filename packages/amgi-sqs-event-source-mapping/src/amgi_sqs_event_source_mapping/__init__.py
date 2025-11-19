import asyncio
import base64
import re
import sys
from collections import defaultdict
from collections import deque
from collections.abc import Iterable
from typing import Any
from typing import Literal
from typing import Optional
from typing import TypedDict

import boto3
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


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


class _Send:
    def __init__(self, sqs_client: Any, message_ids: Iterable[str]) -> None:
        self._sqs_client = sqs_client
        self.message_ids = set(message_ids)

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.message_ids.discard(event["id"])
        if event["type"] == "message.send":
            queue_url_response = await asyncio.to_thread(
                self._sqs_client.get_queue_url, QueueName=event["address"]
            )
            await asyncio.to_thread(
                self._sqs_client.send_message,
                QueueUrl=queue_url_response["QueueUrl"],
                MessageBody=(
                    "" if event["payload"] is None else event["payload"].decode()
                ),
                MessageAttributes={
                    name.decode(): {
                        "StringValue": value.decode(),
                        "DataType": "StringValue",
                    }
                    for name, value in event["headers"]
                },
            )


class SqsHandler:
    def __init__(
        self,
        app: AMGIApplication,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        self._app = app
        self._loop = asyncio.get_event_loop()
        self._sqs_client = boto3.client(
            "sqs",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def __call__(
        self, event: _SqsEventSourceMapping, context: Any
    ) -> _BatchItemFailures:
        return self._loop.run_until_complete(self._call(event))

    async def _call(self, event: _SqsEventSourceMapping) -> _BatchItemFailures:
        event_source_arn_records = defaultdict(list)
        for record in event["Records"]:
            event_source_arn_records[record["eventSourceARN"]].append(record)

        unacked_message_ids = await asyncio.gather(
            *(
                self._call_source_batch(event_source_arn, records)
                for event_source_arn, records in event_source_arn_records.items()
            )
        )

        return {
            "batchItemFailures": [
                {"itemIdentifier": message_id}
                for unacked_message_ids_batch in unacked_message_ids
                for message_id in unacked_message_ids_batch
            ]
        }

    async def _call_source_batch(
        self, event_source_arn: str, records: Iterable[_Record]
    ) -> Iterable[str]:
        event_source_arn_match = EVENT_SOURCE_ARN_PATTERN.match(event_source_arn)
        message_ids = [record["messageId"] for record in records]
        if event_source_arn_match is None:
            return message_ids
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": event_source_arn_match["queue"],
        }

        records_send = _Send(self._sqs_client, message_ids)
        await self._app(scope, _Receive(records), records_send)
        return records_send.message_ids
