import asyncio
from typing import Generator
from unittest.mock import Mock
from uuid import uuid4

import boto3
import pytest
from amgi_sqs_event_source_mapping import MessageSend
from amgi_sqs_event_source_mapping import SqsBatchFailureError
from amgi_sqs_event_source_mapping import SqsEventSourceMappingHandler
from moto import mock_aws
from moto.core.models import MockAWS
from test_utils import MockApp


@pytest.fixture
def mock_sqs() -> Generator[MockAWS, None, None]:
    with mock_aws() as _mock_aws:
        yield _mock_aws


async def test_sqs_handler_record_send(
    mock_sqs: MockAWS,
) -> None:
    app = MockApp()
    sqs_event_source_mapping_handler = SqsEventSourceMappingHandler(
        app,
        region_name="us-east-1",
        lifespan=False,
    )
    sqs_client = boto3.client("sqs", region_name="us-east-1")

    send_queue_name = f"send-{uuid4()}"
    send_queue_url = sqs_client.create_queue(QueueName=send_queue_name)["QueueUrl"]

    call_task = asyncio.get_running_loop().create_task(
        sqs_event_source_mapping_handler._call(
            {
                "Records": [
                    {
                        "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                        "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                        "body": "Test message.",
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "SentTimestamp": "1545082649183",
                            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                            "ApproximateFirstReceiveTimestamp": "1545082649185",
                        },
                        "messageAttributes": {
                            "myAttribute": {
                                "stringValue": "myValue",
                                "stringListValues": [],
                                "binaryListValues": [],
                                "dataType": "String",
                            }
                        },
                        "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                        "awsRegion": "us-east-2",
                    }
                ]
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_queue_name,
                "headers": [(b"test", b"test")],
                "payload": b"test",
            }
        )

        messages_response = sqs_client.receive_message(
            QueueUrl=send_queue_url, MessageAttributeNames=["All"]
        )
        assert "Messages" in messages_response
        assert len(messages_response["Messages"]) == 1
        message = messages_response["Messages"][0]
        assert message["Body"] == "test"
        assert message["MessageAttributes"] == {
            "test": {"StringValue": "test", "DataType": "String"}
        }

    await call_task


async def test_sqs_handler_record_send_invalid_message(
    mock_sqs: MockAWS,
) -> None:
    app = MockApp()
    sqs_event_source_mapping_handler = SqsEventSourceMappingHandler(
        app,
        region_name="us-east-1",
        lifespan=False,
    )
    sqs_client = boto3.client("sqs", region_name="us-east-1")

    send_queue_name = f"send-{uuid4()}"
    sqs_client.create_queue(QueueName=send_queue_name)

    call_task = asyncio.get_running_loop().create_task(
        sqs_event_source_mapping_handler._call(
            {
                "Records": [
                    {
                        "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                        "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                        "body": "Test message.",
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "SentTimestamp": "1545082649183",
                            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                            "ApproximateFirstReceiveTimestamp": "1545082649185",
                        },
                        "messageAttributes": {
                            "myAttribute": {
                                "stringValue": "myValue",
                                "stringListValues": [],
                                "binaryListValues": [],
                                "dataType": "String",
                            }
                        },
                        "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                        "awsRegion": "us-east-2",
                    }
                ]
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        with pytest.raises(SqsBatchFailureError):
            await send(
                {
                    "type": "message.send",
                    "address": send_queue_name,
                    "headers": [],
                    "bindings": {"sqs": {"delay_seconds": 901}},
                }
            )

    await call_task


async def test_message_send(mock_sqs: MockAWS) -> None:
    sqs_client = boto3.client("sqs", region_name="us-east-1")

    send_queue_name = f"send-{uuid4()}"
    send_queue_url = sqs_client.create_queue(QueueName=send_queue_name)["QueueUrl"]

    async with MessageSend(region_name="us-east-1") as message_send:
        await message_send(
            {
                "type": "message.send",
                "address": send_queue_name,
                "payload": b"test",
                "headers": [(b"test", b"test")],
            }
        )

        messages_response = sqs_client.receive_message(
            QueueUrl=send_queue_url, MessageAttributeNames=["All"]
        )
        assert "Messages" in messages_response
        assert len(messages_response["Messages"]) == 1
        message = messages_response["Messages"][0]
        assert message["Body"] == "test"
        assert message["MessageAttributes"] == {
            "test": {"StringValue": "test", "DataType": "String"}
        }
