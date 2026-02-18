import asyncio
from typing import Generator
from uuid import uuid4

import pytest
from amgi_sqs_event_source_mapping import MessageSend
from amgi_sqs_event_source_mapping import SqsBatchFailureError
from amgi_sqs_event_source_mapping import SqsEventSourceMappingHandler
from test_utils import MockApp
from testcontainers.localstack import LocalStackContainer


@pytest.fixture(scope="module")
def localstack_container() -> Generator[LocalStackContainer, None, None]:
    with LocalStackContainer(image="ghcr.io/asyncfast/localstack:4.9.2").with_services(
        "sqs"
    ) as localstack_container:
        yield localstack_container


@pytest.mark.integration
async def test_sqs_handler_record_send(
    localstack_container: LocalStackContainer,
) -> None:
    app = MockApp()
    sqs_event_source_mapping_handler = SqsEventSourceMappingHandler(
        app,
        region_name=localstack_container.region_name,
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="testcontainers-localstack",
        aws_secret_access_key="testcontainers-localstack",
        lifespan=False,
    )
    sqs_client = localstack_container.get_client("sqs")

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
            "test": {"StringValue": "test", "DataType": "StringValue"}
        }

    await call_task


@pytest.mark.integration
async def test_sqs_handler_record_send_invalid_message(
    localstack_container: LocalStackContainer,
) -> None:
    app = MockApp()
    sqs_event_source_mapping_handler = SqsEventSourceMappingHandler(
        app,
        region_name=localstack_container.region_name,
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="testcontainers-localstack",
        aws_secret_access_key="testcontainers-localstack",
        lifespan=False,
    )
    sqs_client = localstack_container.get_client("sqs")

    send_queue_name = f"send-{uuid4()}"
    sqs_client.create_queue(
        QueueName=send_queue_name, Attributes={"MaximumMessageSize": "1024"}
    )

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
        )
    )
    async with app.call() as (scope, receive, send):
        with pytest.raises(SqsBatchFailureError):
            await send(
                {
                    "type": "message.send",
                    "address": send_queue_name,
                    "headers": [],
                    "payload": b"a" * 1025,
                }
            )

    await call_task


@pytest.mark.integration
async def test_message_send(localstack_container: LocalStackContainer) -> None:
    sqs_client = localstack_container.get_client("sqs")

    send_queue_name = f"send-{uuid4()}"
    send_queue_url = sqs_client.create_queue(QueueName=send_queue_name)["QueueUrl"]

    async with MessageSend(
        region_name=localstack_container.region_name,
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="testcontainers-localstack",
        aws_secret_access_key="testcontainers-localstack",
    ) as message_send:
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
            "test": {"StringValue": "test", "DataType": "StringValue"}
        }
