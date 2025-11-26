import asyncio
import base64
from collections.abc import AsyncGenerator
from collections.abc import Generator
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

import boto3
import pytest
from amgi_sqs_event_source_mapping import SqsHandler
from test_utils import MockApp


@pytest.fixture
def mock_sqs_client() -> Generator[Mock, None, None]:
    with patch.object(boto3, "client") as mock_sqs_client:
        yield mock_sqs_client


@pytest.fixture
async def app_sqs_handler(
    mock_sqs_client: Mock,
) -> AsyncGenerator[tuple[MockApp, SqsHandler], None]:
    app = MockApp()
    sqs_handler = SqsHandler(app)

    loop = asyncio.get_event_loop()

    call_task = loop.create_task(
        sqs_handler._call(
            {"Records": []},
        )
    )
    async with app.lifespan():
        yield app, sqs_handler
        shutdown_task = loop.create_task(sqs_handler._shutdown())

    await shutdown_task
    await call_task


@pytest.fixture
def app(app_sqs_handler: tuple[MockApp, SqsHandler]) -> MockApp:
    return app_sqs_handler[0]


@pytest.fixture
def sqs_handler(app_sqs_handler: tuple[MockApp, SqsHandler]) -> SqsHandler:
    return app_sqs_handler[1]


async def test_sqs_handler_records(app: MockApp, sqs_handler: SqsHandler) -> None:
    call_task = asyncio.get_running_loop().create_task(
        sqs_handler._call(
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
                    },
                    {
                        "messageId": "2e1424d4-f796-459a-8184-9c92662be6da",
                        "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                        "body": "Test message.",
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "SentTimestamp": "1545082650636",
                            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                            "ApproximateFirstReceiveTimestamp": "1545082650649",
                        },
                        "messageAttributes": {},
                        "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                        "awsRegion": "us-east-2",
                    },
                ]
            },
        )
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "my-queue",
            "state": {},
        }

        assert await receive() == {
            "type": "message.receive",
            "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "headers": [(b"myAttribute", b"myValue")],
            "payload": b"Test message.",
            "more_messages": True,
        }
        await send(
            {
                "type": "message.ack",
                "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            }
        )
        assert await receive() == {
            "type": "message.receive",
            "id": "2e1424d4-f796-459a-8184-9c92662be6da",
            "headers": [],
            "payload": b"Test message.",
            "more_messages": False,
        }
        await send(
            {
                "type": "message.ack",
                "id": "2e1424d4-f796-459a-8184-9c92662be6da",
            }
        )

    batch_item_failures = await call_task
    assert batch_item_failures == {"batchItemFailures": []}


async def test_sqs_handler_record_nack(app: MockApp, sqs_handler: SqsHandler) -> None:
    call_task = asyncio.get_running_loop().create_task(
        sqs_handler._call(
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
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "my-queue",
            "state": {},
        }

        assert await receive() == {
            "type": "message.receive",
            "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "headers": [(b"myAttribute", b"myValue")],
            "payload": b"Test message.",
            "more_messages": False,
        }
        await send(
            {
                "type": "message.nack",
                "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "message": "failed to process",
            }
        )

    batch_item_failures = await call_task
    assert batch_item_failures == {
        "batchItemFailures": [
            {"itemIdentifier": "059f36b4-87a3-44ab-83d2-661975830a7d"}
        ]
    }


async def test_sqs_handler_record_unacked(
    app: MockApp, sqs_handler: SqsHandler
) -> None:
    call_task = asyncio.get_running_loop().create_task(
        sqs_handler._call(
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
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "my-queue",
            "state": {},
        }

        assert await receive() == {
            "type": "message.receive",
            "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "headers": [(b"myAttribute", b"myValue")],
            "payload": b"Test message.",
            "more_messages": False,
        }

    batch_item_failures = await call_task
    assert batch_item_failures == {
        "batchItemFailures": [
            {"itemIdentifier": "059f36b4-87a3-44ab-83d2-661975830a7d"}
        ]
    }


async def test_sqs_handler_record_message_attribute_binary_value(
    app: MockApp,
    sqs_handler: SqsHandler,
) -> None:
    call_task = asyncio.get_running_loop().create_task(
        sqs_handler._call(
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
                                "binaryValue": base64.b64encode(b"myValue").decode(),
                                "dataType": "Binary",
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
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "my-queue",
            "state": {},
        }

        assert await receive() == {
            "type": "message.receive",
            "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "headers": [(b"myAttribute", b"myValue")],
            "payload": b"Test message.",
            "more_messages": False,
        }
        await send(
            {
                "type": "message.ack",
                "id": "059f36b4-87a3-44ab-83d2-661975830a7d",
            }
        )

    batch_item_failures = await call_task
    assert batch_item_failures == {"batchItemFailures": []}


async def test_sqs_handler_record_corrupted(
    app: MockApp, sqs_handler: SqsHandler
) -> None:
    call_task = asyncio.get_running_loop().create_task(
        sqs_handler._call(
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
                        "md5OfBody": "00000000000000000000000000000000",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                        "awsRegion": "us-east-2",
                    }
                ]
            },
        )
    )

    batch_item_failures = await call_task
    assert batch_item_failures == {
        "batchItemFailures": [
            {"itemIdentifier": "059f36b4-87a3-44ab-83d2-661975830a7d"}
        ]
    }


async def test_lifespan(
    mock_sqs_client: Mock,
) -> None:
    app = MockApp()
    sqs_handler = SqsHandler(app)

    loop = asyncio.get_event_loop()
    state_item = uuid4()

    lifespan_task = loop.create_task(
        sqs_handler._call(
            {"Records": []},
        )
    )
    async with app.lifespan({"item": state_item}):
        await lifespan_task

        call_task = asyncio.get_running_loop().create_task(
            sqs_handler._call(
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
            assert scope == {
                "type": "message",
                "amgi": {"version": "1.0", "spec_version": "1.0"},
                "address": "my-queue",
                "state": {"item": state_item},
            }

        await call_task
        shutdown_task = loop.create_task(sqs_handler._shutdown())

    await shutdown_task
