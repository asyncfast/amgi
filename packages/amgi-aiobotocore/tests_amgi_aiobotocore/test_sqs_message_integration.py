from collections.abc import AsyncGenerator
from typing import Any
from typing import cast
from uuid import uuid4

import pytest
from amgi_aiobotocore.sqs import Server
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from test_utils import MockApp
from testcontainers.localstack import LocalStackContainer


class _StrMatcher:
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, str)


@pytest.fixture(scope="module")
async def localstack_container() -> AsyncGenerator[LocalStackContainer, None]:
    with LocalStackContainer(
        image="localstack/localstack:4.9.2"
    ) as localstack_container:
        yield localstack_container


@pytest.fixture
async def sqs_client(localstack_container: LocalStackContainer) -> Any:
    return localstack_container.get_client("sqs")


@pytest.fixture
def queue_name_url(sqs_client: Any) -> tuple[str, str]:
    queue_name = f"receive-{uuid4()}"
    queue_url = cast(
        str,
        sqs_client.create_queue(
            QueueName=queue_name, Attributes={"VisibilityTimeout": "0"}
        )["QueueUrl"],
    )
    return queue_name, queue_url


@pytest.fixture
def queue_name(queue_name_url: tuple[str, str]) -> str:
    return queue_name_url[0]


@pytest.fixture
def queue_url(queue_name_url: tuple[str, str]) -> str:
    return queue_name_url[1]


@pytest.fixture
async def app(
    queue_name: str, localstack_container: LocalStackContainer
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        queue_name,
        region_name=localstack_container.region_name,
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="testcontainers-localstack",
        aws_secret_access_key="testcontainers-localstack",
    )
    async with app.lifespan(server=server):
        yield app


async def test_message(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
        MessageAttributes={
            "string-value": {"StringValue": "string", "DataType": "StringValue"},
            "bytes-value": {"BinaryValue": b"bytes", "DataType": "BinaryValue"},
        },
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": queue_name,
            "state": {},
        }
        message_receive = await receive()

        assert message_receive["type"] == "message.receive"
        assert message_receive == {
            "type": "message.receive",
            "id": _StrMatcher(),
            "headers": [(b"string-value", b"string"), (b"bytes-value", b"bytes")],
            "payload": b"value",
            "more_messages": False,
        }

        message_ack_event: MessageAckEvent = {
            "type": "message.ack",
            "id": message_receive["id"],
        }
        await send(message_ack_event)

    assert "Messages" not in (
        sqs_client.receive_message(
            QueueUrl=queue_url,
        )
    )


async def test_message_nack(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
        MessageAttributes={
            "string-value": {"StringValue": "string", "DataType": "StringValue"},
            "bytes-value": {"BinaryValue": b"bytes", "DataType": "BinaryValue"},
        },
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": queue_name,
            "state": {},
        }
        message_receive = await receive()

        assert message_receive["type"] == "message.receive"
        assert message_receive == {
            "type": "message.receive",
            "id": _StrMatcher(),
            "headers": [(b"string-value", b"string"), (b"bytes-value", b"bytes")],
            "payload": b"value",
            "more_messages": False,
        }

        message_ack_event: MessageNackEvent = {
            "type": "message.nack",
            "id": message_receive["id"],
            "message": "",
        }
        await send(message_ack_event)

    messages_response = sqs_client.receive_message(
        QueueUrl=queue_url,
    )
    assert "Messages" in messages_response
    assert len(messages_response["Messages"]) == 1


async def test_message_send(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    send_queue_name = f"send-{uuid4()}"
    send_queue_url = sqs_client.create_queue(QueueName=send_queue_name)["QueueUrl"]
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
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


async def test_lifespan(
    queue_url: str,
    queue_name: str,
    localstack_container: LocalStackContainer,
    sqs_client: Any,
) -> None:
    app = MockApp()
    server = Server(
        app,
        queue_name,
        region_name=localstack_container.region_name,
        endpoint_url=localstack_container.get_url(),
        aws_access_key_id="testcontainers-localstack",
        aws_secret_access_key="testcontainers-localstack",
    )

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody="value",
            MessageAttributes={
                "string-value": {"StringValue": "string", "DataType": "StringValue"},
                "bytes-value": {"BinaryValue": b"bytes", "DataType": "BinaryValue"},
            },
        )

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": queue_name,
                "amgi": {"spec_version": "1.0", "version": "1.0"},
                "type": "message",
                "state": {"item": state_item},
            }
