from collections.abc import AsyncGenerator
from typing import Any
from typing import cast
from typing import Generator
from uuid import uuid4

import pytest
from aiobotocore.session import get_session
from amgi_aiobotocore.sqs import _run_cli
from amgi_aiobotocore.sqs import run
from amgi_aiobotocore.sqs import Server
from amgi_aiobotocore.sqs import SqsBatchFailureError
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from test_utils import assert_run_can_terminate
from test_utils import MockApp


class _StrMatcher:
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, str)


@pytest.fixture(scope="module")
def moto_endpoint() -> Generator[str, None, None]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
async def sqs_client(moto_endpoint: str) -> AsyncGenerator[Any, None]:
    session = get_session()
    async with session.create_client(
        "sqs",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        yield client


@pytest.fixture
async def queue_name_url(sqs_client: Any) -> tuple[str, str]:
    queue_name = f"receive-{uuid4()}"
    create_queue_response = await sqs_client.create_queue(
        QueueName=queue_name, Attributes={"VisibilityTimeout": "0"}
    )
    return queue_name, cast(
        str,
        create_queue_response["QueueUrl"],
    )


@pytest.fixture
def queue_name(queue_name_url: tuple[str, str]) -> str:
    return queue_name_url[0]


@pytest.fixture
def queue_url(queue_name_url: tuple[str, str]) -> str:
    return queue_name_url[1]


@pytest.fixture
async def app(queue_name: str, moto_endpoint: str) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        queue_name,
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    async with app.lifespan(server=server):
        yield app


async def test_message(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
        MessageAttributes={
            "string-value": {"StringValue": "string", "DataType": "String.Value"},
            "bytes-value": {"BinaryValue": b"bytes", "DataType": "Binary.Value"},
        },
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": queue_name,
            "headers": [(b"string-value", b"string"), (b"bytes-value", b"bytes")],
            "payload": b"value",
            "state": {},
        }

        message_ack_event: MessageAckEvent = {
            "type": "message.ack",
        }
        await send(message_ack_event)

    assert "Messages" not in (
        await sqs_client.receive_message(
            QueueUrl=queue_url,
        )
    )


async def test_message_nack(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
        MessageAttributes={
            "string-value": {"StringValue": "string", "DataType": "String.Value"},
            "bytes-value": {"BinaryValue": b"bytes", "DataType": "Binary.Value"},
        },
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": queue_name,
            "headers": [(b"string-value", b"string"), (b"bytes-value", b"bytes")],
            "payload": b"value",
            "state": {},
        }

        message_ack_event: MessageNackEvent = {
            "type": "message.nack",
            "message": "",
        }
        await send(message_ack_event)

    messages_response = await sqs_client.receive_message(
        QueueUrl=queue_url,
    )
    assert "Messages" in messages_response
    assert len(messages_response["Messages"]) == 1


async def test_message_send(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    send_queue_name = f"send-{uuid4()}"
    create_queue_response = await sqs_client.create_queue(QueueName=send_queue_name)
    send_queue_url = create_queue_response["QueueUrl"]

    await sqs_client.send_message(
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

        messages_response = await sqs_client.receive_message(
            QueueUrl=send_queue_url, MessageAttributeNames=["All"]
        )
        assert "Messages" in messages_response
        assert len(messages_response["Messages"]) == 1
        message = messages_response["Messages"][0]
        assert message["Body"] == "test"
        assert message["MessageAttributes"] == {
            "test": {"StringValue": "test", "DataType": "String"}
        }


async def test_message_send_invalid_message(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    send_queue_name = f"send-{uuid4()}"
    await sqs_client.create_queue(QueueName=send_queue_name)
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
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


async def test_message_send_does_not_cache_invalid_queue_url(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    send_queue_name = f"send-{uuid4()}"
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
    )

    async with app.call() as (scope, receive, send):
        with pytest.raises(
            match="GetQueueUrl operation: The specified queue does not exist"
        ):
            await send(
                {
                    "type": "message.send",
                    "address": send_queue_name,
                    "headers": [(b"test", b"test")],
                    "payload": b"test",
                }
            )

        create_queue_response = await sqs_client.create_queue(QueueName=send_queue_name)
        send_queue_url = create_queue_response["QueueUrl"]

        await send(
            {
                "type": "message.send",
                "address": send_queue_name,
                "headers": [],
                "payload": b"test",
            }
        )

        messages_response = await sqs_client.receive_message(
            QueueUrl=send_queue_url, MessageAttributeNames=["All"]
        )
        assert "Messages" in messages_response
        assert len(messages_response["Messages"]) == 1
        message = messages_response["Messages"][0]
        assert message["Body"] == "test"


async def test_lifespan(
    queue_url: str,
    queue_name: str,
    moto_endpoint: str,
    sqs_client: Any,
) -> None:
    app = MockApp()
    server = Server(
        app,
        queue_name,
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody="value",
            MessageAttributes={
                "string-value": {"StringValue": "string", "DataType": "String.Value"},
                "bytes-value": {"BinaryValue": b"bytes", "DataType": "Binary.Value"},
            },
        )

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": queue_name,
                "headers": [(b"string-value", b"string"), (b"bytes-value", b"bytes")],
                "payload": b"value",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "message",
                "state": {"item": state_item},
            }


def test_run(queue_name: str, moto_endpoint: str) -> None:
    assert_run_can_terminate(
        run,
        queue_name,
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def test_run_cli(queue_name: str, moto_endpoint: str) -> None:
    assert_run_can_terminate(
        _run_cli,
        [queue_name],
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


async def test_message_receive_not_callable(
    app: MockApp, queue_url: str, queue_name: str, sqs_client: Any
) -> None:
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody="value",
        MessageAttributes={
            "string-value": {"StringValue": "string", "DataType": "String.Value"},
            "bytes-value": {"BinaryValue": b"bytes", "DataType": "Binary.Value"},
        },
    )
    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
