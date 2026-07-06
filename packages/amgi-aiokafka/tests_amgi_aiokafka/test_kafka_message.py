import asyncio
import logging
from asyncio import Lock
from collections.abc import AsyncGenerator
from typing import Generator
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin import NewTopic
from aiokafka.errors import CommitFailedError
from aiokafka.errors import TopicAlreadyExistsError
from amgi_aiokafka import _CancelListener
from amgi_aiokafka import _run_cli
from amgi_aiokafka import run
from amgi_aiokafka import Server
from amgi_types import MessageAckEvent
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    with KafkaContainer(image="ghcr.io/asyncfast/cp-kafka:7.6.0") as kafka_container:
        yield kafka_container


@pytest.fixture
def bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()  # type: ignore


async def create_topic(bootstrap_server: str, topic: str) -> str:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_server)
    await admin.start()

    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:  # pragma: no cover
        pass
    finally:
        await admin.close()

    return topic


@pytest.fixture
async def receive_topic(bootstrap_server: str) -> str:
    return await create_topic(bootstrap_server, f"receive-{uuid4()}")


@pytest.fixture
async def send_topic(bootstrap_server: str) -> str:
    return await create_topic(bootstrap_server, f"send-{uuid4()}")


@pytest.fixture
async def app(
    bootstrap_server: str, receive_topic: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    server = Server(
        app,
        receive_topic,
        bootstrap_servers=bootstrap_server,
        group_id=str(uuid4()),
        auto_offset_reset="earliest",
    )

    async with app.lifespan(server=server):
        yield app


@pytest.mark.integration
async def test_message(bootstrap_server: str, app: MockApp, receive_topic: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(
        receive_topic, b"value", b"key", headers=[("test", b"test")]
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "address": receive_topic,
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [(b"test", b"test")],
            "payload": b"value",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "type": "message",
            "state": {},
        }

        message_ack_event: MessageAckEvent = {
            "type": "message.ack",
        }
        await send(message_ack_event)

    await producer.stop()


@pytest.mark.integration
async def test_message_send(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server, auto_offset_reset="earliest"
    ) as consumer:
        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [(b"test", b"test")],
                    "payload": b"test",
                }
            )

        message = await consumer.getone()
        assert message.topic == send_topic
        assert message.value == b"test"
        assert message.headers == (("test", b"test"),)

    await producer.stop()


@pytest.mark.integration
async def test_message_send_kafka_key(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")

    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server, auto_offset_reset="earliest"
    ) as consumer:
        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [],
                    "bindings": {"kafka": {"key": b"test"}},
                }
            )

        message = await consumer.getone()
        assert message.topic == send_topic
        assert message.key == b"test"

    await producer.stop()


@pytest.mark.integration
async def test_lifespan(bootstrap_server: str, receive_topic: str) -> None:
    app = MockApp()
    server = Server(
        app,
        receive_topic,
        bootstrap_servers=bootstrap_server,
        group_id=None,
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    state_item = uuid4()

    async with app.lifespan({"item": state_item}, server):
        await producer.send_and_wait(
            receive_topic,
            b"",
        )
        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": receive_topic,
                "bindings": {"kafka": {"key": None}},
                "headers": [],
                "payload": b"",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "message",
                "state": {"item": state_item},
            }


@pytest.mark.integration
def test_run(bootstrap_server: str, receive_topic: str) -> None:
    assert_run_can_terminate(run, receive_topic, bootstrap_servers=bootstrap_server)


@pytest.mark.integration
def test_run_cli(bootstrap_server: str, receive_topic: str) -> None:
    assert_run_can_terminate(
        _run_cli, [receive_topic], bootstrap_servers=bootstrap_server
    )


@pytest.mark.integration
async def test_message_receive_not_callable(
    bootstrap_server: str, app: MockApp, receive_topic: str, send_topic: str
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    await producer.start()

    await producer.send_and_wait(receive_topic, b"")
    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()


async def test_revoked_partition_task_cancellation_get_ignored() -> None:
    mock_awaitable = AsyncMock(side_effect=asyncio.CancelledError)
    cancel_listener = _CancelListener(
        Lock(),
        {TopicPartition("test", 0): asyncio.create_task(mock_awaitable())},
        set(),
    )

    await cancel_listener.on_partitions_revoked([TopicPartition("test", 0)])


async def test_revoked_partition_task_exception_gets_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_awaitable = AsyncMock(side_effect=Exception("error"))
    partition_task = asyncio.create_task(mock_awaitable())
    await asyncio.sleep(0)
    cancel_listener = _CancelListener(
        Lock(), {TopicPartition("test", 0): partition_task}, set()
    )

    with caplog.at_level(logging.ERROR, logger="amgi-aiokafka.error"):
        await cancel_listener.on_partitions_revoked([TopicPartition("test", 0)])

    assert any(
        "Partition task failed during revoke" in record.message
        for record in caplog.records
    )


async def test_revoked_partition_task_base_exception_is_raised() -> None:
    base_exception = BaseException("error")
    mock_awaitable = AsyncMock(side_effect=base_exception)
    partition_task = asyncio.create_task(mock_awaitable())
    await asyncio.sleep(0)
    cancel_listener = _CancelListener(
        Lock(), {TopicPartition("test", 0): partition_task}, set()
    )

    with pytest.raises(BaseException) as exc_info:
        await cancel_listener.on_partitions_revoked([TopicPartition("test", 0)])

    assert exc_info.value == base_exception


async def test_main_loop_partition_task_cancellation_gets_ignored() -> None:
    server = Server(MockApp(), "test", bootstrap_servers="unused", group_id=None)
    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    mock_consumer.getmany.return_value = {TopicPartition("test", 0): [Mock()]}

    async def cancel_partition(*args: object) -> None:
        server.stop()
        raise asyncio.CancelledError

    with patch.object(
        server, "_handle_partition_records", side_effect=cancel_partition
    ):
        await server._main_loop(mock_consumer, {}, AsyncMock())


async def test_main_loop_partition_task_exception_gets_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = Server(MockApp(), "test", bootstrap_servers="unused", group_id=None)
    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    mock_consumer.getmany.return_value = {TopicPartition("test", 0): [Mock()]}

    async def fail_partition(*args: object) -> None:
        server.stop()
        raise Exception("error")

    with (
        patch.object(server, "_handle_partition_records", side_effect=fail_partition),
        caplog.at_level(logging.ERROR, logger="amgi-aiokafka.error"),
    ):
        await server._main_loop(mock_consumer, {}, AsyncMock())

    assert any("Partition task failed" in record.message for record in caplog.records)


async def test_main_loop_partition_task_base_exception_gets_raised() -> None:
    server = Server(MockApp(), "test", bootstrap_servers="unused", group_id=None)
    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    mock_consumer.getmany.return_value = {TopicPartition("test", 0): [Mock()]}
    base_exception = BaseException("error")

    with (
        patch.object(server, "_handle_partition_records", side_effect=base_exception),
        pytest.raises(BaseException) as exc_info,
    ):
        await server._main_loop(mock_consumer, {}, AsyncMock())

    assert exc_info.value == base_exception


async def test_handle_partition_records_logs_commit_failed_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = Server(MockApp(), "test", bootstrap_servers="unused", group_id="test")
    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    mock_consumer.commit.side_effect = CommitFailedError
    mock_record = Mock()
    mock_record.offset = 0

    with (
        patch.object(server, "_handle_record", return_value=True),
        caplog.at_level(logging.WARNING, logger="amgi-aiokafka.error"),
    ):
        await server._handle_partition_records(
            mock_consumer, TopicPartition("test", 0), [mock_record], AsyncMock(), {}
        )

    assert any(
        "Commit failed for test[0] while committing offset 1" in record.message
        for record in caplog.records
    )
