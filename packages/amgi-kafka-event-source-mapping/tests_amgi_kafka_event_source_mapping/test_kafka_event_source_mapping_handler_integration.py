import asyncio
from typing import Generator
from unittest.mock import Mock
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin import NewTopic
from aiokafka.errors import KafkaConnectionError
from aiokafka.errors import TopicAlreadyExistsError
from amgi_kafka_event_source_mapping import KafkaEventSourceMappingHandler
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
async def send_topic(bootstrap_server: str) -> str:
    return await create_topic(bootstrap_server, f"send-{uuid4()}")


@pytest.mark.integration
async def test_kafka_event_source_mapping_handler_message_send(
    bootstrap_server: str, send_topic: str
) -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, lifespan=False
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": bootstrap_server,
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": None,
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [],
                        },
                    ]
                },
            },
        )
    )
    async with AIOKafkaConsumer(
        send_topic, bootstrap_servers=bootstrap_server, auto_offset_reset="earliest"
    ) as consumer:
        async with app.call() as (scope, receive, send):
            assert scope == {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "mytopic",
                "bindings": {"kafka": {"key": None}},
                "headers": [],
                "payload": b"Hello, this is a test.",
                "state": {},
            }

            await send(
                {
                    "type": "message.send",
                    "address": send_topic,
                    "headers": [(b"test", b"test1")],
                    "payload": b"test1",
                }
            )

        await call_task

        message = await consumer.getone()
        assert message.topic == send_topic
        assert message.value == b"test1"
        assert message.headers == (("test", b"test1"),)


@pytest.mark.integration
def test_kafka_event_source_mapping_handler_message_send_error() -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, lifespan=False
    )

    with pytest.raises(KafkaConnectionError):
        kafka_event_source_mapping_handler(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": str(uuid4()),
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": None,
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [],
                        },
                    ]
                },
            },
            Mock(),
        )
