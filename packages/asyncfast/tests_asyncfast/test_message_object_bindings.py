from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from asyncfast import Message
from asyncfast.bindings import KafkaKey
from asyncfast.bindings import SqsDelaySeconds
from asyncfast.bindings import SqsMessageDeduplicationId
from asyncfast.bindings import SqsMessageGroupId
from tests_asyncfast.conftest import MessageBenchmark


def test_message_binding_kafka_key(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[UUID, KafkaKey()]

    response = Response(key=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {"kafka": {"key": b"ec5e9f87-c896-4fb1-b028-8352ef654e05"}},
        "headers": [],
    }


def test_message_binding_kafka_key_int(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        key: Annotated[int, KafkaKey()]

    response = Response(key=10)

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {"kafka": {"key": b"10"}},
        "headers": [],
    }


def test_message_binding_sqs_delay_seconds(message_benchmark: MessageBenchmark) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        delay: Annotated[int, SqsDelaySeconds()]

    response = Response(delay=900)

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {"sqs": {"delay_seconds": 900}},
        "headers": [],
    }


def test_message_binding_sqs_message_group_id(
    message_benchmark: MessageBenchmark,
) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        group_id: Annotated[UUID, SqsMessageGroupId()]

    response = Response(group_id=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05"))

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {
            "sqs": {"message_group_id": "ec5e9f87-c896-4fb1-b028-8352ef654e05"}
        },
        "headers": [],
    }


def test_message_binding_sqs_message_deduplication_id(
    message_benchmark: MessageBenchmark,
) -> None:
    @dataclass
    class Response(Message, address="response_channel"):
        message_deduplication_id: Annotated[UUID, SqsMessageDeduplicationId()]

    response = Response(
        message_deduplication_id=UUID("ec5e9f87-c896-4fb1-b028-8352ef654e05")
    )

    assert message_benchmark(response) == {
        "address": "response_channel",
        "bindings": {
            "sqs": {"message_deduplication_id": "ec5e9f87-c896-4fb1-b028-8352ef654e05"}
        },
        "headers": [],
    }
