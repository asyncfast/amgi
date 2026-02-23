import asyncio
import logging
from queue import Queue
from unittest.mock import AsyncMock
from unittest.mock import Mock
from uuid import uuid4

import pytest
from amgi_kafka_event_source_mapping import _KafkaEventSourceMapping
from amgi_kafka_event_source_mapping import KafkaEventSourceMappingHandler
from amgi_kafka_event_source_mapping import NackError
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import Scope
from test_utils import MockApp


async def test_kafka_event_source_mapping_handler_records() -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, lifespan=False, message_send=AsyncMock()
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": "a2V5",
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [
                                {
                                    "headerKey": [
                                        104,
                                        101,
                                        97,
                                        100,
                                        101,
                                        114,
                                        86,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ]
                                }
                            ],
                        },
                    ]
                },
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": "mytopic",
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [(b"headerKey", b"headerValue")],
            "payload": b"Hello, this is a test.",
            "state": {},
        }

        await send(
            {
                "type": "message.ack",
            }
        )

    await call_task


async def test_kafka_event_source_mapping_handler_error_nack() -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, on_nack="error", lifespan=False, message_send=AsyncMock()
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": "a2V5",
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [
                                {
                                    "headerKey": [
                                        104,
                                        101,
                                        97,
                                        100,
                                        101,
                                        114,
                                        86,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ]
                                }
                            ],
                        },
                    ]
                },
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": "mytopic",
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [(b"headerKey", b"headerValue")],
            "payload": b"Hello, this is a test.",
            "state": {},
        }

        await send(
            {
                "type": "message.nack",
                "message": "Failed to process record",
            }
        )

    with pytest.raises(NackError, match="Failed to process record"):
        await call_task


async def test_kafka_event_source_mapping_handler_log_nack(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, on_nack="log", lifespan=False, message_send=AsyncMock()
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": "a2V5",
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [
                                {
                                    "headerKey": [
                                        104,
                                        101,
                                        97,
                                        100,
                                        101,
                                        114,
                                        86,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ]
                                }
                            ],
                        },
                    ]
                },
            },
            Mock(),
        )
    )
    with caplog.at_level(logging.ERROR, logger="amgi-kafka-event-source-mapping.error"):
        async with app.call() as (scope, receive, send):
            assert scope == {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "mytopic",
                "bindings": {"kafka": {"key": b"key"}},
                "headers": [(b"headerKey", b"headerValue")],
                "payload": b"Hello, this is a test.",
                "state": {},
            }

            await send(
                {
                    "type": "message.nack",
                    "message": "Failed to process record",
                }
            )

        await call_task
        assert any(
            "Failed to process record" in record.message for record in caplog.records
        )


async def test_lifespan() -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, message_send=AsyncMock()
    )

    loop = asyncio.get_event_loop()
    state_item = uuid4()

    lifespan_task = loop.create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {},
            },
            Mock(),
        )
    )
    async with app.lifespan({"item": state_item}):
        await lifespan_task

        call_task = asyncio.get_running_loop().create_task(
            kafka_event_source_mapping_handler._call(
                {
                    "eventSource": "aws:kafka",
                    "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                    "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                    "records": {
                        "mytopic-0": [
                            {
                                "topic": "mytopic",
                                "partition": 0,
                                "offset": 15,
                                "timestamp": 1545084650987,
                                "timestampType": "CREATE_TIME",
                                "key": "a2V5",
                                "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                                "headers": [
                                    {
                                        "headerKey": [
                                            104,
                                            101,
                                            97,
                                            100,
                                            101,
                                            114,
                                            86,
                                            97,
                                            108,
                                            117,
                                            101,
                                        ]
                                    }
                                ],
                            },
                        ]
                    },
                },
                Mock(),
            )
        )
        async with app.call() as (scope, receive, send):
            assert scope == {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "mytopic",
                "bindings": {"kafka": {"key": b"key"}},
                "headers": [(b"headerKey", b"headerValue")],
                "payload": b"Hello, this is a test.",
                "state": {"item": state_item},
            }

        await call_task
        shutdown_task = loop.create_task(kafka_event_source_mapping_handler._shutdown())

    await shutdown_task


def test_lifespan_and_shutdown() -> None:
    queue = Queue[Exception | None]()

    async def _app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        try:
            assert scope["type"] == "lifespan"
            lifespan_startup = await receive()
            assert lifespan_startup == {"type": "lifespan.startup"}
            await send(
                {
                    "type": "lifespan.startup.complete",
                }
            )
            lifespan_shutdown = await receive()
            assert lifespan_shutdown == {"type": "lifespan.shutdown"}
            await send(
                {
                    "type": "lifespan.shutdown.complete",
                }
            )
            queue.put(None)
        except Exception as e:  # pragma: no cover
            queue.put(e)
            raise

    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(_app)

    kafka_event_source_mapping_handler(
        {
            "eventSource": "aws:kafka",
            "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
            "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
            "records": {},
        },
        Mock(),
    )

    kafka_event_source_mapping_handler._sigterm_handler()

    exception = queue.get()
    assert exception is None


async def test_kafka_event_source_mapping_handler_message_send() -> None:
    app = MockApp()
    mock_message_send = AsyncMock()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, lifespan=False, message_send=mock_message_send
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": "a2V5",
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [
                                {
                                    "headerKey": [
                                        104,
                                        101,
                                        97,
                                        100,
                                        101,
                                        114,
                                        86,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ]
                                }
                            ],
                        },
                    ]
                },
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": "mytopic",
            "bindings": {"kafka": {"key": b"key"}},
            "headers": [(b"headerKey", b"headerValue")],
            "payload": b"Hello, this is a test.",
            "state": {},
        }

        await send(
            {
                "type": "message.send",
                "address": "test",
                "headers": [(b"test", b"test")],
                "payload": b"test",
            }
        )

    await call_task

    mock_message_send.__aenter__.return_value.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "test",
            "headers": [(b"test", b"test")],
            "payload": b"test",
        }
    )


async def test_kafka_event_source_mapping_receive_not_callable() -> None:
    app = MockApp()
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app, lifespan=False, message_send=AsyncMock()
    )

    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            {
                "eventSource": "aws:kafka",
                "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
                "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
                "records": {
                    "mytopic-0": [
                        {
                            "topic": "mytopic",
                            "partition": 0,
                            "offset": 15,
                            "timestamp": 1545084650987,
                            "timestampType": "CREATE_TIME",
                            "key": "a2V5",
                            "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                            "headers": [
                                {
                                    "headerKey": [
                                        104,
                                        101,
                                        97,
                                        100,
                                        101,
                                        114,
                                        86,
                                        97,
                                        108,
                                        117,
                                        101,
                                    ]
                                }
                            ],
                        },
                    ]
                },
            },
            Mock(),
        )
    )
    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()

    await call_task


async def test_kafka_event_source_mapping_handler_invocation_hook() -> None:
    app = MockApp()
    mock_invocation_hook = Mock(return_value=AsyncMock())
    kafka_event_source_mapping_handler = KafkaEventSourceMappingHandler(
        app,
        lifespan=False,
        message_send=AsyncMock(),
        invocation_hook=mock_invocation_hook,
    )

    mock_context = Mock()
    event: _KafkaEventSourceMapping = {
        "eventSource": "aws:kafka",
        "eventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
        "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
        "records": {
            "mytopic-0": [
                {
                    "topic": "mytopic",
                    "partition": 0,
                    "offset": 15,
                    "timestamp": 1545084650987,
                    "timestampType": "CREATE_TIME",
                    "key": "a2V5",
                    "value": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                    "headers": [
                        {
                            "headerKey": [
                                104,
                                101,
                                97,
                                100,
                                101,
                                114,
                                86,
                                97,
                                108,
                                117,
                                101,
                            ]
                        }
                    ],
                },
            ]
        },
    }
    call_task = asyncio.get_running_loop().create_task(
        kafka_event_source_mapping_handler._call(
            event,
            mock_context,
        )
    )
    async with app.call():
        mock_invocation_hook.assert_called_once_with(event, mock_context)
        mock_invocation_hook.return_value.__aenter__.assert_awaited_once()
        mock_invocation_hook.return_value.__aexit__.assert_not_awaited()

    await call_task

    mock_invocation_hook.return_value.__aexit__.assert_awaited_once()
