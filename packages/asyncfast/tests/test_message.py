from typing import Annotated
from typing import Iterable
from typing import Tuple
from unittest.mock import _Call
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock
from uuid import UUID

import pytest
from asyncfast import AsyncFast
from asyncfast import Header
from pydantic import BaseModel


async def test_message_payload() -> None:
    app = AsyncFast()

    class Payload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: Payload) -> None:
        test_mock(payload)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": [],
            "payload": b'{"id":1}',
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(Payload(id=1))


async def test_message_header_string() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(etag: Annotated[str, Header()]) -> None:
        test_mock(etag)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": [(b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
            "payload": None,
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with("33a64df551425fcc55e4d42a148795d9f25f89d4")


async def test_message_header_integer() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(id: Annotated[int, Header()]) -> None:
        test_mock(id)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": [(b"Id", b"10")],
            "payload": None,
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(10)


async def test_message_header_underscore_to_hyphen() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(
        idempotency_key: Annotated[UUID, Header()],
    ) -> None:
        test_mock(idempotency_key)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": [(b"Idempotency-Key", b"8e03978e-40d5-43e8-bc93-6894a57f9324")],
            "payload": None,
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(UUID("8e03978e-40d5-43e8-bc93-6894a57f9324"))


async def test_message_headers_multiple() -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(
        id: Annotated[int, Header()],
        etag: Annotated[str, Header()],
    ) -> None:
        test_mock(id, etag)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": [
                (b"Id", b"10"),
                (b"ETag", b"33a64df551425fcc55e4d42a148795d9f25f89d4"),
            ],
            "payload": None,
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(10, "33a64df551425fcc55e4d42a148795d9f25f89d4")


@pytest.mark.parametrize(
    ["headers", "expected_call"],
    [
        (
            [(b"Id", b"33a64df551425fcc55e4d42a148795d9f25f89d4")],
            call("33a64df551425fcc55e4d42a148795d9f25f89d4"),
        ),
        ([], call(None)),
    ],
)
async def test_message_header_optional(
    headers: Iterable[Tuple[bytes, bytes]], expected_call: _Call
) -> None:
    app = AsyncFast()

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(id: Annotated[str | None, Header()] = None) -> None:
        test_mock(id)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "topic",
            "headers": headers,
            "payload": None,
        },
        AsyncMock(),
        AsyncMock(),
    )

    assert test_mock.mock_calls == [expected_call]
