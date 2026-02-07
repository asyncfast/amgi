from typing import Annotated
from typing import Any
from typing import AsyncGenerator
from typing import Generator
from typing import Mapping
from unittest.mock import AsyncMock
from unittest.mock import Mock

from asyncfast._channel import channel
from asyncfast._channel import Header
from asyncfast._channel import MessageReceive
from asyncfast._channel import MessageSender
from asyncfast.bindings import KafkaKey


async def test_payload_basic() -> None:
    mock = Mock()

    def func(i: int) -> None:
        mock(i)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "payload": b"1",
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with(1)


async def test_header_basic() -> None:
    mock = Mock()

    def func(header: Annotated[str, Header()]) -> None:
        mock(header)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [(b"header", b"value")],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with("value")


async def test_header_default() -> None:
    mock = Mock()

    def func(header: Annotated[str, Header()] = "value") -> None:
        mock(header)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with("value")


async def test_header_underscore_to_hyphen() -> None:
    mock = Mock()

    def func(header_name: Annotated[str, Header()]) -> None:
        mock(header_name)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [(b"header-name", b"value")],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with("value")


async def test_header_alias() -> None:
    mock = Mock()

    def func(etag: Annotated[str, Header(alias="ETag")]) -> None:
        mock(etag)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [(b"ETag", b"9e30981e-02d5-11f1-9648-e323315723e1")],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with("9e30981e-02d5-11f1-9648-e323315723e1")


async def test_address_parameter() -> None:
    mock = Mock()

    def func(user: str) -> None:
        mock(user)

    await channel(func, {"user"}).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
            },
            {"user": "54a08cc6-02db-11f1-afbf-f3f4688d5de4"},
        ),
        Mock(),
    )

    mock.assert_called_with("54a08cc6-02db-11f1-afbf-f3f4688d5de4")


async def test_binding() -> None:
    mock = Mock()

    def func(key: Annotated[int, KafkaKey()]) -> None:
        mock(key)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "bindings": {"kafka": {"key": b"123"}},
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with(123)


async def test_binding_default() -> None:
    mock = Mock()

    def func(key: Annotated[int, KafkaKey()] = 123) -> None:
        mock(key)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with(123)


async def test_async_func() -> None:
    mock = AsyncMock()

    async def func(i: int) -> None:
        await mock(i)

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "payload": b"1",
            },
            {},
        ),
        Mock(),
    )

    mock.assert_awaited_once_with(1)


async def test_async_generator_func() -> None:
    send_mock = AsyncMock()

    async def func() -> AsyncGenerator[Mapping[str, Any], None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "payload": b"1",
            },
            {},
        ),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "send_topic",
            "headers": [(b"Id", b"10")],
            "payload": b'{"key": "KEY-001"}',
        }
    )


async def test_sync_generator_func() -> None:
    send_mock = AsyncMock()

    def func() -> Generator[Mapping[str, Any], None, None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "payload": b"1",
            },
            {},
        ),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "send_topic",
            "headers": [(b"Id", b"10")],
            "payload": b'{"key": "KEY-001"}',
        }
    )


async def test_message_sender() -> None:
    send_mock = AsyncMock()

    async def func(message_sender: MessageSender[Mapping[str, Any]]) -> None:
        await message_sender.send(
            {
                "address": "send_topic",
                "payload": b'{"key": "KEY-001"}',
                "headers": [(b"Id", b"10")],
            }
        )

    await channel(func, set()).call(
        MessageReceive(
            {
                "type": "message.receive",
                "id": "id",
                "headers": [],
                "payload": b"1",
            },
            {},
        ),
        send_mock,
    )

    send_mock.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "send_topic",
            "headers": [(b"Id", b"10")],
            "payload": b'{"key": "KEY-001"}',
        }
    )
