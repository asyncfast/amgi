from typing import Annotated
from typing import Any
from typing import AsyncGenerator
from typing import Generator
from typing import Mapping
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock

from asyncfast._channel import channel
from asyncfast._channel import Depends
from asyncfast._channel import Header
from asyncfast._channel import MessageReceive
from asyncfast._channel import MessageSender
from asyncfast.bindings import KafkaKey


async def test_payload_basic() -> None:
    mock = Mock()

    def func(i: int) -> None:
        mock(i)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, {"user"}).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel.54a08cc6-02db-11f1-afbf-f3f4688d5de4",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
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


async def test_async_depends() -> None:
    mock = Mock()

    async def dependency(
        header1: Annotated[int, Header()], header2: Annotated[int, Header()]
    ) -> dict[str, int]:
        return {
            "header1": header1,
            "header2": header2,
        }

    def func(headers: Annotated[dict[str, int], Depends(dependency)]) -> None:
        mock(headers)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [(b"header1", b"1"), (b"header2", b"2")],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with({"header1": 1, "header2": 2})


async def test_sync_depends() -> None:
    mock = Mock()

    def dependency(
        header1: Annotated[int, Header()], header2: Annotated[int, Header()]
    ) -> dict[str, int]:
        return {
            "header1": header1,
            "header2": header2,
        }

    def func(headers: Annotated[dict[str, int], Depends(dependency)]) -> None:
        mock(headers)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [(b"header1", b"1"), (b"header2", b"2")],
            },
            {},
        ),
        Mock(),
    )

    mock.assert_called_with({"header1": 1, "header2": 2})


async def test_async_depends_use_cache() -> None:
    mock_func = Mock()
    mock_dependency = Mock()

    async def dependency() -> Any:
        return mock_dependency()

    def func(
        dependency1: Annotated[Any, Depends(dependency)],
        dependency2: Annotated[Any, Depends(dependency)],
    ) -> None:
        mock_func(dependency1, dependency2)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [],
            },
            {},
        ),
        Mock(),
    )

    mock_func.assert_called_with(
        mock_dependency.return_value, mock_dependency.return_value
    )
    mock_dependency.assert_called_once()


async def test_async_depends_use_cache_false() -> None:
    mock_func = Mock()
    mock_dependency = Mock()

    async def dependency() -> Any:
        return mock_dependency()

    def func(
        dependency1: Annotated[Any, Depends(dependency, use_cache=False)],
        dependency2: Annotated[Any, Depends(dependency, use_cache=False)],
    ) -> None:
        mock_func(dependency1, dependency2)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [],
            },
            {},
        ),
        Mock(),
    )

    mock_func.assert_called_with(
        mock_dependency.return_value, mock_dependency.return_value
    )
    assert mock_dependency.call_count == 2


async def test_async_yielding_depends() -> None:
    parent = Mock()

    mock = Mock()
    mock_close = Mock()

    parent.attach_mock(mock, "func")
    parent.attach_mock(mock_close, "close")

    async def dependency(
        header1: Annotated[int, Header()], header2: Annotated[int, Header()]
    ) -> AsyncGenerator[dict[str, int], None]:
        yield {
            "header1": header1,
            "header2": header2,
        }
        mock_close()

    def func(headers: Annotated[dict[str, int], Depends(dependency)]) -> None:
        mock(headers)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [(b"header1", b"1"), (b"header2", b"2")],
            },
            {},
        ),
        Mock(),
    )

    assert parent.mock_calls == [
        call.func({"header1": 1, "header2": 2}),
        call.close(),
    ]


async def test_sync_yielding_depends() -> None:
    parent = Mock()

    mock = Mock()
    mock_close = Mock()

    parent.attach_mock(mock, "func")
    parent.attach_mock(mock_close, "close")

    def dependency(
        header1: Annotated[int, Header()], header2: Annotated[int, Header()]
    ) -> Generator[dict[str, int], None, None]:
        yield {
            "header1": header1,
            "header2": header2,
        }
        mock_close()

    def func(headers: Annotated[dict[str, int], Depends(dependency)]) -> None:
        mock(headers)

    await channel(func, set()).invoke(
        MessageReceive(
            {
                "type": "message",
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "address": "channel",
                "headers": [(b"header1", b"1"), (b"header2", b"2")],
            },
            {},
        ),
        Mock(),
    )

    assert parent.mock_calls == [
        call.func({"header1": 1, "header2": 2}),
        call.close(),
    ]
