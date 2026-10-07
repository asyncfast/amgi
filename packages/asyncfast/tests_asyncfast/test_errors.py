from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Union

import pytest
from asyncfast import AsyncFast
from asyncfast import InvalidChannelDefinitionError
from asyncfast import Message
from asyncfast import Reply
from asyncfast import ReplyAddress
from asyncfast._asyncapi import get_asyncapi
from asyncfast._channel import ChannelRouter


async def test_only_one_payload() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("topic")
        async def topic_handler(payload1: int, payload2: int) -> None:
            pass  # pragma: no cover


def test_reply_address_invalid_location() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        ReplyAddress("replyTo")


def test_reply_address_location_without_fragment() -> None:
    assert ReplyAddress("$message.header").location == "$message.header"
    assert ReplyAddress("$message.payload").location == "$message.payload"


def test_reply_address_empty_fragment_location() -> None:
    assert ReplyAddress("$message.header#").location == "$message.header#"


def test_reply_address_location_with_escapes() -> None:
    location = "$message.payload#/a~1b~0c"
    assert ReplyAddress(location).location == location


def test_reply_address_trailing_newline_location() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        ReplyAddress("$message.header#/replyTo\n")


def test_reply_address_invalid_escape_location() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        ReplyAddress("$message.header#/replyTo~2")


def test_reply_address_invalid_reference_location() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        ReplyAddress("$message.headers#/replyTo")


def test_reply_invalid_str_address() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        Reply(address="not-a-runtime-expression")


def test_reply_no_send_message() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping", reply=Reply())
        async def ping(payload: int) -> None:
            pass  # pragma: no cover


def test_reply_multiple_send_messages() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    @dataclass
    class PongError(Message, address="pong_error"):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping", reply=Reply())
        async def ping(payload: int) -> AsyncGenerator[Union[Pong, PongError], None]:
            yield Pong(payload="pong")  # pragma: no cover


def test_reply_invalid_channel_not_registered() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping", reply=Reply())
        async def ping(payload: int) -> None:
            pass  # pragma: no cover

    assert app._router.channels == []
    assert app.asyncapi()["channels"] == {}


def test_reply_multiple_send_messages_asyncapi() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    @dataclass
    class PongError(Message, address="pong_error"):
        payload: str

    async def ping(payload: int) -> AsyncGenerator[Union[Pong, PongError], None]:
        yield Pong(payload="pong")  # pragma: no cover

    router = ChannelRouter()
    router.add_channel("ping", ping, Reply())

    with pytest.raises(InvalidChannelDefinitionError):
        get_asyncapi(title="AsyncFast", version="0.1.0", router=router)
