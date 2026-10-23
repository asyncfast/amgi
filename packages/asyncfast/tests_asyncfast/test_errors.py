from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated
from typing import Union

import pytest
from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import InvalidChannelDefinitionError
from asyncfast import Message
from asyncfast import Reply
from asyncfast._asyncapi import get_asyncapi
from asyncfast._channel import ChannelRouter


async def test_only_one_payload() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("topic")
        async def topic_handler(payload1: int, payload2: int) -> None:
            pass  # pragma: no cover


def test_reply_header_without_alias() -> None:
    with pytest.raises(InvalidChannelDefinitionError):
        Reply(address=Header())


def test_reply_not_message() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(payload: int) -> Annotated[int, Reply()]:
            return payload  # pragma: no cover


def test_reply_union_of_messages() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    @dataclass
    class PongError(Message, address="pong_error"):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(payload: int) -> Annotated[Union[Pong, PongError], Reply()]:
            return Pong(payload="pong")  # pragma: no cover


def test_reply_message_without_address() -> None:
    @dataclass
    class Pong(Message, address=None):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(payload: int) -> Annotated[Pong, Reply()]:
            return Pong(payload="pong")  # pragma: no cover


def test_reply_header_address_message_with_address() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(
            payload: int,
        ) -> Annotated[Pong, Reply(address=Header(alias="replyTo"))]:
            return Pong(payload="pong")  # pragma: no cover


def test_reply_multiple_replies() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(payload: int) -> Annotated[Pong, Reply(), Reply()]:
            return Pong(payload="pong")  # pragma: no cover


def test_reply_generator() -> None:
    @dataclass
    class Pong(Message, address="pong"):
        payload: str

    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(
            payload: int,
        ) -> Annotated[AsyncGenerator[Pong, None], Reply()]:
            yield Pong(payload="pong")  # pragma: no cover


def test_reply_invalid_channel_not_registered() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("ping")
        async def ping(payload: int) -> Annotated[int, Reply()]:
            return payload  # pragma: no cover

    assert app._router.channels == []
    assert app.asyncapi()["channels"] == {}


def test_reply_invalid_asyncapi() -> None:
    async def ping(payload: int) -> Annotated[int, Reply()]:
        return payload  # pragma: no cover

    router = ChannelRouter()
    router.add_channel("ping", ping)

    with pytest.raises(InvalidChannelDefinitionError):
        get_asyncapi(title="AsyncFast", version="0.1.0", router=router)
