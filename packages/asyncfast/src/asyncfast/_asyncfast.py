from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from functools import partial
from re import Pattern
from typing import Any
from typing import TypeVar

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from amgi_types import MessageScope
from amgi_types import Scope
from asyncfast._asyncapi import ChannelDefinition
from asyncfast._asyncapi import get_asyncapi
from asyncfast._channel import Channel
from asyncfast._channel import channel as make_channel
from asyncfast._channel import MessageReceive
from asyncfast._utils import get_address_pattern

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
Lifespan = Callable[["AsyncFast"], AbstractAsyncContextManager[None]]


class ChannelNotFoundError(LookupError):
    def __init__(self, address: str) -> None:
        super().__init__(f"Couldn't resolve address: {address}")
        self.address = address


class AsyncFast:
    def __init__(
        self,
        title: str = "AsyncFast",
        version: str = "0.1.0",
        lifespan: Lifespan | None = None,
    ) -> None:
        self._channels: list[_Channel] = []
        self._title = title
        self._version = version
        self._lifespan_context = lifespan
        self._lifespan: AbstractAsyncContextManager[None] | None = None
        self._asyncapi_schema: dict[str, Any] | None = None

    @property
    def title(self) -> str:
        return self._title

    @property
    def version(self) -> str:
        return self._version

    def channel(self, address: str) -> Callable[[DecoratedCallable], DecoratedCallable]:
        return partial(self._add_channel, address)

    def _add_channel(
        self, address: str, function: DecoratedCallable
    ) -> DecoratedCallable:
        address_pattern = get_address_pattern(address)

        channel = _Channel(
            address,
            address_pattern,
            make_channel(function, address),
        )

        self._channels.append(channel)
        return function

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    if self._lifespan_context is not None:
                        self._lifespan = self._lifespan_context(self)
                        await self._lifespan.__aenter__()
                    lifespan_startup_complete_event: LifespanStartupCompleteEvent = {
                        "type": "lifespan.startup.complete"
                    }
                    await send(lifespan_startup_complete_event)
                elif message["type"] == "lifespan.shutdown":
                    if self._lifespan is not None:
                        await self._lifespan.__aexit__(None, None, None)
                    lifespan_shutdown_complete_event: LifespanShutdownCompleteEvent = {
                        "type": "lifespan.shutdown.complete"
                    }
                    await send(lifespan_shutdown_complete_event)
                    return
        elif scope["type"] == "message":
            address = scope["address"]
            for channel in self._channels:
                parameters = channel.match(address)
                if parameters is not None:
                    await channel(scope, send, parameters)
                    return
            raise ChannelNotFoundError(address)

    def asyncapi(self) -> dict[str, Any]:
        if not self._asyncapi_schema:
            channel_definitions = tuple(
                ChannelDefinition(channel._channel_invoker)
                for channel in self._channels
            )
            self._asyncapi_schema = get_asyncapi(
                title=self.title,
                version=self.version,
                channel_definitions=channel_definitions,
            )

        return self._asyncapi_schema


class _Channel:
    def __init__(
        self,
        address: str,
        address_pattern: Pattern[str],
        channel_invoker: Channel,
    ) -> None:
        self._address = address
        self._address_pattern = address_pattern
        self._channel_invoker = channel_invoker

    def match(self, address: str) -> dict[str, str] | None:
        match = self._address_pattern.match(address)
        if match:
            return match.groupdict()
        return None

    async def __call__(
        self,
        scope: MessageScope,
        send: AMGISendCallable,
        parameters: dict[str, str],
    ) -> None:
        try:
            await self._channel_invoker.invoke(MessageReceive(scope, parameters), send)

            message_ack_event: MessageAckEvent = {
                "type": "message.ack",
            }
            await send(message_ack_event)
        except Exception as e:
            message_nack_event: MessageNackEvent = {
                "type": "message.nack",
                "message": str(e),
            }
            await send(message_nack_event)
