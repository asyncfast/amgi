import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from functools import partial
from re import Pattern
from typing import Any
from typing import get_args
from typing import TypeVar

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import Scope
from asyncfast._asyncapi import ChannelDefinition
from asyncfast._asyncapi import MessageDefinition
from asyncfast._channel import Channel
from asyncfast._channel import channel as make_channel
from asyncfast._channel import MessageReceive
from asyncfast._utils import _address_pattern
from asyncfast._utils import _get_address_parameters
from asyncfast.bindings import Binding
from pydantic import TypeAdapter
from pydantic.json_schema import GenerateJsonSchema
from pydantic.json_schema import JsonSchemaMode
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
M = TypeVar("M", bound=Mapping[str, Any])
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
        address_pattern = _address_pattern(address)

        channel = _Channel(
            address,
            address_pattern,
            make_channel(function, _get_address_parameters(address)),
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
                    await channel(scope, receive, send, parameters)
                    return
            raise ChannelNotFoundError(address)

    def asyncapi(self) -> dict[str, Any]:
        schema_generator = GenerateJsonSchema(
            ref_template="#/components/schemas/{model}"
        )

        field_mapping, definitions = schema_generator.generate_definitions(
            inputs=list(self._generate_inputs())
        )
        return {
            "asyncapi": "3.0.0",
            "info": {
                "title": self.title,
                "version": self.version,
            },
            "channels": dict(_generate_channels(self._channels)),
            "operations": dict(_generate_operations(self._channels)),
            "components": {
                "messages": dict(_generate_messages(self._channels, field_mapping)),
                **({"schemas": definitions} if definitions else {}),
            },
        }

    def _generate_inputs(
        self,
    ) -> Generator[tuple[int, JsonSchemaMode, CoreSchema], None, None]:
        for channel in self._channels:
            for binding_resolver in channel._channel_definition.bindings:
                yield hash(
                    binding_resolver.type
                ), "validation", binding_resolver.type_adapter.core_schema

            headers_model = channel._channel_definition.headers_model
            if headers_model:
                yield hash(headers_model), "validation", TypeAdapter(
                    headers_model
                ).core_schema
            payload = channel._channel_definition.payload
            if payload:
                yield hash(payload.type), "validation", payload.type_adapter.core_schema

            for message in channel._channel_definition.messages:
                if message.__payload__:
                    _, field = message.__payload__

                    yield hash(field), "serialization", field.type_adapter.core_schema

                for _, _, field in message.__bindings__.values():
                    yield hash(
                        field.type
                    ), "serialization", field.type_adapter.core_schema

                message_headers_model = message._headers_model()
                if message_headers_model:
                    yield hash(message_headers_model), "serialization", TypeAdapter(
                        message_headers_model
                    ).core_schema


async def _receive_messages(
    receive: AMGIReceiveCallable,
) -> AsyncGenerator[MessageReceiveEvent, None]:
    more_messages = True
    while more_messages:
        message = await receive()
        assert message["type"] == "message.receive"
        yield message
        more_messages = message.get("more_messages", False)


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
        self._channel_definition = ChannelDefinition(channel_invoker)

    @property
    def address(self) -> str:
        return self._address

    def match(self, address: str) -> dict[str, str] | None:
        match = self._address_pattern.match(address)
        if match:
            return match.groupdict()
        return None

    async def __call__(
        self,
        scope: MessageScope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
        parameters: dict[str, str],
    ) -> None:
        ack_out_of_order = "message.ack.out_of_order" in scope.get("extensions", {})
        if ack_out_of_order:
            await asyncio.gather(
                *[
                    self._handle_message(message, parameters, send)
                    async for message in _receive_messages(receive)
                ]
            )
        else:
            async for message in _receive_messages(receive):
                await self._handle_message(message, parameters, send)

    async def _handle_message(
        self,
        message: MessageReceiveEvent,
        parameters: dict[str, str],
        send: AMGISendCallable,
    ) -> None:
        try:

            await self._channel_invoker.invoke(
                MessageReceive(message, parameters), send
            )

            message_ack_event: MessageAckEvent = {
                "type": "message.ack",
                "id": message["id"],
            }
            await send(message_ack_event)
        except Exception as e:
            message_nack_event: MessageNackEvent = {
                "type": "message.nack",
                "id": message["id"],
                "message": str(e),
            }
            await send(message_nack_event)


def _generate_messages(
    channels: Iterable[_Channel],
    field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel in channels:
        message = {}

        headers_model = channel._channel_definition.headers_model
        if headers_model:
            message["headers"] = field_mapping[hash(headers_model), "validation"]

        payload = channel._channel_definition.payload
        if payload:
            message["payload"] = field_mapping[hash(payload.type), "validation"]

        bindings: dict[str, dict[str, Any]]
        if channel._channel_definition.bindings:
            bindings = {}
            for binding_resolver in channel._channel_definition.bindings:

                bindings.setdefault(binding_resolver.protocol, {})[
                    binding_resolver.field_name
                ] = field_mapping[hash(binding_resolver.type), "validation"]
            message["bindings"] = bindings

        yield f"{channel._channel_definition.title}Message", message

        for channel_message in channel._channel_definition.messages:
            message_message = {}

            if channel_message.__payload__:
                _, field = channel_message.__payload__
                message_message["payload"] = field_mapping[hash(field), "serialization"]

            message_headers_model = channel_message._headers_model()
            if message_headers_model:
                message_message["headers"] = field_mapping[
                    hash(message_headers_model), "serialization"
                ]

            if channel_message.__bindings__:
                bindings = {}
                for _, _, field in channel_message.__bindings__.values():
                    binding_type = get_args(field.type)[1]
                    assert isinstance(binding_type, Binding)

                    bindings.setdefault(binding_type.__protocol__, {})[
                        binding_type.__field_name__
                    ] = field_mapping[hash(field), "serialization"]
                message_message["bindings"] = bindings

            yield channel_message.__name__, message_message


def _generate_channels(
    channels: Iterable[_Channel],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel in channels:
        yield channel._channel_definition.title, MessageDefinition(
            channel.address,
            f"{channel._channel_definition.title}Message",
            channel._channel_definition.parameters,
        ).definition

        for message in channel._channel_definition.messages:
            yield message.__name__, MessageDefinition(
                message.__address__,
                message.__name__,
                {name for name in message.__parameters__},
            ).definition


def _generate_operations(
    channels: Iterable[_Channel],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel in channels:
        yield f"receive{channel._channel_definition.title}", {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{channel._channel_definition.title}"},
        }

        for message in channel._channel_definition.messages:
            yield f"send{message.__name__}", {
                "action": "send",
                "channel": {"$ref": f"#/channels/{message.__name__}"},
            }
