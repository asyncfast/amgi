import inspect
from collections.abc import AsyncGenerator
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from inspect import Signature
from types import UnionType
from typing import Any
from typing import get_args
from typing import get_origin
from typing import Union

from asyncfast._channel import BindingResolver
from asyncfast._channel import CallableResolver
from asyncfast._channel import Channel
from asyncfast._channel import HeaderResolver
from asyncfast._channel import MessageSenderResolver
from asyncfast._channel import PayloadResolver
from asyncfast._channel import Resolver
from asyncfast._channel import Router
from asyncfast._message import Message
from pydantic import BaseModel
from pydantic import create_model
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo
from pydantic.json_schema import GenerateJsonSchema
from pydantic.json_schema import JsonSchemaMode
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema


def generate_resolvers(
    callable_resolver: CallableResolver,
) -> Generator[Resolver[Any], None, None]:
    for _, resolver in callable_resolver.resolvers:
        yield resolver
    for _, dependency in callable_resolver.dependencies:
        yield from generate_resolvers(dependency)


@dataclass(frozen=True)
class MessageDefinition:
    name: str
    address: str | None
    parameters: set[str]
    headers: Sequence[tuple[str, type[Any], Any]]
    bindings: Sequence[tuple[str, str, type[Any], CoreSchema]]
    payload: tuple[type[Any], CoreSchema] | None

    @property
    def channel_definition(self) -> dict[str, Any]:
        definition = {
            "address": self.address,
            "messages": {self.name: {"$ref": f"#/components/messages/{self.name}"}},
        }
        if self.parameters:
            definition["parameters"] = {name: {} for name in self.parameters}
        return definition

    @cached_property
    def headers_model(
        self,
    ) -> type[BaseModel] | None:

        if not self.headers:
            return None
        field_definitions: dict[str, Any] = {
            name: (
                type_,
                FieldInfo(
                    default=default,
                ),
            )
            for name, type_, default in self.headers
        }
        return create_model(
            f"{self.name}Headers", __base__=BaseModel, **field_definitions
        )

    def message(
        self,
        field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
        json_schema_mode: JsonSchemaMode,
    ) -> dict[str, Any]:
        message = {}

        headers_model = self.headers_model
        if headers_model:
            message["headers"] = field_mapping[hash(headers_model), json_schema_mode]

        payload = self.payload
        if payload:
            type_, _ = payload
            message["payload"] = field_mapping[hash(type_), json_schema_mode]

        if self.bindings:
            bindings: dict[str, dict[str, Any]] = {}
            for (
                protocol,
                field_name,
                type_,
                _,
            ) in self.bindings:
                bindings.setdefault(protocol, {})[field_name] = field_mapping[
                    hash(type_), json_schema_mode
                ]
            message["bindings"] = bindings
        return message

    def generate_inputs(
        self, json_schema_mode: JsonSchemaMode
    ) -> Generator[tuple[int, JsonSchemaMode, CoreSchema], None, None]:
        for _, _, type_, core_schema in self.bindings:
            yield hash(type_), json_schema_mode, core_schema

        headers_model = self.headers_model
        if headers_model:
            yield hash(headers_model), json_schema_mode, TypeAdapter(
                headers_model
            ).core_schema
        payload = self.payload
        if payload:
            type_, core_schema = payload
            yield hash(type_), json_schema_mode, core_schema


@dataclass(frozen=True)
class ChannelDefinition:
    channel: Channel

    @cached_property
    def bindings(self) -> Sequence[BindingResolver[Any]]:
        return [
            resolver
            for _, resolver in self.channel.resolvers
            if isinstance(resolver, BindingResolver)
        ]

    @cached_property
    def parameters(self) -> set[str]:
        return self.channel.parameters

    @cached_property
    def name(self) -> str:
        return self.channel.func.__name__

    @cached_property
    def title(self) -> str:
        return "".join(part.title() for part in self.name.split("_"))

    @cached_property
    def resolvers(self) -> Sequence[Resolver[Any]]:
        return tuple(generate_resolvers(self.channel))

    @cached_property
    def payload(self) -> PayloadResolver[Any] | None:
        payloads = [
            resolver
            for resolver in self.resolvers
            if isinstance(resolver, PayloadResolver)
        ]
        if payloads:
            return payloads[0]
        return None

    def generate_send_messages(self) -> Generator[type[Message], None, None]:
        signature = inspect.signature(self.channel.func)

        return_annotation = signature.return_annotation
        if return_annotation is not Signature.empty and (
            get_origin(return_annotation) is AsyncGenerator
            or get_origin(return_annotation) is Generator
        ):
            generator_type = get_args(return_annotation)[0]
            if is_union(generator_type):
                for type_ in get_args(generator_type):
                    if is_message(type_):
                        yield type_
            elif is_message(generator_type):
                yield generator_type

        for resolver in self.resolvers:
            if isinstance(resolver, MessageSenderResolver):
                message_sender_type = get_args(resolver.type)[0]
                if is_union(message_sender_type):
                    for type_ in get_args(message_sender_type):
                        if is_message(type_):
                            yield type_
                elif is_message(message_sender_type):
                    yield message_sender_type

    @cached_property
    def send_messages(self) -> Sequence[type[Message]]:
        return tuple(self.generate_send_messages())

    @cached_property
    def send_message_definitions(self) -> Sequence[MessageDefinition]:
        return tuple(
            MessageDefinition(
                message.__name__,
                message.__address__,
                {name for name, _ in message.__parameters__},
                [(alias, type_, ...) for _, alias, type_, _ in message.__headers__],
                [
                    (protocol, field_name, type_, type_adapter.core_schema)
                    for _, protocol, field_name, type_, type_adapter in message.__bindings__
                ],
                (
                    (
                        message.__payload__[1],
                        message.__payload__[2].core_schema,
                    )
                    if message.__payload__
                    else None
                ),
            )
            for message in self.send_messages
        )

    @cached_property
    def message_definition(self) -> MessageDefinition:
        return MessageDefinition(
            f"{self.title}Message",
            self.channel.address,
            self.parameters,
            [
                (
                    resolver.name,
                    resolver.type,
                    ... if resolver.required else resolver.default,
                )
                for resolver in self.resolvers
                if isinstance(resolver, HeaderResolver)
            ],
            [
                (
                    binding_resolver.protocol,
                    binding_resolver.field_name,
                    binding_resolver.type,
                    binding_resolver.type_adapter.core_schema,
                )
                for binding_resolver in self.bindings
            ],
            (
                (self.payload.type, self.payload.type_adapter.core_schema)
                if self.payload
                else None
            ),
        )


def is_union(type_annotation: type) -> bool:
    origin = get_origin(type_annotation)
    return origin is Union or origin is UnionType


def is_message(cls: type[Any]) -> bool:
    try:
        return issubclass(cls, Message)
    except TypeError:  # pragma: no cover
        return False


def generate_inputs(
    channel_definitions: Iterable[ChannelDefinition],
) -> Generator[tuple[int, JsonSchemaMode, CoreSchema], None, None]:
    for channel_definition in channel_definitions:
        yield from channel_definition.message_definition.generate_inputs("validation")

        for send_message_definition in channel_definition.send_message_definitions:
            yield from send_message_definition.generate_inputs("serialization")


def generate_messages(
    channel_definitions: Iterable[ChannelDefinition],
    field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel_definition in channel_definitions:

        yield channel_definition.message_definition.name, channel_definition.message_definition.message(
            field_mapping, "validation"
        )

        for send_message_definition in channel_definition.send_message_definitions:
            yield send_message_definition.name, send_message_definition.message(
                field_mapping, "serialization"
            )


def generate_channels(
    channel_definitions: Iterable[ChannelDefinition],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel_definition in channel_definitions:
        yield channel_definition.title, channel_definition.message_definition.channel_definition

        for send_message_definition in channel_definition.send_message_definitions:
            yield send_message_definition.name, send_message_definition.channel_definition


def generate_operations(
    channel_definitions: Iterable[ChannelDefinition],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel_definition in channel_definitions:
        yield f"receive{channel_definition.title}", {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{channel_definition.title}"},
        }

        for message in channel_definition.send_messages:
            yield f"send{message.__name__}", {
                "action": "send",
                "channel": {"$ref": f"#/channels/{message.__name__}"},
            }


def get_asyncapi(
    *,
    title: str,
    version: str,
    router: Router,
) -> dict[str, Any]:
    channel_definitions = tuple(
        ChannelDefinition(channel) for channel in router.channels
    )
    schema_generator = GenerateJsonSchema(ref_template="#/components/schemas/{model}")

    field_mapping, definitions = schema_generator.generate_definitions(
        inputs=list(generate_inputs(channel_definitions))
    )

    return {
        "asyncapi": "3.0.0",
        "info": {
            "title": title,
            "version": version,
        },
        "channels": dict(generate_channels(channel_definitions)),
        "operations": dict(generate_operations(channel_definitions)),
        "components": {
            "messages": dict(generate_messages(channel_definitions, field_mapping)),
            **({"schemas": definitions} if definitions else {}),
        },
    }
