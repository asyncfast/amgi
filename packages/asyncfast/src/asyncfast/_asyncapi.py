import inspect
from collections.abc import AsyncGenerator
from collections.abc import Generator
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
from asyncfast._channel import Channel
from asyncfast._channel import HeaderResolver
from asyncfast._channel import MessageSenderResolver
from asyncfast._channel import PayloadResolver
from asyncfast._message import Message
from pydantic import BaseModel
from pydantic import create_model
from pydantic.fields import FieldInfo


@dataclass(frozen=True)
class ChannelDefinition:
    channel: Channel

    @cached_property
    def bindings(self) -> Sequence[BindingResolver[Any]]:
        return [
            resolver
            for resolver in self.channel.resolvers.values()
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
    def headers_model(
        self,
    ) -> type[BaseModel] | None:
        headers = [
            resolver
            for resolver in self.channel.resolvers.values()
            if isinstance(resolver, HeaderResolver)
        ]
        if not headers:
            return None
        field_definitions: dict[str, Any] = {
            resolver.name: (
                resolver.type,
                FieldInfo(
                    default=... if resolver.required else resolver.default,
                ),
            )
            for resolver in headers
        }
        return create_model(
            f"{self.title}Headers", __base__=BaseModel, **field_definitions
        )

    @cached_property
    def payload(self) -> PayloadResolver[Any] | None:
        payloads = [
            resolver
            for resolver in self.channel.resolvers.values()
            if isinstance(resolver, PayloadResolver)
        ]
        if payloads:
            return payloads[0]
        return None

    def generate_messages(self) -> Generator[type[Message], None, None]:
        signature = inspect.signature(self.channel.func)

        return_annotation = signature.return_annotation
        if return_annotation is not Signature.empty and (
            get_origin(return_annotation) is AsyncGenerator
            or get_origin(return_annotation) is Generator
        ):
            generator_type = get_args(return_annotation)[0]
            if _is_union(generator_type):
                for type in get_args(generator_type):
                    if _is_message(type):
                        yield type
            elif _is_message(generator_type):
                yield generator_type

        for resolver in self.channel.resolvers.values():
            if isinstance(resolver, MessageSenderResolver):
                message_sender_type = get_args(resolver.type)[0]
                if _is_union(message_sender_type):
                    for type in get_args(message_sender_type):
                        if _is_message(type):
                            yield type
                elif _is_message(message_sender_type):
                    yield message_sender_type

    @cached_property
    def messages(self) -> Sequence[type[Message]]:
        return tuple(self.generate_messages())


@dataclass(frozen=True)
class MessageDefinition:
    address: str | None
    name: str
    parameters: set[str]

    @property
    def definition(self) -> dict[str, Any]:
        definition = {
            "address": self.address,
            "messages": {self.name: {"$ref": f"#/components/messages/{self.name}"}},
        }
        if self.parameters:
            definition["parameters"] = {name: {} for name in self.parameters}
        return definition


def _is_union(type_annotation: type) -> bool:
    origin = get_origin(type_annotation)
    return origin is Union or origin is UnionType


def _is_message(cls: type[Any]) -> bool:
    try:
        return issubclass(cls, Message)
    except TypeError:  # pragma: no cover
        return False
