import asyncio
import inspect
import re
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from typing import Annotated
from typing import Any
from typing import Generic
from typing import get_args
from typing import get_origin
from typing import TypeVar

from amgi_types import AMGISendCallable
from amgi_types import MessageReceiveEvent
from amgi_types import MessageSendEvent
from asyncfast.bindings import Binding
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo

_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
_PARAMETER_PATTERN = re.compile(r"{(.*)}")


T = TypeVar("T")
M = TypeVar("M", bound=Mapping[str, Any])


class InvalidChannelDefinitionError(ValueError):
    """
    Raised when a channel or message handler is defined with an invalid shape.
    """


class Header(FieldInfo):
    pass


class Payload(FieldInfo):
    pass


class Parameter(FieldInfo):
    pass


@dataclass(frozen=True)
class MessageReceive:
    message: MessageReceiveEvent
    address_parameters: dict[str, str]

    @cached_property
    def headers(self) -> dict[str, bytes]:
        return {key.decode(): value for key, value in reversed(self.message["headers"])}


class Resolver(Generic[T], ABC):
    @abstractmethod
    def resolve(self, message_receive: MessageReceive, send: AMGISendCallable) -> T: ...


@dataclass(frozen=True)
class TypeResolve(Resolver[T], ABC):
    type: type[T]

    @cached_property
    def type_adapter(self) -> TypeAdapter[T]:
        return TypeAdapter(self.type)


@dataclass(frozen=True)
class PayloadResolver(TypeResolve[T]):
    def resolve(self, message_receive: MessageReceive, send: AMGISendCallable) -> T:
        payload: bytes | None = message_receive.message.get("payload")
        if payload is None:
            return self.type_adapter.validate_python(None)
        return self.type_adapter.validate_json(payload)


@dataclass(frozen=True)
class BindingResolver(TypeResolve[T]):
    protocol: str
    field_name: str
    default: T

    def resolve(self, message_receive: MessageReceive, send: AMGISendCallable) -> T:
        bindings = message_receive.message.get("bindings", {})

        return self.type_adapter.validate_python(
            bindings.get(self.protocol, {}).get(self.field_name, self.default)
        )


@dataclass(frozen=True)
class AddressParameterResolver(Resolver[str]):
    name: str

    def resolve(self, message_receive: MessageReceive, send: AMGISendCallable) -> str:
        return message_receive.address_parameters[self.name]


@dataclass(frozen=True)
class HeaderResolver(TypeResolve[T]):
    name: str
    default: T
    required: bool

    def resolve(self, message_receive: MessageReceive, send: AMGISendCallable) -> T:
        if not self.required:
            value = message_receive.headers.get(self.name, self.default)
        else:
            value = message_receive.headers[self.name]
        return self.type_adapter.validate_python(value)


async def send_message(send: AMGISendCallable, message: Mapping[str, Any]) -> None:
    message_send_event: MessageSendEvent = {
        "type": "message.send",
        "address": message["address"],
        "headers": message["headers"],
        "payload": message.get("payload"),
    }
    await send(message_send_event)


class MessageSender(Generic[M]):
    def __init__(self, send: AMGISendCallable) -> None:
        self._send = send

    async def send(self, message: M) -> None:
        await send_message(self._send, message)


@dataclass(frozen=True)
class MessageSenderResolver(Resolver[MessageSender[M]]):
    type: type[MessageSender[M]]

    def resolve(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> MessageSender[M]:
        return MessageSender(send)


@dataclass(frozen=True)
class Channel(ABC):
    func: Callable[..., Any]
    parameters: set[str]
    resolvers: dict[str, Resolver[Any]]

    def resolve(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> dict[str, Any]:
        return {
            name: resolver.resolve(message_receive, send)
            for name, resolver in self.resolvers.items()
        }

    @abstractmethod
    async def call(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> None: ...


@dataclass(frozen=True)
class SyncChannel(Channel):
    async def call(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> None:
        await asyncio.to_thread(self.func, **self.resolve(message_receive, send))


@dataclass(frozen=True)
class AsyncChannel(Channel):
    async def call(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> None:
        await self.func(**self.resolve(message_receive, send))


class AsyncGeneratorChannel(Channel):
    async def call(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> None:
        agen = self.func(**self.resolve(message_receive, send))
        exception: Exception | None = None
        while True:
            try:
                if exception is None:
                    message = await agen.__anext__()
                else:
                    message = await agen.athrow(exception)
                try:
                    await send_message(send, message)
                except Exception as e:
                    exception = e
                else:
                    exception = None
            except StopAsyncIteration:
                break


def _throw_or_none(gen: Generator[Any, None, None], exception: Exception) -> Any:
    try:
        return gen.throw(exception)
    except StopIteration:
        return None


class SyncGeneratorChannel(Channel):
    async def call(
        self, message_receive: MessageReceive, send: AMGISendCallable
    ) -> None:

        gen = self.func(**self.resolve(message_receive, send))
        exception: Exception | None = None
        while True:
            if exception is None:
                message = await asyncio.to_thread(next, gen, None)
            else:
                message = await asyncio.to_thread(_throw_or_none, gen, exception)
            if message is None:
                break
            try:
                await send_message(send, message)
            except Exception as e:
                exception = e
            else:
                exception = None


def parameter_resolver(
    name: str, parameter: inspect.Parameter, address_parameters: set[str]
) -> Resolver[Any]:
    if name in address_parameters:
        return AddressParameterResolver(name)
    if get_origin(parameter.annotation) is Annotated:
        _, annotation, *_ = get_args(parameter.annotation)
        if isinstance(annotation, Header):
            header_name = (
                annotation.alias if annotation.alias else name.replace("_", "-")
            )

            return HeaderResolver(
                parameter.annotation,
                header_name,
                parameter.default,
                parameter.default is parameter.empty,
            )
        if isinstance(annotation, Binding):
            return BindingResolver(
                parameter.annotation,
                annotation.__protocol__,
                annotation.__field_name__,
                parameter.default,
            )
    if get_origin(parameter.annotation) is MessageSender:
        return MessageSenderResolver(parameter.annotation)

    return PayloadResolver(parameter.annotation)


def channel(func: Callable[..., Any], address_parameters: set[str]) -> Channel:
    signature = inspect.signature(func)
    resolvers = {
        name: parameter_resolver(name, parameter, address_parameters)
        for name, parameter in signature.parameters.items()
    }

    payloads = sum(
        isinstance(resolver, PayloadResolver) for resolver in resolvers.values()
    )
    if payloads > 1:
        raise InvalidChannelDefinitionError("Channel must have no more than 1 payload")

    message_senders = sum(
        isinstance(resolver, MessageSenderResolver) for resolver in resolvers.values()
    )
    if message_senders > 1:
        raise InvalidChannelDefinitionError(
            "Channel must have no more than 1 message sender"
        )

    if inspect.iscoroutinefunction(func):
        return AsyncChannel(func, address_parameters, resolvers)
    if inspect.isasyncgenfunction(func):
        return AsyncGeneratorChannel(func, address_parameters, resolvers)
    if inspect.isgeneratorfunction(func):
        return SyncGeneratorChannel(func, address_parameters, resolvers)
    return SyncChannel(func, address_parameters, resolvers)
