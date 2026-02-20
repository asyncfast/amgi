from __future__ import annotations

import asyncio
import inspect
import re
from abc import ABC
from abc import abstractmethod
from asyncio import AbstractEventLoop
from asyncio import Task
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from contextlib import AsyncExitStack
from dataclasses import dataclass
from dataclasses import KW_ONLY
from functools import cached_property
from functools import wraps
from re import Pattern
from typing import Annotated
from typing import Any
from typing import Generic
from typing import get_args
from typing import get_origin
from typing import ParamSpec
from typing import TypeVar

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from asyncfast._utils import get_address_parameters
from asyncfast._utils import get_address_pattern
from asyncfast.bindings import Binding
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo

_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
_PARAMETER_PATTERN = re.compile(r"{(.*)}")

P = ParamSpec("P")
T = TypeVar("T")
M = TypeVar("M", bound=Mapping[str, Any])
DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


def _next_or_stop(generator: Generator[T, None, Any]) -> T | StopIteration:
    try:
        return next(generator)
    except StopIteration as e:
        return e


def _throw_or_stop(
    generator: Generator[T, None, Any], exc: BaseException
) -> T | StopIteration:
    try:
        return generator.throw(exc)
    except StopIteration as e:
        return e


def asyncify_generator(
    func: Callable[P, Generator[T, None, Any]],
) -> Callable[P, AsyncGenerator[T]]:
    @wraps(func)
    async def wrapped_generator(*args: P.args, **kwargs: P.kwargs) -> AsyncGenerator[T]:
        generator = func(*args, **kwargs)
        try:
            result: T | StopIteration = await asyncio.to_thread(
                _next_or_stop, generator
            )

            while True:
                if isinstance(result, StopIteration):
                    return

                try:
                    yield result
                except BaseException as exc:
                    if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                        raise
                    result = await asyncio.to_thread(_throw_or_stop, generator, exc)
                else:
                    result = await asyncio.to_thread(_next_or_stop, generator)
        finally:
            generator.close()

    return wrapped_generator


class InvalidChannelDefinitionError(ValueError):
    """
    Raised when a channel or message handler is defined with an invalid shape.
    """


class RouteInvariantError(RuntimeError):
    """Raised when a selected route fails to match its address."""


class ChannelNotFoundError(LookupError):
    def __init__(self, address: str) -> None:
        super().__init__(f"Couldn't resolve address: {address}")
        self.address = address


class Header(FieldInfo):
    pass


class Payload(FieldInfo):
    pass


class Parameter(FieldInfo):
    pass


@dataclass(frozen=True)
class Depends:
    func: Callable[..., Any]
    _: KW_ONLY
    use_cache: bool = True


@dataclass(frozen=True)
class MessageReceive:
    message: MessageScope
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
class CallableResolver(ABC):
    func: Callable[..., Any]
    resolvers: dict[str, Resolver[Any]]
    dependencies: dict[str, DependencyResolver]

    async def resolve(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> dict[str, Any]:
        resolver_result = {
            name: resolver.resolve(message_receive, send)
            for name, resolver in self.resolvers.items()
        }

        dependency_result = dict(
            zip(
                self.dependencies.keys(),
                await asyncio.gather(
                    *(
                        dependency_cache.resolve(
                            dependency, message_receive, send, async_exit_stack
                        )
                        for dependency in self.dependencies.values()
                    )
                ),
            )
        )

        return {**resolver_result, **dependency_result}

    @abstractmethod
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> Any: ...


@dataclass(frozen=True)
class DependencyResolver(CallableResolver, ABC):
    use_cache: bool


@dataclass(frozen=True)
class SyncDependencyResolver(DependencyResolver):
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> Any:
        return await asyncio.to_thread(
            self.func,
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            ),
        )


@dataclass(frozen=True)
class AsyncDependencyResolver(DependencyResolver):
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> Any:
        return await self.func(
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            )
        )


class AsyncYieldingDependencyResolver(DependencyResolver):
    @cached_property
    def async_context_manager(self) -> Callable[..., AbstractAsyncContextManager[Any]]:
        return asynccontextmanager(self.func)

    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> Any:
        return await async_exit_stack.enter_async_context(
            self.async_context_manager(
                **await self.resolve(
                    message_receive, send, dependency_cache, async_exit_stack
                )
            )
        )


class SyncYieldingDependencyResolver(DependencyResolver):
    @cached_property
    def async_generator_func(self) -> Callable[..., AsyncGenerator[Any]]:
        return asyncify_generator(self.func)

    @cached_property
    def async_context_manager(self) -> Callable[..., AbstractAsyncContextManager[Any]]:
        return asynccontextmanager(self.async_generator_func)

    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> Any:
        return await async_exit_stack.enter_async_context(
            self.async_context_manager(
                **await self.resolve(
                    message_receive, send, dependency_cache, async_exit_stack
                )
            )
        )


class DependencyCache:
    def __init__(self, loop: AbstractEventLoop) -> None:
        self.loop = loop
        self.cache: dict[Callable[..., Any], Task[Any]] = {}

    def resolve(
        self,
        dependency: DependencyResolver,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        async_exit_stack: AsyncExitStack,
    ) -> Task[Any]:
        if dependency.use_cache:
            if dependency.func not in self.cache:
                self.cache[dependency.func] = self.loop.create_task(
                    dependency.call(message_receive, send, self, async_exit_stack)
                )
            return self.cache[dependency.func]
        return self.loop.create_task(
            dependency.call(message_receive, send, self, async_exit_stack)
        )


@dataclass(frozen=True)
class Channel(CallableResolver, ABC):
    address: str
    address_pattern: Pattern[str]
    parameters: set[str]

    async def __call__(
        self,
        scope: MessageScope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
        parameters: dict[str, str] | None = None,
    ) -> None:
        parameters = self.match(scope["address"]) if parameters is None else parameters
        if parameters is None:
            raise RouteInvariantError(
                f"Selected route did not match address {scope['address']!r}"
            )
        message_receive = MessageReceive(scope, parameters)
        dependency_cache = DependencyCache(asyncio.get_event_loop())
        async with AsyncExitStack() as async_exit_stack:
            await self.call(message_receive, send, dependency_cache, async_exit_stack)

    def match(self, address: str) -> dict[str, str] | None:
        match = self.address_pattern.match(address)
        if match:
            return match.groupdict()
        return None


@dataclass(frozen=True)
class SyncChannel(Channel):
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> None:
        await asyncio.to_thread(
            self.func,
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            ),
        )


@dataclass(frozen=True)
class AsyncChannel(Channel):
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> None:
        await self.func(
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            )
        )


async def handle_async_generator(
    agen: AsyncGenerator[Any], send: AMGISendCallable
) -> None:
    try:
        while True:
            try:
                message = await agen.__anext__()
            except StopAsyncIteration:
                return

            while True:
                try:
                    await send_message(send, message)
                except Exception as exc:
                    try:
                        message = await agen.athrow(exc)
                    except StopAsyncIteration:
                        return
                else:
                    break
    finally:
        await agen.aclose()


class AsyncGeneratorChannel(Channel):
    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> None:
        agen = self.func(
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            )
        )

        await handle_async_generator(agen, send)


class SyncGeneratorChannel(Channel):
    @cached_property
    def async_generator_func(self) -> Callable[..., AsyncGenerator[Any]]:
        return asyncify_generator(self.func)

    async def call(
        self,
        message_receive: MessageReceive,
        send: AMGISendCallable,
        dependency_cache: DependencyCache,
        async_exit_stack: AsyncExitStack,
    ) -> None:

        agen = self.async_generator_func(
            **await self.resolve(
                message_receive, send, dependency_cache, async_exit_stack
            )
        )

        await handle_async_generator(agen, send)


def parameter_resolver(
    name: str, parameter: inspect.Parameter, address_parameters: set[str]
) -> Resolver[Any] | DependencyResolver:
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
        if isinstance(annotation, Depends):
            resolvers, dependencies = resolvers_dependencies(
                annotation.func, address_parameters
            )

            if inspect.iscoroutinefunction(annotation.func):
                return AsyncDependencyResolver(
                    annotation.func, resolvers, dependencies, annotation.use_cache
                )
            if inspect.isasyncgenfunction(annotation.func):
                return AsyncYieldingDependencyResolver(
                    annotation.func, resolvers, dependencies, annotation.use_cache
                )
            if inspect.isgeneratorfunction(annotation.func):
                return SyncYieldingDependencyResolver(
                    annotation.func, resolvers, dependencies, annotation.use_cache
                )
            return SyncDependencyResolver(
                annotation.func, resolvers, dependencies, annotation.use_cache
            )

    if get_origin(parameter.annotation) is MessageSender:
        return MessageSenderResolver(parameter.annotation)

    type_ = object if parameter.empty == parameter.annotation else parameter.annotation
    return PayloadResolver(type_)


def resolvers_dependencies(
    func: Callable[..., Any], address_parameters: set[str]
) -> tuple[dict[str, Resolver[Any]], dict[str, DependencyResolver]]:
    signature = inspect.signature(func)
    resolvers = {}
    dependencies = {}
    for name, parameter in signature.parameters.items():
        resolver = parameter_resolver(name, parameter, address_parameters)
        if isinstance(resolver, Resolver):
            resolvers[name] = resolver
        else:
            dependencies[name] = resolver
    return resolvers, dependencies


def get_channel(func: Callable[..., Any], address: str) -> Channel:
    address_parameters = get_address_parameters(address)
    address_pattern = get_address_pattern(address)
    resolvers, dependencies = resolvers_dependencies(func, address_parameters)

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
        return AsyncChannel(
            func, resolvers, dependencies, address, address_pattern, address_parameters
        )
    if inspect.isasyncgenfunction(func):
        return AsyncGeneratorChannel(
            func, resolvers, dependencies, address, address_pattern, address_parameters
        )
    if inspect.isgeneratorfunction(func):
        return SyncGeneratorChannel(
            func, resolvers, dependencies, address, address_pattern, address_parameters
        )
    return SyncChannel(
        func, resolvers, dependencies, address, address_pattern, address_parameters
    )


class Router:
    def __init__(self) -> None:
        self.channels: list[Channel] = []

    def add_channel(self, address: str, func: Callable[..., Any]) -> None:
        self.channels.append(get_channel(func, address))

    async def __call__(
        self, scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        try:
            await self.call_channel(scope, receive, send)

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

    async def call_channel(
        self, scope: MessageScope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        address = scope["address"]
        for channel in self.channels:
            parameters = channel.match(address)
            if parameters is not None:
                await channel(scope, receive, send, parameters)
                return
        raise ChannelNotFoundError(address)
