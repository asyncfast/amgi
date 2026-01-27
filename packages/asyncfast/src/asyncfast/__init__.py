import asyncio
import inspect
import re
from collections import Counter
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from functools import cached_property
from functools import partial
from inspect import Signature
from re import Pattern
from types import UnionType
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import get_args
from typing import get_origin
from typing import TypeVar
from typing import Union

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from amgi_types import Scope
from asyncfast.bindings import Binding
from pydantic import BaseModel
from pydantic import create_model
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo
from pydantic.json_schema import GenerateJsonSchema
from pydantic.json_schema import JsonSchemaMode
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
M = TypeVar("M", bound=Mapping[str, Any])
Lifespan = Callable[["AsyncFast"], AbstractAsyncContextManager[None]]


_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
_PARAMETER_PATTERN = re.compile(r"{(.*)}")


class InvalidChannelDefinitionError(ValueError):
    """
    Raised when a channel or message handler is defined with an invalid shape.
    """


async def _send_message(send: AMGISendCallable, message: Mapping[str, Any]) -> None:
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
        await _send_message(self._send, message)


class _Field:
    def __init__(self, type_: type):
        self.type = type_
        self.type_adapter = TypeAdapter[Any](type_)

    def __hash__(self) -> int:
        return hash(self.type)


class Message(Mapping[str, Any]):

    __address__: ClassVar[str | None] = None
    __headers__: ClassVar[dict[str, _Field]]
    __headers_model__: ClassVar[type[BaseModel] | None]
    __parameters__: ClassVar[dict[str, TypeAdapter[Any]]]
    __payload__: ClassVar[tuple[str, _Field] | None]
    __bindings__: ClassVar[dict[str, _Field]]

    def __init_subclass__(cls, address: str | None = None, **kwargs: Any) -> None:
        cls.__address__ = address
        annotations = list(_generate_message_annotations(address, cls.__annotations__))

        headers = {
            name: _Field(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Header)
        }

        parameters = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Parameter)
        }

        bindings = {
            name: _Field(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Binding)
        }

        payloads = [
            (name, _Field(annotated))
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Payload)
        ]

        assert len(payloads) <= 1, "Channel must have no more than 1 payload"

        payload = payloads[0] if len(payloads) == 1 else None

        cls.__headers__ = headers
        cls.__parameters__ = parameters
        cls.__payload__ = payload
        cls.__bindings__ = bindings

    def __getitem__(self, key: str, /) -> Any:
        if key == "address":
            return self._get_address()
        elif key == "headers":
            return self._get_headers()
        elif key == "payload" and self.__payload__:
            return self._get_payload()
        elif key == "bindings" and self.__bindings__:
            return self._get_bindings()
        raise KeyError(key)

    def __len__(self) -> int:
        payload = 1 if self.__payload__ else 0
        bindings = 1 if self.__bindings__ else 0
        return 2 + payload + bindings

    def __iter__(self) -> Iterator[str]:
        yield from ("address", "headers")
        if self.__payload__:
            yield "payload"
        if self.__bindings__:
            yield "bindings"

    def _get_address(self) -> str | None:
        if self.__address__ is None:
            return None
        parameters = {
            name: type_adapter.dump_python(getattr(self, name))
            for name, type_adapter in self.__parameters__.items()
        }

        return self.__address__.format(**parameters)

    def _generate_headers(self) -> Iterable[tuple[str, bytes]]:
        for name, field in self.__headers__.items():
            _, annotation = get_args(field.type)
            alias = annotation.alias if annotation.alias else name.replace("_", "-")
            yield alias, self._get_value(name, field.type_adapter)

    def _get_headers(self) -> Iterable[tuple[bytes, bytes]]:
        return [(name.encode(), value) for name, value in self._generate_headers()]

    def _get_value(self, name: str, type_adapter: TypeAdapter[Any]) -> bytes:
        value = getattr(self, name)
        python = type_adapter.dump_python(value, mode="json")
        if isinstance(python, str):
            return python.encode()
        if isinstance(python, bytes):
            return python
        return type_adapter.dump_json(value)

    def _get_payload(self) -> bytes | None:
        if self.__payload__ is None:
            return None
        name, field = self.__payload__
        return field.type_adapter.dump_json(getattr(self, name))

    def _get_bindings(self) -> dict[str, dict[str, Any]]:
        bindings: dict[str, dict[str, Any]] = {}
        for name, field in self.__bindings__.items():
            binding_type = get_args(field.type)[1]
            assert isinstance(binding_type, Binding)

            bindings.setdefault(binding_type.__protocol__, {})[
                binding_type.__field_name__
            ] = self._get_value(name, field.type_adapter)
        return bindings

    @classmethod
    def _headers_model(cls) -> type[BaseModel] | None:
        if not hasattr(cls, "__headers_model__"):
            if cls.__headers__:
                cls.__headers_model__ = _create_headers_model(
                    f"{cls.__name__}Headers", cls.__headers__
                )
            else:
                cls.__headers_model__ = None
        return cls.__headers_model__


def _generate_message_annotations(
    address: str | None,
    fields: dict[str, Any],
) -> Generator[tuple[str, type[Annotated[Any, Any]]], None, None]:
    address_parameters = _get_address_parameters(address)
    for name, field in fields.items():
        if get_origin(field) is Annotated:
            yield name, field
        elif name in address_parameters:
            yield name, Annotated[field, Parameter()]  # type: ignore[misc]
        else:
            yield name, Annotated[field, Payload()]  # type: ignore[misc]


def _is_message(cls: type[Any]) -> bool:
    try:
        return issubclass(cls, Message)
    except TypeError:
        return False


def _is_union(type_annotation: type) -> bool:
    origin = get_origin(type_annotation)
    return origin is Union or origin is UnionType


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
        signature = inspect.signature(function)

        messages = []
        return_annotation = signature.return_annotation
        if return_annotation is not Signature.empty and (
            get_origin(return_annotation) is AsyncGenerator
            or get_origin(return_annotation) is Generator
        ):
            async_generator_type = get_args(return_annotation)[0]
            if _is_union(async_generator_type):
                messages = [
                    type for type in get_args(async_generator_type) if _is_message(type)
                ]
            elif _is_message(async_generator_type):
                messages = [get_args(return_annotation)[0]]

        annotations = list(_generate_annotations(address, signature))

        headers = {
            name: _Field(annotated)
            for name, annotated in annotations
            if get_origin(annotated) is Annotated
            and isinstance(get_args(annotated)[1], Header)
        }

        parameters = {
            name: _Field(annotated)
            for name, annotated in annotations
            if get_origin(annotated) is Annotated
            and isinstance(get_args(annotated)[1], Parameter)
        }

        payloads = [
            (name, _Field(annotated))
            for name, annotated in annotations
            if get_origin(annotated) is Annotated
            and isinstance(get_args(annotated)[1], Payload)
        ]

        bindings = {
            name: _Field(annotated)
            for name, annotated in annotations
            if get_origin(annotated) is Annotated
            and isinstance(get_args(annotated)[1], Binding)
        }

        message_senders = [
            name
            for name, annotated in annotations
            if get_origin(annotated) is MessageSender
        ]
        for name, annotated in annotations:
            if get_origin(annotated) is MessageSender:
                (message_sender_type,) = get_args(annotated)
                if _is_union(message_sender_type):
                    messages = [
                        type
                        for type in get_args(message_sender_type)
                        if _is_message(type)
                    ]
                elif _is_message(message_sender_type):
                    messages = [message_sender_type]

        if len(payloads) > 1:
            raise InvalidChannelDefinitionError(
                "Channel must have no more than 1 payload"
            )

        payload = payloads[0] if len(payloads) == 1 else None

        if len(message_senders) > 1:
            raise InvalidChannelDefinitionError(
                "Channel must have no more than 1 message sender"
            )

        message_sender = message_senders[0] if len(message_senders) == 1 else None

        address_pattern = _address_pattern(address)

        channel = _Channel(
            address,
            address_pattern,
            function,
            headers,
            parameters,
            payload,
            messages,
            bindings,
            message_sender,
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
                    break

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
            for field in channel._bindings.values():
                yield hash(field), "validation", field.type_adapter.core_schema

            headers_model = channel.headers_model
            if headers_model:
                yield hash(headers_model), "validation", TypeAdapter(
                    headers_model
                ).core_schema
            payload = channel.payload
            if payload:
                _, field = payload
                yield hash(field), "validation", field.type_adapter.core_schema

            for message in channel.messages:
                if message.__payload__:
                    _, field = message.__payload__

                    yield hash(field), "serialization", field.type_adapter.core_schema

                for field in message.__bindings__.values():
                    yield hash(
                        field.type
                    ), "serialization", field.type_adapter.core_schema

                message_headers_model = message._headers_model()
                if message_headers_model:
                    yield hash(message_headers_model), "serialization", TypeAdapter(
                        message_headers_model
                    ).core_schema


def _generate_annotations(
    address: str,
    signature: Signature,
) -> Generator[tuple[str, type[Any]], None, None]:

    address_parameters = _get_address_parameters(address)

    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if get_origin(annotation) is Annotated:
            if parameter.default != parameter.empty:
                args = get_args(annotation)
                args[1].default = parameter.default
            yield name, annotation
        elif get_origin(annotation) is MessageSender:
            yield name, annotation
        elif name in address_parameters:
            yield name, Annotated[annotation, Parameter()]  # type: ignore[misc]
        else:
            yield name, Annotated[annotation, Payload()]  # type: ignore[misc]


async def _handle_async_generator(
    handler: Callable[..., AsyncGenerator[Any, None]],
    arguments: dict[str, Any],
    send: AMGISendCallable,
) -> None:
    agen = handler(**arguments)
    exception: Exception | None = None
    while True:
        try:
            if exception is None:
                send_message = await agen.__anext__()
            else:
                send_message = await agen.athrow(exception)
            try:
                await _send_message(send, send_message)
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


async def _handle_generator(
    handler: Callable[..., Generator[Any, None, None]],
    arguments: dict[str, Any],
    send: AMGISendCallable,
) -> None:
    gen = handler(**arguments)
    exception: Exception | None = None
    while True:
        if exception is None:
            send_message = await asyncio.to_thread(next, gen, None)
        else:
            send_message = await asyncio.to_thread(_throw_or_none, gen, exception)
        if send_message is None:
            break
        try:
            await _send_message(send, send_message)
        except Exception as e:
            exception = e
        else:
            exception = None


async def _receive_messages(
    receive: AMGIReceiveCallable,
) -> AsyncGenerator[MessageReceiveEvent, None]:
    more_messages = True
    while more_messages:
        message = await receive()
        assert message["type"] == "message.receive"
        yield message
        more_messages = message.get("more_messages", False)


def _generate_field_definitions(
    headers: Mapping[str, _Field],
) -> Iterator[tuple[str, Any]]:
    for name, field in headers.items():
        type_, annotation = get_args(field.type)
        alias = annotation.alias if annotation.alias else name.replace("_", "-")
        yield alias, (type_, annotation)


def _create_headers_model(
    headers_name: str, headers: Mapping[str, _Field]
) -> type[BaseModel]:
    return create_model(
        headers_name, __base__=BaseModel, **dict(_generate_field_definitions(headers))
    )


class _Channel:

    def __init__(
        self,
        address: str,
        address_pattern: Pattern[str],
        handler: Callable[..., Awaitable[None]],
        headers: Mapping[str, _Field],
        parameters: Mapping[str, _Field],
        payload: tuple[str, _Field] | None,
        messages: Sequence[type[Message]],
        bindings: Mapping[str, _Field],
        message_sender: str | None,
    ) -> None:
        self._address = address
        self._address_pattern = address_pattern
        self._handler = handler
        self._headers = headers
        self._parameters = parameters
        self._payload = payload
        self._messages = messages
        self._bindings = bindings
        self._message_sender = message_sender

    @property
    def address(self) -> str:
        return self._address

    @property
    def name(self) -> str:
        return self._handler.__name__

    @cached_property
    def title(self) -> str:
        return "".join(part.title() for part in self.name.split("_"))

    @property
    def headers(self) -> Mapping[str, _Field]:
        return self._headers

    @cached_property
    def headers_model(self) -> type[BaseModel] | None:
        if self._headers:
            return _create_headers_model(f"{self.title}Headers", self._headers)
        return None

    @property
    def payload(self) -> tuple[str, _Field] | None:
        return self._payload

    @property
    def parameters(self) -> Mapping[str, _Field]:
        return self._parameters

    @property
    def messages(self) -> Sequence[type[Message]]:
        return self._messages

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
            arguments = dict(self._generate_arguments(message, parameters, send))

            if inspect.isasyncgenfunction(self._handler):
                await _handle_async_generator(self._handler, arguments, send)
            elif inspect.isgeneratorfunction(self._handler):
                await _handle_generator(self._handler, arguments, send)
            elif inspect.iscoroutinefunction(self._handler):
                await self._handler(**arguments)
            else:
                await asyncio.to_thread(self._handler, **arguments)

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

    def _generate_arguments(
        self,
        message_receive_event: MessageReceiveEvent,
        parameters: dict[str, str],
        send: AMGISendCallable,
    ) -> Generator[tuple[str, Any], None, None]:
        yield from self._generate_headers(message_receive_event)
        yield from self._generate_payload(message_receive_event)
        yield from self._generate_parameters(parameters)
        yield from self._generate_bindings(message_receive_event)
        if self._message_sender:
            yield self._message_sender, MessageSender(send)

    def _generate_headers(
        self, message_receive_event: MessageReceiveEvent
    ) -> Generator[tuple[str, Any], None, None]:
        if self.headers:
            headers = _Headers(message_receive_event["headers"])
            for name, field in self.headers.items():
                annotated_args = get_args(field.type)
                header_alias = annotated_args[1].alias
                alias = header_alias if header_alias else name.replace("_", "-")
                header = headers.get(
                    alias, annotated_args[1].get_default(call_default_factory=True)
                )
                value = TypeAdapter(annotated_args[0]).validate_python(
                    header, from_attributes=True
                )
                yield name, value

    def _generate_payload(
        self, message_receive_event: MessageReceiveEvent
    ) -> Generator[tuple[str, Any], None, None]:
        if self.payload:
            name, field = self.payload
            payload: bytes | None = message_receive_event.get("payload")
            if payload is None:
                value = field.type_adapter.validate_python(None)
            else:
                value = field.type_adapter.validate_json(payload)
            yield name, value

    def _generate_bindings(
        self, message_receive_event: MessageReceiveEvent
    ) -> Generator[tuple[str, Any], None, None]:
        if self._bindings:
            bindings = message_receive_event.get("bindings", {})
            for name, field in self._bindings.items():
                binding_type = get_args(field.type)[1]
                assert isinstance(binding_type, Binding)

                yield name, field.type_adapter.validate_python(
                    bindings.get(binding_type.__protocol__, {}).get(
                        binding_type.__field_name__
                    )
                )

    def _generate_parameters(
        self, parameters: dict[str, str]
    ) -> Generator[tuple[str, Any], None, None]:
        if self._parameters:
            for name, field in self._parameters.items():
                yield name, field.type_adapter.validate_python(parameters[name])


def _generate_messages(
    channels: Iterable[_Channel],
    field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel in channels:
        message = {}

        headers_model = channel.headers_model
        if headers_model:
            message["headers"] = field_mapping[
                hash(channel.headers_model), "validation"
            ]

        payload = channel.payload
        if payload:
            _, field = payload
            message["payload"] = field_mapping[hash(field), "validation"]

        bindings: dict[str, dict[str, Any]]
        if channel._bindings:
            bindings = {}
            for field in channel._bindings.values():
                binding_type = get_args(field.type)[1]
                assert isinstance(binding_type, Binding)

                bindings.setdefault(binding_type.__protocol__, {})[
                    binding_type.__field_name__
                ] = field_mapping[hash(field), "validation"]
            message["bindings"] = bindings

        yield f"{channel.title}Message", message

        for channel_message in channel.messages:
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
                for field in channel_message.__bindings__.values():
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
        message_name = f"{channel.title}Message"
        channel_definition = {
            "address": channel.address,
            "messages": {
                message_name: {"$ref": f"#/components/messages/{message_name}"}
            },
        }

        if channel.parameters:
            channel_definition["parameters"] = {name: {} for name in channel.parameters}

        yield channel.title, channel_definition

        for message in channel.messages:
            message_channel_definition = {
                "address": message.__address__,
                "messages": {
                    message.__name__: {
                        "$ref": f"#/components/messages/{message.__name__}"
                    }
                },
            }

            if message.__parameters__:
                message_channel_definition["parameters"] = {
                    name: {} for name in message.__parameters__
                }

            yield message.__name__, message_channel_definition


def _generate_operations(
    channels: Iterable[_Channel],
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    for channel in channels:
        yield f"receive{channel.title}", {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{channel.title}"},
        }

        for message in channel.messages:
            yield f"send{message.__name__}", {
                "action": "send",
                "channel": {"$ref": f"#/channels/{message.__name__}"},
            }


class Header(FieldInfo):
    pass


class Payload(FieldInfo):
    pass


class Parameter(FieldInfo):
    pass


def _get_address_parameters(address: str | None) -> set[str]:
    if address is None:
        return set()
    parameters = _PARAMETER_PATTERN.findall(address)
    for parameter in parameters:
        assert _FIELD_PATTERN.match(parameter), f"Parameter '{parameter}' is not valid"

    duplicates = {item for item, count in Counter(parameters).items() if count > 1}
    assert len(duplicates) == 0, f"Address contains duplicate parameters: {duplicates}"
    return set(parameters)


class _Headers(Mapping[str, str]):

    def __init__(self, raw_list: Iterable[tuple[bytes, bytes]]) -> None:
        self.raw_list = list(raw_list)

    def __getitem__(self, key: str, /) -> str:
        for header_key, header_value in self.raw_list:
            if header_key.decode() == key:
                return header_value.decode()
        raise KeyError(key)

    def __len__(self) -> int:
        return len(self.raw_list)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def keys(self) -> list[str]:  # type: ignore[override]
        return [key.decode() for key, _ in self.raw_list]


def _address_pattern(address: str) -> Pattern[str]:
    index = 0
    address_regex = "^"
    for match in _PARAMETER_PATTERN.finditer(address):
        (name,) = match.groups()
        address_regex += re.escape(address[index : match.start()])
        address_regex += f"(?P<{name}>.*)"

        index = match.end()

    address_regex += re.escape(address[index:]) + "$"
    return re.compile(address_regex)
