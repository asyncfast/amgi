import inspect
import json
import re
from collections.abc import AsyncGenerator
from functools import cached_property
from functools import partial
from inspect import Signature
from re import Pattern
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Counter
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from amgi_types import Scope
from pydantic import BaseModel
from pydantic import create_model
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo
from pydantic.json_schema import GenerateJsonSchema
from pydantic.json_schema import JsonSchemaMode
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema
from typing_extensions import Annotated
from typing_extensions import get_args
from typing_extensions import get_origin


DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
_PARAMETER_PATTERN = re.compile(r"{(.*)}")


class Message(Mapping[str, Any]):

    __address__: ClassVar[Optional[str]] = None
    __headers__: ClassVar[Dict[str, TypeAdapter[Any]]]
    __parameters__: ClassVar[Dict[str, TypeAdapter[Any]]]
    __payload__: ClassVar[Optional[Tuple[str, TypeAdapter[Any]]]]

    def __init_subclass__(cls, address: Optional[str] = None, **kwargs: Any) -> None:
        cls.__address__ = address
        annotations = list(_generate_message_annotations(address, cls.__annotations__))

        headers = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Header)
        }

        parameters = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Parameter)
        }

        payloads = [
            (name, TypeAdapter(annotated))
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Payload)
        ]

        assert len(payloads) <= 1, "Channel must have no more than 1 payload"

        payload = payloads[0] if len(payloads) == 1 else None

        cls.__headers__ = headers
        cls.__parameters__ = parameters
        cls.__payload__ = payload

    def __getitem__(self, key: str, /) -> Any:
        if key == "address":
            return self._get_address()
        elif key == "headers":
            return self._get_headers()
        elif key == "payload":
            return self._get_payload()
        raise KeyError(key)

    def __len__(self) -> int:
        return 3

    def __iter__(self) -> Iterator[str]:
        return iter(("address", "headers", "payload"))

    def _get_address(self) -> Optional[str]:
        if self.__address__ is None:
            return None
        parameters = {
            name: type_adapter.dump_python(getattr(self, name))
            for name, type_adapter in self.__parameters__.items()
        }

        return self.__address__.format(**parameters)

    def _get_headers(self) -> Iterable[Tuple[bytes, bytes]]:
        return [
            (name.encode(), self._get_value(name, type_adapter))
            for name, type_adapter in self.__headers__.items()
        ]

    def _get_value(self, name: str, type_adapter: TypeAdapter[Any]) -> bytes:
        json_value = type_adapter.dump_json(getattr(self, name))
        value = json.loads(json_value)
        if isinstance(value, str):
            return value.encode()
        return json_value

    def _get_payload(self) -> Optional[bytes]:
        if self.__payload__ is None:
            return None
        name, type_adapter = self.__payload__
        return type_adapter.dump_json(getattr(self, name))


def _generate_message_annotations(
    address: Optional[str],
    fields: dict[str, Any],
) -> Generator[Tuple[str, Type[Annotated[Any, Any]]], None, None]:
    address_parameters = _get_address_parameters(address)

    for name, field in fields.items():
        if get_origin(field) is Annotated:
            yield name, field
        elif name in address_parameters:
            yield name, Annotated[field, Parameter()]  # type: ignore[misc]
        else:
            yield name, Annotated[field, Payload()]  # type: ignore[misc]


class AsyncFast:
    def __init__(
        self, title: Optional[str] = None, version: Optional[str] = None
    ) -> None:
        self._channels: List[Channel] = []
        self._title = title or "AsyncFast"
        self._version = version or "0.1.0"

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
        if (
            return_annotation is not Signature.empty
            and get_origin(return_annotation) is AsyncGenerator
        ):
            async_generator_type = get_args(return_annotation)[0]
            if get_origin(async_generator_type) is Union:  # type: ignore[comparison-overlap]
                messages = [
                    type
                    for type in get_args(async_generator_type)
                    if inspect.isclass(type) and issubclass(type, Message)
                ]
            elif inspect.isclass(async_generator_type) and issubclass(
                async_generator_type, Message
            ):
                messages = [get_args(return_annotation)[0]]

        annotations = list(_generate_annotations(address, signature))

        headers = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Header)
        }

        parameters = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Parameter)
        }

        payloads = [
            (name, TypeAdapter(annotated))
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Payload)
        ]

        assert len(payloads) <= 1, "Channel must have no more than 1 payload"

        payload = payloads[0] if len(payloads) == 1 else None

        address_pattern = _address_pattern(address)

        channel = Channel(
            address, address_pattern, function, headers, parameters, payload, messages
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
                    lifespan_startup_complete_event: LifespanStartupCompleteEvent = {
                        "type": "lifespan.startup.complete"
                    }
                    await send(lifespan_startup_complete_event)
                elif message["type"] == "lifespan.shutdown":
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

    def asyncapi(self) -> Dict[str, Any]:
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
    ) -> Generator[Tuple[int, JsonSchemaMode, CoreSchema], None, None]:
        for channel in self._channels:
            headers_model = channel.headers_model
            if headers_model:
                yield hash(headers_model), "serialization", TypeAdapter(
                    headers_model
                ).core_schema
            payload = channel.payload
            if payload:
                _, type_adapter = payload
                yield hash(
                    type_adapter._type
                ), "serialization", type_adapter.core_schema

            for message in channel.messages:
                if message.__payload__:
                    _, type_adapter = message.__payload__

                    yield hash(
                        type_adapter._type
                    ), "serialization", type_adapter.core_schema


def _generate_annotations(
    address: str,
    signature: Signature,
) -> Generator[Tuple[str, Type[Annotated[Any, Any]]], None, None]:

    address_parameters = _get_address_parameters(address)

    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if get_origin(annotation) is Annotated:
            if parameter.default != parameter.empty:
                args = get_args(annotation)
                args[1].default = parameter.default
            yield name, annotation
        elif name in address_parameters:
            yield name, Annotated[annotation, Parameter()]  # type: ignore[misc]
        else:
            yield name, Annotated[annotation, Payload()]  # type: ignore[misc]


class Channel:

    def __init__(
        self,
        address: str,
        address_pattern: Pattern[str],
        handler: Callable[..., Awaitable[None]],
        headers: Mapping[str, TypeAdapter[Any]],
        parameters: Mapping[str, TypeAdapter[Any]],
        payload: Optional[Tuple[str, TypeAdapter[Any]]],
        messages: Sequence[Type[Message]],
    ) -> None:
        self._address = address
        self._address_pattern = address_pattern
        self._handler = handler
        self._headers = headers
        self._parameters = parameters
        self._payload = payload
        self._messages = messages

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
    def headers(self) -> Mapping[str, TypeAdapter[Any]]:
        return self._headers

    @cached_property
    def headers_model(self) -> Optional[Type[BaseModel]]:
        if self._headers:
            headers_name = f"{self.title}Headers"
            headers_model = create_model(
                headers_name,
                **{
                    name.replace("_", "-"): value._type
                    for name, value in self._headers.items()
                },
                __base__=BaseModel,
            )
            return headers_model
        return None

    @property
    def payload(self) -> Optional[Tuple[str, TypeAdapter[Any]]]:
        return self._payload

    @property
    def parameters(self) -> Mapping[str, TypeAdapter[Any]]:
        return self._parameters

    @property
    def messages(self) -> Sequence[Type[Message]]:
        return self._messages

    def match(self, address: str) -> Optional[Dict[str, str]]:
        match = self._address_pattern.match(address)
        if match:
            return match.groupdict()
        return None

    async def __call__(
        self,
        scope: MessageScope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
        parameters: Dict[str, str],
    ) -> None:
        arguments = dict(self._generate_arguments(scope, parameters))
        if inspect.isasyncgenfunction(self._handler):
            async for message in self._handler(**arguments):
                message_send_event: MessageSendEvent = {
                    "type": "message.send",
                    "address": message["address"],
                    "headers": message["headers"],
                    "payload": message.get("payload"),
                }
                await send(message_send_event)
        else:
            await self._handler(**arguments)

    def _generate_arguments(
        self, scope: MessageScope, parameters: Dict[str, str]
    ) -> Generator[Tuple[str, Any], None, None]:

        if self.headers:
            headers = Headers(scope["headers"])
            for name, type_adapter in self.headers.items():
                annotated_args = get_args(type_adapter._type)
                header_alias = annotated_args[1].alias
                alias = header_alias if header_alias else name.replace("_", "-")
                header = headers.get(
                    alias, annotated_args[1].get_default(call_default_factory=True)
                )
                value = TypeAdapter(annotated_args[0]).validate_python(
                    header, from_attributes=True
                )
                yield name, value

        if self.payload:
            name, type_adapter = self.payload
            payload = scope.get("payload")
            payload_obj = None if payload is None else json.loads(payload)
            value = type_adapter.validate_python(payload_obj, from_attributes=True)
            yield name, value

        if self._parameters:
            for name, type_adapter in self._parameters.items():
                yield name, type_adapter.validate_python(parameters[name])


def _generate_messages(
    channels: Iterable[Channel],
    field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        message = {}

        headers_model = channel.headers_model
        if headers_model:
            message["headers"] = field_mapping[
                hash(channel.headers_model), "serialization"
            ]

        payload = channel.payload
        if payload:
            _, type_adapter = payload
            message["payload"] = field_mapping[
                hash(type_adapter._type), "serialization"
            ]

        yield f"{channel.title}Message", message

        for channel_message in channel.messages:
            message_message = {}

            if channel_message.__payload__:
                _, type_adapter = channel_message.__payload__
                message_message["payload"] = field_mapping[
                    hash(type_adapter._type), "serialization"
                ]

            yield channel_message.__name__, message_message


def _generate_channels(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
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
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
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


def _get_address_parameters(address: Optional[str]) -> Set[str]:
    if address is None:
        return set()
    parameters = _PARAMETER_PATTERN.findall(address)
    for parameter in parameters:
        assert _FIELD_PATTERN.match(parameter), f"Parameter '{parameter}' is not valid"

    duplicates = {item for item, count in Counter(parameters).items() if count > 1}
    assert len(duplicates) == 0, f"Address contains duplicate parameters: {duplicates}"
    return set(parameters)


class Headers(Mapping[str, str]):

    def __init__(self, raw_list: Iterable[Tuple[bytes, bytes]]) -> None:
        self.raw_list = list(raw_list)

    def __getitem__(self, key: str, /) -> str:
        for header_key, header_value in self.raw_list:
            if header_key.decode().lower() == key.lower():
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
