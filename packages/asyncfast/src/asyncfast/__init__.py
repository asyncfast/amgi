import inspect
import json
import re
from functools import cached_property
from functools import partial
from re import Pattern
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Counter
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar

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
        annotations = list(_generate_annotations(address, function))
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
            address, address_pattern, function, headers, parameters, payload
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
                "schemas": definitions,
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


def _generate_annotations(
    address: str,
    function: Callable[..., Any],
) -> Generator[Tuple[str, Type[Annotated[Any, Any]]], None, None]:

    address_parameters = _get_address_parameters(address)
    signature = inspect.signature(function)

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
    ) -> None:
        self._address = address
        self._address_pattern = address_pattern
        self._handler = handler
        self._headers = headers
        self._parameters = parameters
        self._payload = payload

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
                    "address": message.address,
                    "headers": message.headers,
                    "payload": message.payload,
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


def _generate_operations(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        yield f"receive{channel.title}", {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{channel.title}"},
        }


class Header(FieldInfo):
    pass


class Payload(FieldInfo):
    pass


class Parameter(FieldInfo):
    pass


def _get_address_parameters(address: str) -> Set[str]:
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
