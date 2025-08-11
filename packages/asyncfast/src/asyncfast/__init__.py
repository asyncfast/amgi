import dataclasses
import inspect
import json
from functools import cached_property
from functools import partial
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar

from pydantic import BaseModel
from pydantic import create_model
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo
from pydantic.json_schema import GenerateJsonSchema
from pydantic.json_schema import JsonSchemaMode
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema
from types_acgi import ACGIReceiveCallable
from types_acgi import ACGISendCallable
from types_acgi import LifespanShutdownCompleteEvent
from types_acgi import LifespanStartupCompleteEvent
from types_acgi import MessageScope
from types_acgi import MessageSendEvent
from types_acgi import Scope
from typing_extensions import Annotated
from typing_extensions import get_args
from typing_extensions import get_origin

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


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
        annotations = list(_generate_annotations(function))
        headers = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Header)
        }

        payloads = [
            (name, TypeAdapter(annotated))
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Payload)
        ]

        assert len(payloads) <= 1, "Channel must have no more than 1 payload"

        payload = payloads[0] if len(payloads) == 1 else None

        channel = Channel(address, function, headers, payload)

        self._channels.append(channel)
        return function

    async def __call__(
        self, scope: Scope, receive: ACGIReceiveCallable, send: ACGISendCallable
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
                if channel.address == address:
                    await channel(scope, receive, send)
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
    function: Callable[..., Any],
) -> Generator[Tuple[str, Type[Annotated[Any, Any]]], None, None]:
    signature = inspect.signature(function)

    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if get_origin(annotation) is Annotated:
            if parameter.default != parameter.empty:
                args = get_args(annotation)
                args[1].default = parameter.default
            yield name, annotation
        elif issubclass(annotation, BaseModel) or dataclasses.is_dataclass(annotation):
            yield name, Annotated[annotation, Payload()]  # type: ignore[misc]


class Channel:

    def __init__(
        self,
        address: str,
        handler: Callable[..., Awaitable[None]],
        headers: Mapping[str, TypeAdapter[Any]],
        payload: Optional[Tuple[str, TypeAdapter[Any]]],
    ) -> None:
        self._address = address
        self._handler = handler
        self._headers = headers
        self._payload = payload

    @property
    def address(self) -> str:
        return self._address

    @property
    def name(self) -> str:
        return self._handler.__name__

    @property
    def headers(self) -> Mapping[str, TypeAdapter[Any]]:
        return self._headers

    @cached_property
    def headers_model(self) -> Optional[Type[BaseModel]]:
        if self._headers:
            headers_name = f"{_pascal_case(self.name)}Headers"
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

    async def __call__(
        self, scope: MessageScope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        arguments = dict(self._generate_arguments(scope))
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
        self, scope: MessageScope
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


def _generate_schemas(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        headers_model = channel.headers_model
        if headers_model:
            headers_name = f"{_pascal_case(channel.name)}Headers"
            yield headers_name, TypeAdapter(headers_model).json_schema()

        payload = channel.payload
        if payload:
            _, type_adapter = payload
            yield type_adapter._type.__name__, type_adapter.json_schema()


def _pascal_case(name: str) -> str:
    return "".join(part.title() for part in name.split("_"))


def _generate_messages(
    channels: Iterable[Channel],
    field_mapping: dict[tuple[int, JsonSchemaMode], JsonSchemaValue],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        pascal_case = _pascal_case(channel.name)
        message_name = f"{pascal_case}Message"
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

        yield message_name, message


def _generate_channels(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        message_name = f"{_pascal_case(channel.name)}Message"
        yield _pascal_case(channel.name), {
            "address": channel.address,
            "messages": {
                message_name: {"$ref": f"#/components/messages/{message_name}"}
            },
        }


def _generate_operations(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        operation_name = (
            f"receive{''.join(part.title() for part in channel.name.split('_'))}"
        )
        yield operation_name, {
            "action": "receive",
            "channel": {"$ref": f"#/channels/{_pascal_case(channel.name)}"},
        }


class Header(FieldInfo):
    pass


class Payload(FieldInfo):
    pass


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
