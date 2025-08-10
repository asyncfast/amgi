import inspect
from functools import partial
from inspect import Signature
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
from typing import TypeVar

from pydantic import BaseModel
from pydantic import create_model
from pydantic import TypeAdapter
from types_acgi import ACGIReceiveCallable
from types_acgi import ACGISendCallable
from types_acgi import MessageScope
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
        self._channels.append(Channel(address, function))
        return function

    async def __call__(
        self, scope: Scope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        elif scope["type"] == "message":
            address = scope["address"]
            for channel in self._channels:
                if channel.address == address:
                    await channel(scope, receive, send)
                    break

    def asyncapi(self) -> Dict[str, Any]:
        return {
            "asyncapi": "3.0.0",
            "info": {
                "title": self.title,
                "version": self.version,
            },
            "channels": dict(_generate_channels(self._channels)),
            "operations": dict(_generate_operations(self._channels)),
            "components": {
                "messages": dict(_generate_messages(self._channels)),
                "schemas": dict(_generate_schemas(self._channels)),
            },
        }


class Channel:

    def __init__(self, address: str, handler: Callable[..., Awaitable[None]]) -> None:
        self._address = address
        self._handler = handler

    @property
    def address(self) -> str:
        return self._address

    @property
    def name(self) -> str:
        return self._handler.__name__

    async def __call__(
        self, scope: MessageScope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        signature = inspect.signature(self._handler)
        arguments = dict(_generate_arguments(scope, signature))
        if inspect.isasyncgenfunction(self._handler):
            async for message in self._handler(**arguments):
                await send(
                    {
                        "type": "message.send",
                        "address": message.address,
                        "headers": message.headers,
                        "payload": message.payload,
                    }
                )
        else:
            await self._handler(**arguments)


def _has_headers(channel: Channel) -> bool:
    signature = inspect.signature(channel._handler)

    for parameter in signature.parameters.values():
        annotation = parameter.annotation
        if get_origin(annotation) is Annotated:
            annotated_args = get_args(annotation)
            if isinstance(annotated_args[1], Header):
                return True
    return False


def _generate_schemas(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        has_headers = _has_headers(channel)
        if has_headers:
            headers_name = f"{_pascal_case(channel.name)}Headers"
            header_fields = dict(_generate_header_fields(channel))
            header_model = create_model(headers_name, **header_fields)
            yield headers_name, TypeAdapter(header_model).json_schema()

        payload = _get_payload(channel)
        if payload:
            yield payload.__name__, TypeAdapter(payload).json_schema()


def _pascal_case(name: str) -> str:
    return "".join(part.title() for part in name.split("_"))


def _generate_header_fields(channel: Channel) -> Generator[Any, None, None]:
    signature = inspect.signature(channel._handler)

    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if get_origin(annotation) is Annotated:
            annotated_args = get_args(annotation)
            if isinstance(annotated_args[1], Header):
                header_alias = annotated_args[1].alias
                alias = header_alias if header_alias else name.replace("_", "-")
                if parameter.default:
                    yield alias, (annotated_args[0], parameter.default)
                else:
                    yield alias, annotated_args[0]


def _get_payload(channel: Channel) -> Optional[Any]:
    signature = inspect.signature(channel._handler)

    for parameter in signature.parameters.values():
        annotation = parameter.annotation
        if issubclass(annotation, BaseModel):
            return annotation
    return None


def _generate_messages(
    channels: Iterable[Channel],
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    for channel in channels:
        pascal_case = _pascal_case(channel.name)
        message_name = f"{pascal_case}Message"
        message = {}

        has_headers = _has_headers(channel)
        if has_headers:
            message["headers"] = {"$ref": f"#/components/schemas/{pascal_case}Headers"}

        payload = _get_payload(channel)
        if payload:
            message["payload"] = {"$ref": f"#/components/schemas/{payload.__name__}"}

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


def _generate_arguments(
    scope: MessageScope, signature: Signature
) -> Generator[Tuple[str, Any], None, None]:
    headers = Headers(scope["headers"])
    for name, parameter in signature.parameters.items():
        annotation = parameter.annotation
        if issubclass(annotation, BaseModel):
            yield name, annotation.model_validate_json(scope["payload"])
        if get_origin(annotation) is Annotated:
            annotated_args = get_args(annotation)
            if isinstance(annotated_args[1], Header):
                header_alias = annotated_args[1].alias
                alias = header_alias if header_alias else name.replace("_", "-")
                header = headers.get(alias, parameter.default)
                value = TypeAdapter(annotated_args[0]).validate_python(
                    header, from_attributes=True
                )
                yield name, value


class Header:
    def __init__(self, alias: Optional[str] = None) -> None:
        self.alias = alias


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
