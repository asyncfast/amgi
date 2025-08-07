import inspect
from functools import partial
from inspect import Signature
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Tuple
from typing import TypeVar

from pydantic import BaseModel
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
    def __init__(self) -> None:
        self._channels: List[Channel] = []

    def channel(self, name: str) -> Callable[[DecoratedCallable], DecoratedCallable]:
        return partial(self._add_channel, name)

    def _add_channel(self, name: str, function: DecoratedCallable) -> DecoratedCallable:
        self._channels.append(Channel(name, function))
        return function

    async def __call__(
        self, scope: Scope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send(
                        {
                            "type": "lifespan.startup.complete",
                            "subscriptions": [
                                {"address": channel.name} for channel in self._channels
                            ],
                        }
                    )
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        elif scope["type"] == "message":
            address = scope["address"]
            for channel in self._channels:
                if channel.name == address:
                    await channel(scope, receive, send)
                    break


class Channel:

    def __init__(self, name: str, handler: Callable[..., Awaitable[None]]) -> None:
        self.name = name
        self._handler = handler

    async def __call__(
        self, scope: MessageScope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        signature = inspect.signature(self._handler)

        await self._handler(**dict(_generate_arguments(scope, signature)))


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
                alias = name.replace("_", "-")
                header = headers.get(alias, parameter.default)
                value = TypeAdapter(annotated_args[0]).validate_python(
                    header, from_attributes=True
                )
                yield name, value


class Header:
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
