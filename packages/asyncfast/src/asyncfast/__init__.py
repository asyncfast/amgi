from functools import partial
from inspect import getfullargspec
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict

from pydantic import BaseModel
from types_acgi import ACGIReceiveCallable
from types_acgi import ACGISendCallable
from types_acgi import Scope


class AsyncFast:
    def __init__(self) -> None:
        self._channels = []

    def channel(self, name):
        return partial(self._add_channel, name)

    def _add_channel(self, name, function: Callable[..., Awaitable[None]]) -> None:
        self._channels.append(Channel(name, function))

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
        self, scope: Scope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        handler_argspec = getfullargspec(self._handler)

        await self._handler(
            **dict(self._generate_arguments(scope, handler_argspec.annotations))
        )

    def _generate_arguments(self, scope: Scope, annotations: Dict[str, Any]):
        for name, annotation in annotations.items():
            if issubclass(annotation, BaseModel):
                yield name, annotation.model_validate_json(scope["payload"])
