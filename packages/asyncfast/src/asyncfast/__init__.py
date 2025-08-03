from functools import partial
from typing import Callable, Awaitable, Dict, Any

from types_acgi import Scope, ACGIReceiveCallable, ACGISendCallable, Message
from pydantic import BaseModel
from inspect import getfullargspec


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
        elif scope["type"] == 'messages':
            address = scope["address"]
            for channel in self._channels:
                if channel.name == address:
                    await channel(scope,receive,send)
                    break



class Channel:

    def __init__(self, name: str, handler: Callable[..., Awaitable[None]]) -> None:
        self.name = name
        self._handler = handler

    async def __call__(
        self, scope: Scope, receive: ACGIReceiveCallable, send: ACGISendCallable
    ) -> None:
        handler_argspec = getfullargspec(self._handler)



        for message in scope["messages"]:
            await self._handler(**dict(self._generate_arguments(message,handler_argspec.annotations)))

    def _generate_arguments(self, message:Message, annotations:Dict[str,Any]):
        for name, annotation in annotations.items():
            if issubclass(annotation, BaseModel):
                yield name, annotation.model_validate_json(message['payload'])
