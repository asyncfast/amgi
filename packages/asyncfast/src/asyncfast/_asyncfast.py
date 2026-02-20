from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import Any
from typing import TypeVar

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import Scope
from asyncfast._asyncapi import get_asyncapi
from asyncfast._channel import Router

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
Lifespan = Callable[["AsyncFast"], AbstractAsyncContextManager[None]]


class AsyncFast:
    def __init__(
        self,
        title: str = "AsyncFast",
        version: str = "0.1.0",
        lifespan: Lifespan | None = None,
    ) -> None:
        self._title = title
        self._version = version
        self._lifespan_context = lifespan
        self._router = Router()
        self._lifespan: AbstractAsyncContextManager[None] | None = None
        self._asyncapi_schema: dict[str, Any] | None = None

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
        self._router.add_channel(address, function)
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
            await self._router(scope, receive, send)

    def asyncapi(self) -> dict[str, Any]:
        if not self._asyncapi_schema:

            self._asyncapi_schema = get_asyncapi(
                title=self.title,
                version=self.version,
                router=self._router,
            )

        return self._asyncapi_schema
