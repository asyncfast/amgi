from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import Any
from typing import ParamSpec
from typing import Protocol
from typing import TypeVar

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import LifespanShutdownCompleteEvent
from amgi_types import LifespanStartupCompleteEvent
from amgi_types import Scope
from asyncfast._asyncapi import get_asyncapi
from asyncfast._channel import Router

P = ParamSpec("P")
DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
Lifespan = Callable[["AsyncFast"], AbstractAsyncContextManager[None]]


class _MiddlewareFactory(Protocol[P]):
    def __call__(
        self, app: AMGIApplication, /, *args: P.args, **kwargs: P.kwargs
    ) -> AMGIApplication: ...  # pragma: no cover


class Middleware:
    def __init__(
        self, cls: _MiddlewareFactory[P], *args: P.args, **kwargs: P.kwargs
    ) -> None:
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def __iter__(self) -> Iterator[Any]:
        as_tuple = (self.cls, self.args, self.kwargs)
        return iter(as_tuple)


class AsyncFast:
    def __init__(
        self,
        title: str = "AsyncFast",
        version: str = "0.1.0",
        lifespan: Lifespan | None = None,
        middleware: Sequence[Middleware] | None = None,
    ) -> None:
        self._title = title
        self._version = version
        self._lifespan_context = lifespan
        self._middleware = list(middleware) if middleware else []
        self._router = Router()
        self._lifespan: AbstractAsyncContextManager[None] | None = None
        self._asyncapi_schema: dict[str, Any] | None = None
        self._middleware_stack: AMGIApplication | None = None

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
        if self._middleware_stack is None:
            self._middleware_stack = self.build_middleware_stack()

        await self._middleware_stack(scope, receive, send)

    async def _app(
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

    def build_middleware_stack(self) -> AMGIApplication:
        app = self._app
        for cls, args, kwargs in self._middleware:
            app = cls(app, *args, **kwargs)
        return app

    def add_middleware(
        self,
        middleware_class: _MiddlewareFactory[P],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._middleware.append(Middleware(middleware_class, *args, **kwargs))
