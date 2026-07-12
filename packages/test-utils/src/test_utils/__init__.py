import asyncio
import importlib
import multiprocessing.synchronize
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Queue
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import partial
from threading import Thread
from typing import Any
from typing import Callable
from typing import cast
from typing import Optional
from typing import Protocol

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import Scope


class _Server(Protocol):
    async def serve(self) -> None:
        pass  # pragma: no cover

    def stop(self) -> None:
        pass  # pragma: no cover


class MockApp:
    def __init__(self) -> None:
        self._call_queue = Queue[
            tuple[Scope, AMGIReceiveCallable, AMGISendCallable, Event]
        ]()

    @asynccontextmanager
    async def call(
        self,
    ) -> AsyncGenerator[tuple[Scope, AMGIReceiveCallable, AMGISendCallable], None]:
        scope, receive, send, return_event = await self._call_queue.get()
        try:
            yield scope, receive, send
        finally:
            return_event.set()

    @asynccontextmanager
    async def lifespan(
        self, state: Optional[dict[str, Any]] = None, server: Optional[_Server] = None
    ) -> AsyncGenerator[None, None]:
        loop = asyncio.get_running_loop()
        serve_task = loop.create_task(server.serve()) if server else None

        async with self.call() as (scope, receive, send):
            assert scope == {
                "amgi": {"version": "2.0", "spec_version": "2.0"},
                "type": "lifespan",
                "state": {},
            }
            if state is not None:
                scope["state"].update(state)
            lifespan_startup = await receive()
            assert lifespan_startup == {"type": "lifespan.startup"}
            await send({"type": "lifespan.startup.complete"})
            yield
            if server:
                server.stop()

            lifespan_shutdown = await receive()
            assert lifespan_shutdown == {"type": "lifespan.shutdown"}
            await send({"type": "lifespan.shutdown.complete"})
        if serve_task:
            await serve_task

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        return_event = Event()
        self._call_queue.put_nowait((scope, receive, send, return_event))
        await return_event.wait()


async def _app(
    scope: Scope,
    receive: AMGIReceiveCallable,
    send: AMGISendCallable,
    lifespan_event: multiprocessing.synchronize.Event,
) -> None:
    if scope["type"] == "lifespan":
        lifespan_event.set()
        raise Exception


def _stop_server(
    server: _Server,
    lifespan_event: multiprocessing.synchronize.Event,
    loop: AbstractEventLoop,
) -> None:
    lifespan_event.wait()
    loop.call_soon_threadsafe(server.stop)


def _run_until_lifespan_then_stop(
    run: Callable[..., None],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    lifespan_event: multiprocessing.synchronize.Event,
) -> None:
    module = importlib.import_module(run.__module__)
    original_server_serve = cast(
        Callable[[_Server], None], getattr(module, "server_serve")
    )

    def server_serve(server: _Server) -> None:
        original_serve = server.serve

        async def serve() -> None:
            loop = asyncio.get_running_loop()
            Thread(
                target=_stop_server,
                args=(server, lifespan_event, loop),
                daemon=True,
            ).start()
            await original_serve()

        setattr(server, "serve", serve)
        try:
            original_server_serve(server)
        finally:
            setattr(server, "serve", original_serve)

    setattr(module, "server_serve", server_serve)
    try:
        run(partial(_app, lifespan_event=lifespan_event), *args, **kwargs)
    finally:
        setattr(module, "server_serve", original_server_serve)


def assert_run_can_terminate(
    run: Callable[..., None], *args: Any, **kwargs: Any
) -> None:
    context = multiprocessing.get_context("spawn")
    lifespan_event = context.Event()

    process = context.Process(
        target=_run_until_lifespan_then_stop,
        args=(run, args, kwargs, lifespan_event),
    )
    process.start()

    try:
        assert lifespan_event.wait(5)
        process.join(5)
    finally:
        if process.is_alive():  # pragma: no cover
            process.kill()
            process.join()

    assert not process.is_alive()
    assert process.exitcode == 0
