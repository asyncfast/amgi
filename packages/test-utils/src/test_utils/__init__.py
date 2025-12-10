import asyncio
import multiprocessing
from asyncio import Event
from asyncio import Queue
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from typing import Callable
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
                "amgi": {"spec_version": "1.0", "version": "1.0"},
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


def assert_run_can_terminate(
    run: Callable[..., None], *args: Any, **kwargs: Any
) -> None:
    lifespan_event = multiprocessing.Event()

    async def _app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            lifespan_event.set()
            raise Exception

    process = multiprocessing.Process(
        target=run,
        args=(_app, *args),
        kwargs=kwargs,
    )
    process.start()

    lifespan_event.wait()
    assert lifespan_event.is_set()

    process.terminate()
    process.join()
    assert not process.is_alive()
