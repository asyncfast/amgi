from asyncio import Event
from asyncio import Queue
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from typing import Optional

from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import Scope


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
        self, state: Optional[dict[str, Any]] = None
    ) -> AsyncGenerator[None, None]:
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
            lifespan_shutdown = await receive()
            assert lifespan_shutdown == {"type": "lifespan.shutdown"}
            await send({"type": "lifespan.shutdown.complete"})

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        return_event = Event()
        self._call_queue.put_nowait((scope, receive, send, return_event))
        await return_event.wait()
