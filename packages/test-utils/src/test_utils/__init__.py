from asyncio import Event
from asyncio import Queue
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

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

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        return_event = Event()
        self._call_queue.put_nowait((scope, receive, send, return_event))
        await return_event.wait()
