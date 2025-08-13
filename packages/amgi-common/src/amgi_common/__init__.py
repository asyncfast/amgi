import asyncio
from asyncio import Event
from asyncio import Queue
from types import TracebackType
from typing import Optional
from typing import Union

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import LifespanScope
from amgi_types import LifespanShutdownEvent
from amgi_types import LifespanStartupEvent


class Lifespan:
    def __init__(self, app: AMGIApplication) -> None:
        self._app = app
        self._receive_queue = Queue[
            Union[LifespanStartupEvent, LifespanShutdownEvent]
        ]()

        self._startup_event = Event()
        self._shutdown_event = Event()

    async def __aenter__(self) -> None:
        loop = asyncio.get_running_loop()
        self.main_task = loop.create_task(self._main())

        startup_event: LifespanStartupEvent = {
            "type": "lifespan.startup",
        }
        await self._receive_queue.put(startup_event)
        await self._startup_event.wait()

    async def _main(self) -> None:
        scope: LifespanScope = {
            "type": "lifespan",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
        }
        await self._app(
            scope,
            self.receive,
            self.send,
        )

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        shutdown_event: LifespanShutdownEvent = {
            "type": "lifespan.shutdown",
        }
        await self._receive_queue.put(shutdown_event)
        await self._shutdown_event.wait()

    async def receive(self) -> AMGIReceiveEvent:
        return await self._receive_queue.get()

    async def send(self, event: AMGISendEvent) -> None:
        event_type = event["type"]

        if event_type in {"lifespan.startup.complete", "lifespan.startup.failed"}:
            self._startup_event.set()
        elif event_type in {"lifespan.shutdown.complete", "lifespan.shutdown.failed"}:
            self._shutdown_event.set()
