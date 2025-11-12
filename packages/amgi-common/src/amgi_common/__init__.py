from __future__ import annotations

import asyncio
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Future
from asyncio import Queue
from collections.abc import Coroutine
from collections.abc import Iterable
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar
from typing import Union

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import LifespanScope
from amgi_types import LifespanShutdownEvent
from amgi_types import LifespanStartupEvent
from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")
R = TypeVar("R")


class Lifespan:
    def __init__(self, app: AMGIApplication) -> None:
        self._app = app
        self._receive_queue = Queue[
            Union[LifespanStartupEvent, LifespanShutdownEvent]
        ]()

        self._startup_event = Event()
        self._shutdown_event = Event()
        self._state: dict[str, Any] = {}

    async def __aenter__(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        self.main_task = loop.create_task(self._main())

        startup_event: LifespanStartupEvent = {
            "type": "lifespan.startup",
        }
        await self._receive_queue.put(startup_event)
        await self._startup_event.wait()
        return self._state

    async def _main(self) -> None:
        scope: LifespanScope = {
            "type": "lifespan",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "state": self._state,
        }
        await self._app(
            scope,
            self.receive,
            self.send,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
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


class Stoppable:
    def __init__(self, stop_event: Event | None = None) -> None:
        self._stop_event = Event() if stop_event is None else stop_event

    def call(
        self,
        function: Callable[P, Coroutine[Any, Any, T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> _StoppableAsyncIterator[T]:
        return _StoppableAsyncIterator(function, args, kwargs, self._stop_event)

    def stop(self) -> None:
        self._stop_event.set()


class _StoppableAsyncIterator(Generic[T]):
    def __init__(
        self,
        function: Callable[..., Coroutine[Any, Any, T]],
        args: Any,
        kwargs: dict[str, Any],
        stop_event: Event,
    ) -> None:
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._stop_event = stop_event

    def __aiter__(self) -> _StoppableAsyncIterator[T]:
        self._loop = asyncio.get_running_loop()
        self._stop_event_task = self._loop.create_task(self._stop_event.wait())
        return self

    async def __anext__(self) -> T:
        callable_task = self._loop.create_task(
            self._function(*self._args, **self._kwargs)
        )
        await asyncio.wait(
            (
                callable_task,
                self._stop_event_task,
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._stop_event.is_set():
            callable_task.cancel()
            raise StopAsyncIteration
        return await callable_task


class OperationBatcher(Generic[T, R]):
    def __init__(
        self,
        function: Callable[[Iterable[T]], Coroutine[Any, Any, Iterable[R | Exception]]],
        batch_size: int | None = None,
    ) -> None:
        self._function = function
        self._batch_size = batch_size
        self._queue = Queue[tuple[T, Future[R]]]()

    async def _process_batch_async(self, batch: Iterable[tuple[T, Future[R]]]) -> None:
        batch_items, batch_futures = zip(*batch)
        for result, future in zip(await self._function(batch_items), batch_futures):
            if isinstance(result, Exception):
                future.set_exception(result)
            else:
                future.set_result(result)

    def _process_batch(self, loop: AbstractEventLoop) -> None:
        batch: list[tuple[T, Future[R]]] = []
        while not self._queue.empty() and (
            self._batch_size is None or len(batch) < self._batch_size
        ):
            batch.append(self._queue.get_nowait())
        if batch:
            loop.create_task(self._process_batch_async(batch))

    async def enqueue(self, item: T) -> R:
        loop = asyncio.get_running_loop()
        future: Future[R] = loop.create_future()
        self._queue.put_nowait((item, future))
        loop.call_soon(self._process_batch, loop)
        return await future
