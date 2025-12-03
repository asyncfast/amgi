from __future__ import annotations

import asyncio
import logging
import sys
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Future
from asyncio import Queue
from asyncio import Task
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Sequence
from functools import partial
from itertools import islice
from signal import SIGINT
from signal import SIGTERM
from types import TracebackType
from typing import Any
from typing import Generic
from typing import Protocol
from typing import TypeVar
from typing import Union

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import LifespanScope
from amgi_types import LifespanShutdownEvent
from amgi_types import LifespanStartupEvent

if sys.version_info >= (3, 13):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")
R = TypeVar("R")
H = TypeVar("H", bound=Hashable)


_logger = logging.getLogger("amgi-common.error")


class LifespanFailureError(Exception):
    """Error thrown during startup, or shutdown"""


class Lifespan:
    def __init__(
        self, app: AMGIApplication, state: dict[str, Any] | None = None
    ) -> None:
        self._app = app
        self._receive_queue = Queue[
            Union[LifespanStartupEvent, LifespanShutdownEvent]
        ]()

        self._startup_event = Event()
        self._startup_error_message: str | None = None
        self._shutdown_event = Event()
        self._shutdown_error_message: str | None = None
        self._state = {} if state is None else state
        self._error_occurred = False

    async def __aenter__(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        self.main_task = loop.create_task(self._main())

        startup_event: LifespanStartupEvent = {
            "type": "lifespan.startup",
        }
        await self._receive_queue.put(startup_event)
        await self._startup_event.wait()
        if self._startup_error_message:
            raise LifespanFailureError(self._startup_error_message)
        return self._state

    async def _main(self) -> None:
        scope: LifespanScope = {
            "type": "lifespan",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "state": self._state,
        }
        try:
            await self._app(
                scope,
                self.receive,
                self.send,
            )
        except Exception:
            self._error_occurred = True
            _logger.info("AMGI 'lifespan' protocol appears unsupported.")
            self._startup_event.set()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._error_occurred:
            return
        shutdown_event: LifespanShutdownEvent = {
            "type": "lifespan.shutdown",
        }
        await self._receive_queue.put(shutdown_event)
        await self._shutdown_event.wait()
        if self._shutdown_error_message:
            raise LifespanFailureError(self._shutdown_error_message)

    async def receive(self) -> AMGIReceiveEvent:
        return await self._receive_queue.get()

    async def send(self, event: AMGISendEvent) -> None:
        if event["type"] == "lifespan.startup.complete":
            self._startup_event.set()
        elif event["type"] == "lifespan.startup.failed":
            self._startup_event.set()
            self._startup_error_message = event["message"]
        elif event["type"] == "lifespan.shutdown.complete":
            self._shutdown_event.set()
        elif event["type"] == "lifespan.shutdown.failed":
            self._shutdown_event.set()
            self._shutdown_error_message = event["message"]


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
        if self._stop_event.is_set():
            raise StopAsyncIteration
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


class OperationBatcherError(Exception):
    """Error raised during operation batch error"""


class OperationBatcher(Generic[T, H, R]):
    def __init__(
        self,
        function: Callable[[Iterable[T]], Coroutine[Any, Any, Sequence[R | Exception]]],
        key: Callable[[T], H],
        batch_size: int | None = None,
    ) -> None:
        self._function = function
        self._key = key
        self._batch_size = batch_size
        self._queue = Queue[tuple[T, Future[R]]]()

    async def _process_batch_async(self, batch: Iterable[tuple[T, Future[R]]]) -> None:
        batch_items, batch_futures = zip(*batch)

        batch_result = await self._function_invoke(batch_items)
        if isinstance(batch_result, Exception):
            for future in batch_futures:
                future.set_exception(batch_result)
        else:
            for result, future in zip(batch_result, batch_futures):
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)

    async def _function_invoke(
        self, batch_items: Sequence[T]
    ) -> Exception | Sequence[R | Exception]:
        try:
            batch_result = await self._function(batch_items)
        except Exception as e:
            return e
        if len(batch_result) != len(batch_items):
            return OperationBatcherError(
                "Batch function did not return the correct number of results"
            )
        return batch_result

    def _process_batch(self, loop: AbstractEventLoop) -> None:
        groups = defaultdict(list)
        while not self._queue.empty():
            item, future = self._queue.get_nowait()
            groups[self._key(item)].append((item, future))
        if groups:
            for group in groups.values():
                for batch in self._get_batch(group):
                    loop.create_task(self._process_batch_async(batch))

    def _get_batch(
        self, group: Iterable[tuple[T, Future[R]]]
    ) -> Iterable[Iterable[tuple[T, Future[R]]]]:
        if self._batch_size is None:
            yield group
        else:
            iterator = iter(group)
            while batch := tuple(islice(iterator, self._batch_size)):
                yield batch

    async def enqueue(self, item: T) -> R:
        loop = asyncio.get_running_loop()
        future: Future[R] = loop.create_future()
        self._queue.put_nowait((item, future))
        loop.call_soon(self._process_batch, loop)
        return await future


class OperationCacher(Generic[H, R]):
    def __init__(self, function: Callable[[H], Coroutine[Any, Any, R]]) -> None:
        self._function = function
        self._cache_tasks: dict[H, Task[R]] = {}

    async def get(self, key: H) -> R:
        task = self._cache_tasks.get(key)
        if task is None:
            task = asyncio.create_task(self._function(key))
            task.add_done_callback(partial(self._remove_on_exception, key))
            self._cache_tasks[key] = task
        return await task

    def _remove_on_exception(self, key: H, task: Task[R]) -> None:
        if task.exception() is not None:
            del self._cache_tasks[key]


class _Server(Protocol):
    async def serve(self) -> None: ...

    def stop(self) -> None: ...


def server_serve(server: _Server) -> None:
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_server_serve_async(server, loop))


async def _server_serve_async(server: _Server, loop: AbstractEventLoop) -> None:
    for signal in (SIGINT, SIGTERM):
        loop.add_signal_handler(signal, server.stop)

    await server.serve()
