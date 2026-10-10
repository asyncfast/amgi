import asyncio
import logging
import sys
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import suppress
from functools import partial
from ssl import SSLContext
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import cast
from typing import Literal

if sys.version_info < (3, 11):  # pragma: no cover
    raise RuntimeError("amgi-stomp requires Python 3.11 or greater")

from typing import Self

import stompman
from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]
_SendT = Callable[[AMGISendEvent], Awaitable[None]]

_FRAME_METADATA_HEADERS = frozenset(
    {"destination", "message-id", "subscription", "ack", "content-length"}
)
_SHUTDOWN_TIMEOUT = 10

logger = logging.getLogger("amgi-stomp.error")


def run(
    app: AMGIApplication,
    *destinations: str,
    host: str = "localhost",
    port: int = 61613,
    login: str = "guest",
    passcode: str = "guest",
    ssl: Literal[True] | SSLContext | None = None,
    heartbeat: tuple[int, int] = (1000, 1000),
    connect_retry_attempts: int = 3,
    connect_retry_interval: int = 1,
    receipt_timeout: float | None = None,
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(
        app,
        *destinations,
        host=host,
        port=port,
        login=login,
        passcode=passcode,
        ssl=ssl,
        heartbeat=heartbeat,
        connect_retry_attempts=connect_retry_attempts,
        connect_retry_interval=connect_retry_interval,
        receipt_timeout=receipt_timeout,
        message_send=message_send,
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    destinations: list[str],
    host: str = "localhost",
    port: int = 61613,
    login: str = "guest",
    passcode: str = "guest",
) -> None:
    run(app, *destinations, host=host, port=port, login=login, passcode=passcode)


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


def _encode_headers(headers: dict[str, str] | None) -> list[tuple[bytes, bytes]]:
    if not headers:
        return []
    return [(key.encode(), value.encode()) for key, value in headers.items()]


def _decode_headers(headers: Sequence[tuple[bytes, bytes]]) -> dict[str, str]:
    return {key.decode(): value.decode() for key, value in headers}


def _create_client(
    connection_parameters: stompman.ConnectionParameters,
    *,
    ssl: Literal[True] | SSLContext | None,
    heartbeat: tuple[int, int],
    connect_retry_attempts: int,
    connect_retry_interval: int,
) -> stompman.Client:
    return stompman.Client(
        servers=[connection_parameters],
        ssl=ssl,
        heartbeat=stompman.Heartbeat(*heartbeat),
        connect_retry_attempts=connect_retry_attempts,
        connect_retry_interval=connect_retry_interval,
    )


class _Send:
    def __init__(
        self,
        frame: stompman.AckableMessageFrame,
        message_send: _MessageSendT,
    ) -> None:
        self._frame = frame
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._frame.ack()
        elif event["type"] == "message.nack":
            await self._frame.nack()
        elif event["type"] == "message.send":
            await self._message_send(event)


class MessageSend:
    def __init__(
        self,
        host: str,
        port: int,
        login: str,
        passcode: str,
        ssl: Literal[True] | SSLContext | None = None,
        heartbeat: tuple[int, int] = (1000, 1000),
        connect_retry_attempts: int = 3,
        connect_retry_interval: int = 1,
        receipt_timeout: float | None = None,
    ) -> None:
        self._client: stompman.Client | None = None
        self._connection_parameters = stompman.ConnectionParameters(
            host=host,
            port=port,
            login=login,
            passcode=passcode,
        )
        self._ssl = ssl
        self._heartbeat = heartbeat
        self._connect_retry_attempts = connect_retry_attempts
        self._connect_retry_interval = connect_retry_interval
        self._receipt_timeout = receipt_timeout

    async def __aenter__(self) -> Self:
        self._client = _create_client(
            self._connection_parameters,
            ssl=self._ssl,
            heartbeat=self._heartbeat,
            connect_retry_attempts=self._connect_retry_attempts,
            connect_retry_interval=self._connect_retry_interval,
        )
        await self._client.__aenter__()
        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        if self._client is None:
            raise RuntimeError("MessageSend not initialized")

        await self._client.send(
            event.get("payload") or b"",
            event["address"],
            headers=_decode_headers(event["headers"]),
            receipt_timeout=self._receipt_timeout,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            await self._client.__aexit__(exc_type, exc_val, exc_tb)


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        *destinations: str,
        host: str,
        port: int,
        login: str,
        passcode: str,
        ssl: Literal[True] | SSLContext | None = None,
        heartbeat: tuple[int, int] = (1000, 1000),
        connect_retry_attempts: int = 3,
        connect_retry_interval: int = 1,
        receipt_timeout: float | None = None,
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._destinations = destinations
        self._connection_parameters = stompman.ConnectionParameters(
            host=host,
            port=port,
            login=login,
            passcode=passcode,
        )
        self._ssl = ssl
        self._heartbeat = heartbeat
        self._connect_retry_attempts = connect_retry_attempts
        self._connect_retry_interval = connect_retry_interval
        self._message_send = message_send or MessageSend(
            host,
            port,
            login,
            passcode,
            ssl=ssl,
            heartbeat=heartbeat,
            connect_retry_attempts=connect_retry_attempts,
            connect_retry_interval=connect_retry_interval,
            receipt_timeout=receipt_timeout,
        )
        self._stopped = asyncio.Event()
        self._tasks = set[asyncio.Task[None]]()
        self._accepting = True

    async def serve(self) -> None:
        client = _create_client(
            self._connection_parameters,
            ssl=self._ssl,
            heartbeat=self._heartbeat,
            connect_retry_attempts=self._connect_retry_attempts,
            connect_retry_interval=self._connect_retry_interval,
        )
        await client.__aenter__()
        try:
            await self._serve(client)
        except BaseException as exc:
            # A connection whose socket has stalled must not block shutdown, so
            # closing it is bounded and best-effort. The in-flight exception is
            # passed through so stompman does not wait for handlers to drain
            # when serve() is already unwinding with an error
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    client.__aexit__(type(exc), exc, exc.__traceback__),
                    _SHUTDOWN_TIMEOUT,
                )
            raise
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                client.__aexit__(None, None, None), _SHUTDOWN_TIMEOUT
            )

    async def _serve(self, client: stompman.Client) -> None:
        message_send = await self._message_send.__aenter__()
        try:
            async with Lifespan(self._app) as state:
                subscriptions = [
                    await client.subscribe_with_manual_ack(
                        destination,
                        partial(
                            self._handle_message,
                            message_send=message_send,
                            state=state,
                        ),
                    )
                    for destination in self._destinations
                ]
                await self._stopped.wait()
                self._accepting = False
                await self._drain_tasks()
                for subscription in subscriptions:
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            subscription.unsubscribe(), _SHUTDOWN_TIMEOUT
                        )
        except BaseException as exc:
            if not await self._exit_message_send(exc):
                raise
        else:
            await self._exit_message_send(None)

    async def _drain_tasks(self) -> None:
        """Wait for in-flight message handlers to complete before unsubscribing.

        stompman refuses to acknowledge messages on an unsubscribed subscription,
        so unsubscribing first would leave messages that were processed, but never
        acknowledged, to be redelivered after a restart. Handlers are drained
        while the subscription is still active. New callbacks are no longer
        accepted by this point, so the set of tasks is stable. Tasks still
        running after the shutdown timeout are cancelled, leaving their
        messages unacknowledged so the broker may redeliver them.
        """
        if not self._tasks:
            return
        _, pending = await asyncio.wait(list(self._tasks), timeout=_SHUTDOWN_TIMEOUT)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _exit_message_send(self, exc: BaseException | None) -> bool:
        """Exit the message send manager, bounded so a stalled socket cannot
        block shutdown. Returns whether the manager suppressed the exception."""
        try:
            if exc is None:
                return bool(
                    await asyncio.wait_for(
                        self._message_send.__aexit__(None, None, None),
                        _SHUTDOWN_TIMEOUT,
                    )
                )
            return bool(
                await asyncio.wait_for(
                    self._message_send.__aexit__(type(exc), exc, exc.__traceback__),
                    _SHUTDOWN_TIMEOUT,
                )
            )
        except asyncio.TimeoutError:
            return False

    async def _handle_message(
        self,
        frame: stompman.AckableMessageFrame,
        *,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        if not self._accepting:
            # Left unacknowledged, so the broker may redeliver it
            return
        task = asyncio.get_running_loop().create_task(
            self._process_message(frame, message_send, state)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _process_message(
        self,
        frame: stompman.AckableMessageFrame,
        message_send: _MessageSendT,
        state: dict[str, Any],
    ) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": frame.headers["destination"],
            "headers": _encode_headers(
                {
                    key: value
                    for key, value in cast("dict[str, str]", frame.headers).items()
                    if key not in _FRAME_METADATA_HEADERS
                }
            ),
            "payload": frame.body,
            "bindings": {
                "stomp": {
                    "message_id": frame.headers["message-id"],
                    "subscription": frame.headers["subscription"],
                }
            },
            "state": state.copy(),
        }
        try:
            await self._app(scope, _receive, _Send(frame, message_send))
        except Exception:
            logger.exception("Application raised an exception")
            # Whether the message is redelivered depends on the broker's
            # policy. The subscription may already be inactive during
            # shutdown, in which case stompman only warns
            with suppress(Exception):
                await frame.nack()

    def stop(self) -> None:
        self._stopped.set()
