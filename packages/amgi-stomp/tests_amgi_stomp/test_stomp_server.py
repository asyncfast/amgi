import asyncio
import logging
from collections.abc import Callable
from collections.abc import Coroutine
from types import SimpleNamespace
from types import TracebackType
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
import stompman
from amgi_stomp import _MessageSendT
from amgi_stomp import Server
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from amgi_types import Scope
from stompman import AckableMessageFrame


class _StubMessageSendManager:
    async def __aenter__(self) -> _MessageSendT:
        async def message_send(event: MessageSendEvent) -> None:
            return None  # pragma: no cover

        return message_send

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return False


class _StubSubscription:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def unsubscribe(self) -> None:
        self._events.append("unsubscribe")


class _StubClient:
    def __init__(self, subscription: _StubSubscription, events: list[str]) -> None:
        self._subscription = subscription
        self._events = events
        self.handler: (
            Callable[[AckableMessageFrame], Coroutine[Any, Any, None]] | None
        ) = None
        self.exit_args: (
            tuple[type[BaseException] | None, BaseException | None, Any] | None
        ) = None
        self.subscribed = asyncio.Event()

    async def __aenter__(self) -> "_StubClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.exit_args = (exc_type, exc_val, exc_tb)

    async def subscribe_with_manual_ack(
        self,
        destination: str,
        handler: Callable[[AckableMessageFrame], Coroutine[Any, Any, None]],
        **kwargs: Any,
    ) -> _StubSubscription:
        self.handler = handler
        self.subscribed.set()
        return self._subscription


def _stub_frame(
    events: list[str] | None = None,
    ack: AsyncMock | None = None,
    nack: AsyncMock | None = None,
) -> AckableMessageFrame:
    async def _ack() -> None:
        if events is not None:
            events.append("ack")

    return cast(
        AckableMessageFrame,
        SimpleNamespace(
            headers={
                "destination": "destination",
                "message-id": "message-id-1",
                "subscription": "subscription-1",
            },
            body=b"value",
            ack=ack or AsyncMock(side_effect=_ack),
            nack=nack or AsyncMock(),
        ),
    )


async def _lifespan_app_call(
    scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
) -> None:
    assert await receive() == {"type": "lifespan.startup"}
    await send({"type": "lifespan.startup.complete"})
    assert await receive() == {"type": "lifespan.shutdown"}
    await send({"type": "lifespan.shutdown.complete"})


async def test_serve_acks_in_flight_message_before_unsubscribe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    message_acked = asyncio.Event()
    release_message = asyncio.Event()

    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            await _lifespan_app_call(scope, receive, send)
            return
        events.append("message-start")
        await send({"type": "message.ack"})
        events.append("message-acked")
        message_acked.set()
        await release_message.wait()
        events.append("message-end")

    stub_client = _StubClient(_StubSubscription(events), events)
    monkeypatch.setattr(stompman, "Client", lambda **kwargs: stub_client)

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
        message_send=_StubMessageSendManager(),
    )
    serve_task = asyncio.create_task(server.serve())
    await asyncio.wait_for(stub_client.subscribed.wait(), 5)

    assert stub_client.handler is not None
    await stub_client.handler(_stub_frame(events))
    await asyncio.wait_for(message_acked.wait(), 5)

    server.stop()
    # serve() must now be draining the in-flight handler, which is still blocked
    # on release_message, so the subscription cannot have been unsubscribed yet
    for _ in range(10):
        await asyncio.sleep(0)
    assert "unsubscribe" not in events

    release_message.set()
    await asyncio.wait_for(serve_task, 5)

    assert events.index("ack") < events.index("unsubscribe")
    assert events.index("message-end") < events.index("unsubscribe")
    assert stub_client.exit_args == (None, None, None)


async def test_process_message_nacks_when_application_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        raise RuntimeError("application error")

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
    )
    ack = AsyncMock()
    nack = AsyncMock()
    frame = _stub_frame(ack=ack, nack=nack)

    with caplog.at_level(logging.ERROR, logger="amgi-stomp.error"):
        await server._process_message(frame, AsyncMock(), {})

    nack.assert_awaited_once_with()
    ack.assert_not_awaited()
    assert "application error" in caplog.text


async def test_process_message_suppresses_nack_failure() -> None:
    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        raise RuntimeError("application error")

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
    )
    nack = AsyncMock(side_effect=RuntimeError("nack failed"))
    frame = _stub_frame(nack=nack)

    await server._process_message(frame, AsyncMock(), {})

    nack.assert_awaited_once_with()


async def test_process_message_propagates_cancellation() -> None:
    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        raise asyncio.CancelledError

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
    )
    ack = AsyncMock()
    nack = AsyncMock()
    frame = _stub_frame(ack=ack, nack=nack)

    with pytest.raises(asyncio.CancelledError):
        await server._process_message(frame, AsyncMock(), {})

    nack.assert_not_awaited()
    ack.assert_not_awaited()


async def test_process_message_scope_bindings() -> None:
    scopes: list[MessageScope] = []

    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        scopes.append(cast(MessageScope, scope))

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
    )
    frame = cast(
        AckableMessageFrame,
        SimpleNamespace(
            headers={
                "destination": "destination",
                "message-id": "message-id-1",
                "subscription": "subscription-1",
                "custom": "value",
            },
            body=b"value",
            ack=AsyncMock(),
            nack=AsyncMock(),
        ),
    )

    await server._process_message(frame, AsyncMock(), {})

    assert scopes[0]["bindings"] == {
        "stomp": {"message_id": "message-id-1", "subscription": "subscription-1"}
    }
    assert dict(scopes[0]["headers"]) == {b"custom": b"value"}


async def test_serve_ignores_messages_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    handled: list[str] = []

    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            await _lifespan_app_call(scope, receive, send)
            return
        handled.append("message")  # pragma: no cover

    stub_client = _StubClient(_StubSubscription(events), events)
    monkeypatch.setattr(stompman, "Client", lambda **kwargs: stub_client)

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
        message_send=_StubMessageSendManager(),
    )
    serve_task = asyncio.create_task(server.serve())
    await asyncio.wait_for(stub_client.subscribed.wait(), 5)

    server.stop()
    for _ in range(10):
        await asyncio.sleep(0)
    assert stub_client.handler is not None
    ack = AsyncMock()
    await stub_client.handler(_stub_frame(ack=ack))
    await asyncio.wait_for(serve_task, 5)

    assert handled == []
    ack.assert_not_awaited()


async def test_serve_cancels_handlers_after_shutdown_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import amgi_stomp

    monkeypatch.setattr(amgi_stomp, "_SHUTDOWN_TIMEOUT", 0.05)
    events: list[str] = []
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def app(
        scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            await _lifespan_app_call(scope, receive, send)
            return
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    stub_client = _StubClient(_StubSubscription(events), events)
    monkeypatch.setattr(stompman, "Client", lambda **kwargs: stub_client)

    server = Server(
        app,
        "destination",
        host="localhost",
        port=61613,
        login="guest",
        passcode="guest",
        message_send=_StubMessageSendManager(),
    )
    serve_task = asyncio.create_task(server.serve())
    await asyncio.wait_for(stub_client.subscribed.wait(), 5)
    assert stub_client.handler is not None
    await stub_client.handler(_stub_frame(events))
    await asyncio.wait_for(started.wait(), 5)

    server.stop()
    await asyncio.wait_for(serve_task, 5)

    assert cancelled.is_set()
    assert "unsubscribe" in events
