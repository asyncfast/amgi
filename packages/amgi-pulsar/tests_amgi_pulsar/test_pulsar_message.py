import asyncio
import logging
from collections.abc import Awaitable
from collections.abc import Callable

import pulsar.asyncio
import pytest
from amgi_pulsar import _decode_headers
from amgi_pulsar import _encode_headers
from amgi_pulsar import MessageSend
from amgi_pulsar import Server
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageSendEvent
from amgi_types import Scope


class _FakeClient:
    def __init__(self) -> None:
        self.created_topics: list[str] = []

    async def create_producer(self, topic: str) -> object:
        self.created_topics.append(topic)
        await asyncio.sleep(0)
        return object()


class _FakeMessage:
    def data(self) -> bytes:
        return b""

    def properties(self) -> dict[str, str]:
        return {}

    def partition_key(self) -> str:
        return ""


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage]) -> None:
        self._messages = list(messages)

    async def receive(self) -> _FakeMessage:
        if self._messages:
            message = self._messages.pop(0)
            await asyncio.sleep(0)
            return message
        await asyncio.Event().wait()
        raise AssertionError  # pragma: no cover


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend("pulsar://localhost:6650")

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send({"type": "message.send", "address": "topic", "headers": []})


async def test_message_send_creates_one_producer_per_topic() -> None:
    message_send = MessageSend("pulsar://localhost:6650")
    fake_client = _FakeClient()
    message_send._client = fake_client

    first, second = await asyncio.gather(
        message_send._get_producer("topic"),
        message_send._get_producer("topic"),
    )
    third = await message_send._get_producer("topic")

    assert first is second is third
    assert fake_client.created_topics == ["topic"]


def test_encode_headers_handles_empty_and_values() -> None:
    assert _encode_headers(None) == []
    assert _encode_headers({}) == []
    assert _encode_headers({"name": "value"}) == [(b"name", b"value")]


def test_decode_headers_handles_empty_and_values() -> None:
    assert _decode_headers([]) == {}
    assert _decode_headers([(b"name", b"value")]) == {"name": "value"}


async def test_message_handler_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    handled_count = 0
    second_message_handled = asyncio.Event()

    async def app(
        scope: Scope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
    ) -> None:
        nonlocal handled_count
        handled_count += 1
        if handled_count == 2:
            second_message_handled.set()
        raise RuntimeError("Handler failed")

    async def message_send(event: MessageSendEvent) -> None:
        raise AssertionError  # pragma: no cover

    server = Server(
        app, "topic", service_url="pulsar://localhost:6650", subscription_name="amgi"
    )
    consumer = _FakeConsumer([_FakeMessage(), _FakeMessage()])

    with caplog.at_level(logging.ERROR):
        topic_loop = asyncio.create_task(
            server._topic_loop(consumer, "topic", message_send, {})
        )
        await asyncio.wait_for(second_message_handled.wait(), timeout=5)
        server.stop()
        await topic_loop

    assert handled_count == 2
    records = [
        record for record in caplog.records if record.name == "amgi-pulsar.error"
    ]
    assert len(records) == 2
    for record in records:
        assert record.levelno == logging.ERROR
        assert record.exc_info is not None
        assert isinstance(record.exc_info[1], RuntimeError)


class _FailingConsumer:
    def __init__(self, handler_started: asyncio.Event | None = None) -> None:
        self._handler_started = handler_started
        self._delivered = False

    async def receive(self) -> _FakeMessage:
        if self._handler_started is not None and not self._delivered:
            self._delivered = True
            return _FakeMessage()
        if self._handler_started is not None:
            await self._handler_started.wait()
        raise RuntimeError("Receive failed")


class _FakeServeClient:
    def __init__(self, consumers: list[object]) -> None:
        self._consumers = consumers
        self.closed = False

    async def subscribe(
        self, topic: str, subscription_name: str, **kwargs: object
    ) -> object:
        return self._consumers.pop(0)

    async def close(self) -> None:
        self.closed = True


class _FakeMessageSend:
    async def __aenter__(self) -> Callable[[MessageSendEvent], Awaitable[None]]:
        async def send(event: MessageSendEvent) -> None:
            raise AssertionError  # pragma: no cover

        return send

    async def __aexit__(self, *args: object) -> None:
        return None


async def test_serve_cancels_topic_loops_when_one_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeServeClient([_FailingConsumer(), _FakeConsumer([])])
    monkeypatch.setattr(pulsar.asyncio, "Client", lambda url: client)

    async def app(
        scope: Scope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
    ) -> None:
        raise RuntimeError("Lifespan unsupported")

    server = Server(
        app,
        "first",
        "second",
        service_url="pulsar://localhost:6650",
        subscription_name="amgi",
        message_send=_FakeMessageSend(),
    )

    with pytest.raises(RuntimeError, match="Receive failed"):
        await server.serve()

    assert client.closed


async def test_cancelled_message_task_is_not_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def app(
        scope: Scope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
    ) -> None:
        raise AssertionError  # pragma: no cover

    server = Server(
        app,
        "topic",
        service_url="pulsar://localhost:6650",
        subscription_name="amgi",
    )

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(wait_forever())
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    with caplog.at_level(logging.ERROR):
        server._task_done(task)

    assert not caplog.records


async def test_serve_cancels_running_handlers_when_topic_loop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()
    client = _FakeServeClient([_FailingConsumer(handler_started)])
    monkeypatch.setattr(pulsar.asyncio, "Client", lambda url: client)

    async def app(
        scope: Scope,
        receive: AMGIReceiveCallable,
        send: AMGISendCallable,
    ) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("Lifespan unsupported")
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    server = Server(
        app,
        "topic",
        service_url="pulsar://localhost:6650",
        subscription_name="amgi",
        message_send=_FakeMessageSend(),
    )

    with pytest.raises(RuntimeError, match="Receive failed"):
        await server.serve()

    assert handler_cancelled.is_set()
    assert client.closed
