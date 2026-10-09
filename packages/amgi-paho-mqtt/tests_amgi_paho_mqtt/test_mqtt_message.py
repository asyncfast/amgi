import asyncio
import socket
import threading
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock

import amgi_paho_mqtt
import pytest
from amgi_paho_mqtt import _ClientLoop
from amgi_paho_mqtt import _Send
from amgi_paho_mqtt import _SocketWatcher
from amgi_paho_mqtt import MessageSend
from amgi_paho_mqtt import PublishError
from amgi_types import MessageAckEvent
from amgi_types import MessageSendEvent
from paho.mqtt.client import Client
from paho.mqtt.client import ConnectFlags
from paho.mqtt.client import DisconnectFlags
from paho.mqtt.client import MQTT_ERR_NO_CONN
from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import MQTTMessageInfo
from paho.mqtt.client import MQTTv311
from paho.mqtt.client import MQTTv5
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode


def _patch_client(
    monkeypatch: pytest.MonkeyPatch, connect: Callable[..., Any] | None = None
) -> list[Mock]:
    """Stub the paho client constructed by MessageSend, returning the clients."""
    clients: list[Mock] = []

    def _create_client(*args: Any, **kwargs: Any) -> Mock:
        client = Mock(spec=Client)
        if connect is not None:
            client.connect.side_effect = connect
        clients.append(client)
        return client

    monkeypatch.setattr(amgi_paho_mqtt, "Client", _create_client)
    return clients


def _run_on_selector_loop(coroutine: Coroutine[Any, Any, None]) -> None:
    """
    Run a coroutine on a selector event loop, as the default proactor event loop
    on Windows does not support add_reader, or add_writer.
    """
    loop = asyncio.SelectorEventLoop()
    try:
        loop.run_until_complete(coroutine)
    finally:
        loop.close()


def _create_mock_message_send(
    protocol: MQTTProtocolVersion = MQTTv311,
) -> tuple[MessageSend, Mock]:
    client = Mock(spec=Client)
    client.publish.return_value = MQTTMessageInfo(1)
    client_loop = _ClientLoop(client)
    message_send = MessageSend(protocol=protocol)
    message_send._client = client
    message_send._client_loop = client_loop
    return message_send, client


async def _send(
    message_send: MessageSend, client: Mock, event: MessageSendEvent
) -> None:
    """Send the event, acknowledging the publish like the broker would."""
    publish_task = asyncio.create_task(message_send(event))
    client_loop = message_send._client_loop
    assert client_loop is not None
    await asyncio.sleep(0)
    client_loop._on_publish(
        client, None, 1, ReasonCode(PacketTypes.PUBACK), Mock(spec=Properties)
    )
    await asyncio.wait_for(publish_task, 1)


async def test_send_message_send() -> None:
    message_send = AsyncMock()
    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [],
        "payload": b"test",
    }

    await _Send(message_send)(event)

    message_send.assert_awaited_once_with(event)


async def test_send_ignores_other_events() -> None:
    message_send = AsyncMock()
    event: MessageAckEvent = {"type": "message.ack"}

    await _Send(message_send)(event)

    message_send.assert_not_awaited()


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend()
    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [],
    }

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send(event)


@pytest.mark.parametrize("missing", ["client", "client_loop"])
async def test_message_send_exit_returns_when_not_initialized(missing: str) -> None:
    message_send = MessageSend()
    if missing == "client":
        message_send._client_loop = Mock()
    else:
        message_send._client = Mock()

    await message_send.__aexit__(None, None, None)


@pytest.mark.parametrize("qos", [1, 2])
async def test_disconnect_fails_all_pending_publishes(qos: int) -> None:
    client = Mock(spec=Client)
    client.publish.side_effect = [MQTTMessageInfo(1), MQTTMessageInfo(2)]
    client_loop = _ClientLoop(client)
    tasks = [
        asyncio.create_task(client_loop.publish("send-topic", b"test", qos))
        for _ in range(2)
    ]
    await asyncio.sleep(0)

    reason_code = ReasonCode(PacketTypes.DISCONNECT, "Unspecified error")
    client_loop._on_disconnect(client, None, DisconnectFlags(False), reason_code, None)
    # A repeated disconnect must not try to fail completed futures again.
    client_loop._on_disconnect(client, None, DisconnectFlags(False), reason_code, None)
    await asyncio.wait_for(client_loop.wait_disconnected(), timeout=1)

    # A late acknowledgement may arrive before the awaiting tasks resume.
    client_loop._on_publish(
        client, None, 1, ReasonCode(PacketTypes.PUBACK), Mock(spec=Properties)
    )

    results = await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True), timeout=1
    )
    assert all(isinstance(result, PublishError) for result in results)
    assert all(str(result) == "Unspecified error" for result in results)

    # The unexpected disconnect schedules a reconnection, which is stopped on close.
    await client_loop.close()


async def test_disconnect_preserves_completed_and_cancelled_publishes() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    client_loop = _ClientLoop(client)
    completed = asyncio.get_running_loop().create_future()
    completed.set_result(None)
    cancelled = asyncio.get_running_loop().create_future()
    cancelled.cancel()
    client_loop._publish_futures[1] = completed
    client_loop._publish_futures[2] = cancelled

    client_loop._on_disconnect(
        client, None, DisconnectFlags(False), ReasonCode(PacketTypes.DISCONNECT), None
    )

    assert completed.result() is None
    assert cancelled.cancelled()
    assert client_loop._reconnect_task is None


@pytest.mark.parametrize("qos", [0, 1])
async def test_publish_while_disconnected_raises(qos: int) -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    client_loop = _ClientLoop(client)

    with pytest.raises(PublishError, match="not currently connected"):
        await asyncio.wait_for(client_loop.publish("send-topic", b"test", qos), 1)


async def test_publish_no_conn_does_not_register_future() -> None:
    client = Mock(spec=Client)
    mqtt_message_info = MQTTMessageInfo(1)
    mqtt_message_info.rc = MQTT_ERR_NO_CONN
    client.publish.return_value = mqtt_message_info
    client_loop = _ClientLoop(client)

    with pytest.raises(PublishError):
        await client_loop.publish("send-topic", b"test", 1)

    assert not client_loop._publish_futures


async def test_unexpected_disconnect_schedules_single_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(amgi_paho_mqtt, "_RECONNECT_DELAY", 0.0)
    client = Mock(spec=Client)
    client.reconnect.return_value = MQTT_ERR_SUCCESS
    client_loop = _ClientLoop(client)

    reason_code = ReasonCode(PacketTypes.DISCONNECT, "Unspecified error")
    client_loop._on_disconnect(client, None, DisconnectFlags(False), reason_code, None)
    # A second disconnect while reconnecting must not spawn a second task.
    client_loop._on_disconnect(client, None, DisconnectFlags(False), reason_code, None)

    reconnect_task = client_loop._reconnect_task
    assert reconnect_task is not None
    await asyncio.wait_for(reconnect_task, 1)

    client.reconnect.assert_called_once()


async def test_reconnect_restores_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(amgi_paho_mqtt, "_RECONNECT_DELAY", 0.0)
    client = Mock(spec=Client)
    client.reconnect.return_value = MQTT_ERR_SUCCESS
    client.publish.return_value = MQTTMessageInfo(1)
    client_loop = _ClientLoop(client)

    client_loop._on_disconnect(
        client,
        None,
        DisconnectFlags(False),
        ReasonCode(PacketTypes.DISCONNECT, "Unspecified error"),
        None,
    )
    reconnect_task = client_loop._reconnect_task
    assert reconnect_task is not None
    await asyncio.wait_for(reconnect_task, 1)

    publish_task = asyncio.create_task(client_loop.publish("send-topic", b"test", 1))
    await asyncio.sleep(0)
    client_loop._on_publish(
        client, None, 1, ReasonCode(PacketTypes.PUBACK), Mock(spec=Properties)
    )

    await asyncio.wait_for(publish_task, 1)
    client.reconnect.assert_called_once()


async def test_reconnect_retries_until_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(amgi_paho_mqtt, "_RECONNECT_DELAY", 0.0)
    client = Mock(spec=Client)
    client.reconnect.side_effect = ConnectionRefusedError()
    client_loop = _ClientLoop(client)

    client_loop._on_disconnect(
        client,
        None,
        DisconnectFlags(False),
        ReasonCode(PacketTypes.DISCONNECT, "Unspecified error"),
        None,
    )
    reconnect_task = client_loop._reconnect_task
    assert reconnect_task is not None

    while client.reconnect.call_count < 2:
        await asyncio.sleep(0)

    await client_loop.close()
    assert reconnect_task.cancelled()

    # After close, no further reconnections are scheduled.
    client_loop._on_disconnect(
        client,
        None,
        DisconnectFlags(False),
        ReasonCode(PacketTypes.DISCONNECT, "Unspecified error"),
        None,
    )
    assert client_loop._reconnect_task is None


async def test_graceful_disconnect_does_not_reconnect() -> None:
    client = Mock(spec=Client)
    client_loop = _ClientLoop(client)

    client_loop._on_disconnect(
        client, None, DisconnectFlags(False), ReasonCode(PacketTypes.DISCONNECT), None
    )

    assert client_loop._reconnect_task is None


async def test_message_send_connect_refused() -> None:
    message_send = MessageSend(port=1)

    with pytest.raises(ConnectionRefusedError):
        await message_send.__aenter__()

    assert message_send._client is None
    assert message_send._client_loop is None


async def test_message_send_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = _patch_client(monkeypatch)
    message_send = MessageSend()

    enter_task = asyncio.create_task(message_send.__aenter__())
    while not clients:
        await asyncio.sleep(0)

    message_send._on_connect(
        clients[0],
        None,
        ConnectFlags(False),
        ReasonCode(PacketTypes.CONNACK, "Not authorized"),
        None,
    )

    with pytest.raises(ConnectionError, match="Not authorized"):
        await asyncio.wait_for(enter_task, 1)

    assert message_send._client is None
    assert message_send._client_loop is None


async def test_message_send_connect_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = _patch_client(monkeypatch)

    # Generous enough that the threaded connect finishes first, even on slow runners.
    message_send = MessageSend(connect_timeout=0.2)
    with pytest.raises(TimeoutError, match="Timed out waiting"):
        await message_send.__aenter__()

    assert clients[0].disconnect.called
    assert message_send._client is None
    assert message_send._client_loop is None

    # A retry starts a fresh connection.
    enter_task = asyncio.create_task(message_send.__aenter__())
    while len(clients) < 2:
        await asyncio.sleep(0)
    message_send._on_connect(
        clients[1], None, ConnectFlags(False), ReasonCode(PacketTypes.CONNACK), None
    )
    await asyncio.wait_for(enter_task, 1)
    assert message_send._client is clients[1]


async def test_message_send_is_reenterable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = _patch_client(monkeypatch)
    message_send = MessageSend()

    enter_task = asyncio.create_task(message_send.__aenter__())
    while not clients:
        await asyncio.sleep(0)
    message_send._on_connect(
        clients[0], None, ConnectFlags(False), ReasonCode(PacketTypes.CONNACK), None
    )
    await asyncio.wait_for(enter_task, 1)

    first_client = clients[0]
    first_client_loop = message_send._client_loop
    assert first_client_loop is not None

    exit_task = asyncio.create_task(message_send.__aexit__(None, None, None))
    while first_client.disconnect.call_count == 0:
        await asyncio.sleep(0)
    first_client_loop._on_disconnect(
        first_client,
        None,
        DisconnectFlags(False),
        ReasonCode(PacketTypes.DISCONNECT),
        None,
    )
    await asyncio.wait_for(exit_task, 1)
    assert message_send._client is None

    # The second session must wait for its own CONNACK.
    enter_task = asyncio.create_task(message_send.__aenter__())
    while len(clients) < 2:
        await asyncio.sleep(0)
    while message_send._client is not clients[1]:
        await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not message_send._connected_event.is_set()
    assert not enter_task.done()
    message_send._on_connect(
        clients[1], None, ConnectFlags(False), ReasonCode(PacketTypes.CONNACK), None
    )
    await asyncio.wait_for(enter_task, 1)
    assert message_send._client is clients[1]


@pytest.mark.parametrize("qos", [0, 1, 2])
@pytest.mark.parametrize("retain", [True, False])
async def test_message_send_extracts_qos_and_retain(qos: int, retain: bool) -> None:
    message_send, client = _create_mock_message_send()

    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [],
        "payload": b"test",
        "bindings": {"mqtt": {"qos": qos, "retain": retain}},
    }
    await _send(message_send, client, event)

    client.publish.assert_called_once_with(
        "send-topic", b"test", qos=qos, retain=retain
    )


async def test_message_send_maps_headers_to_user_properties() -> None:
    message_send, client = _create_mock_message_send(MQTTv5)

    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [(b"content-type", b"application/json"), (b"trace-id", b"abc")],
        "payload": b"test",
    }
    await _send(message_send, client, event)

    client.publish.assert_called_once()
    call = client.publish.call_args
    assert call.args == ("send-topic", b"test")
    assert set(call.kwargs) == {"qos", "retain", "properties"}
    assert call.kwargs["qos"] == 0
    assert call.kwargs["retain"] is False
    properties = call.kwargs["properties"]
    assert properties.ContentType == "application/json"
    assert properties.UserProperty == [("trace-id", "abc")]


async def test_message_send_drops_headers_for_mqtt_v3() -> None:
    message_send, client = _create_mock_message_send()

    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [(b"trace-id", b"abc")],
        "payload": b"test",
    }
    await _send(message_send, client, event)

    client.publish.assert_called_once_with("send-topic", b"test", qos=0, retain=False)


async def test_message_send_v5_without_headers_sends_no_properties() -> None:
    message_send, client = _create_mock_message_send(MQTTv5)

    event: MessageSendEvent = {
        "type": "message.send",
        "address": "send-topic",
        "headers": [],
        "payload": b"test",
    }
    await _send(message_send, client, event)

    client.publish.assert_called_once_with("send-topic", b"test", qos=0, retain=False)


async def test_cancelled_reconnect_disconnects_late_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(amgi_paho_mqtt, "_RECONNECT_DELAY", 0.0)
    release = threading.Event()
    started = threading.Event()

    def _slow_reconnect() -> int:
        started.set()
        release.wait(5)
        return int(MQTT_ERR_SUCCESS)

    client = Mock(spec=Client)
    client.reconnect.side_effect = _slow_reconnect
    client_loop = _ClientLoop(client)
    client_loop._on_disconnect(
        client,
        None,
        DisconnectFlags(False),
        ReasonCode(PacketTypes.DISCONNECT, "Unspecified error"),
        None,
    )
    while not started.is_set():
        await asyncio.sleep(0)

    await client_loop.close()
    client.disconnect.assert_not_called()

    release.set()
    while not client.disconnect.called:
        await asyncio.sleep(0)


async def test_late_reconnect_failure_does_not_disconnect() -> None:
    client = Mock(spec=Client)
    client_loop = _ClientLoop(client)

    failed: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    failed.set_exception(OSError())
    client_loop._disconnect_late_reconnect(failed)

    cancelled: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    cancelled.cancel()
    client_loop._disconnect_late_reconnect(cancelled)

    refused: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    refused.set_result(MQTT_ERR_NO_CONN)
    client_loop._disconnect_late_reconnect(refused)

    client.disconnect.assert_not_called()


async def test_message_send_connect_timeout_cleans_up_late_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    clients = _patch_client(monkeypatch, lambda *a: release.wait(5))

    message_send = MessageSend(connect_timeout=0.01)
    with pytest.raises(TimeoutError, match="Timed out connecting"):
        await message_send.__aenter__()
    assert clients[0].disconnect.call_count == 1

    release.set()
    while message_send._late_cleanups or clients[0].disconnect.call_count < 2:
        await asyncio.sleep(0)


async def test_message_send_connect_worker_failure_skips_late_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = _patch_client(monkeypatch, Mock(side_effect=OSError("refused")))

    message_send = MessageSend()
    with pytest.raises(OSError):
        await message_send.__aenter__()
    assert clients[0].disconnect.call_count == 1
    assert not message_send._late_cleanups


async def test_message_send_cancel_while_connecting_aborts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = _patch_client(monkeypatch)
    message_send = MessageSend()

    enter_task = asyncio.create_task(message_send.__aenter__())
    while message_send._client is None:
        await asyncio.sleep(0)
    enter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await enter_task

    assert clients[0].disconnect.called
    assert message_send._client is None
    assert message_send._client_loop is None


async def test_message_send_cancel_during_connect_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    started = threading.Event()

    def _connect(*a: Any) -> None:
        started.set()
        release.wait(5)

    created = _patch_client(monkeypatch, _connect)

    message_send = MessageSend()
    enter_task = asyncio.create_task(message_send.__aenter__())
    while not started.is_set():
        await asyncio.sleep(0.001)
    enter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await enter_task

    release.set()
    while created[0].disconnect.call_count < 2:
        await asyncio.sleep(0.001)


async def test_reconnect_retries_on_error_return_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(amgi_paho_mqtt, "_RECONNECT_DELAY", 0.0)
    client = Mock(spec=Client)
    client.reconnect.side_effect = [MQTT_ERR_NO_CONN, MQTT_ERR_SUCCESS]
    client_loop = _ClientLoop(client)

    client_loop._schedule_reconnect()
    reconnect_task = client_loop._reconnect_task
    assert reconnect_task is not None
    await asyncio.wait_for(reconnect_task, 1)

    assert client.reconnect.call_count == 2


def test_socket_open_registers_reader_and_close_unregisters() -> None:
    _run_on_selector_loop(_socket_open_registers_reader_and_close_unregisters())


async def _socket_open_registers_reader_and_close_unregisters() -> None:
    client = Mock(spec=Client)
    client.loop_misc.return_value = MQTT_ERR_SUCCESS
    client_loop = _ClientLoop(client)
    sock, peer = socket.socketpair()
    try:
        client_loop._on_socket_open(client, None, sock)
        misc_task = client_loop._misc_task
        assert misc_task is not None

        peer.send(b"x")
        while not client.loop_read.called:
            await asyncio.sleep(0)

        client_loop._on_socket_register_write(client, None, sock)
        while not client.loop_write.called:
            await asyncio.sleep(0)
        client_loop._on_socket_unregister_write(client, None, sock)

        client_loop._on_socket_close(client, None, sock)
        assert client_loop._misc_task is None
        await asyncio.sleep(0)
        assert misc_task.done()
    finally:
        sock.close()
        peer.close()


async def test_socket_open_after_close_does_not_register() -> None:
    client = Mock(spec=Client)
    client_loop = _ClientLoop(client)
    sock, peer = socket.socketpair()
    try:
        await client_loop.close()
        client_loop._on_socket_open(client, None, sock)
        assert client_loop._misc_task is None
    finally:
        sock.close()
        peer.close()


def test_close_cancels_misc_task() -> None:
    _run_on_selector_loop(_close_cancels_misc_task())


async def _close_cancels_misc_task() -> None:
    client = Mock(spec=Client)
    client.loop_misc.return_value = MQTT_ERR_SUCCESS
    client_loop = _ClientLoop(client)
    sock, peer = socket.socketpair()
    try:
        client_loop._on_socket_open(client, None, sock)
        misc_task = client_loop._misc_task
        assert misc_task is not None
        await client_loop.close()
        await asyncio.sleep(0)
        assert misc_task.done()
    finally:
        client_loop.loop.remove_reader(sock)
        sock.close()
        peer.close()


def test_call_on_loop_without_running_loop_marshals_to_loop() -> None:
    client_loop = _ClientLoop(Mock(spec=Client))
    loop = Mock(spec=asyncio.AbstractEventLoop)
    client_loop.__dict__["loop"] = loop
    callback = Mock()

    client_loop._call_on_loop(callback)

    loop.call_soon_threadsafe.assert_called_once_with(callback)
    callback.assert_not_called()


async def test_message_send_connect_timeout_then_worker_failure_skips_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()

    def _connect(*a: Any) -> None:
        release.wait(5)
        raise OSError("refused")

    clients = _patch_client(monkeypatch, _connect)

    message_send = MessageSend(connect_timeout=0.01)
    with pytest.raises(TimeoutError, match="Timed out connecting"):
        await message_send.__aenter__()

    release.set()
    await asyncio.sleep(0.1)

    assert clients[0].disconnect.call_count == 1
    assert not message_send._late_cleanups


async def test_socket_watcher_fallback_drives_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Like the proactor event loop on Windows, which does not support add_reader.
    monkeypatch.setattr(
        asyncio.get_running_loop(),
        "add_reader",
        Mock(side_effect=NotImplementedError),
    )
    client = Mock(spec=Client)
    client.loop_misc.return_value = MQTT_ERR_SUCCESS
    callback_threads: set[int] = set()
    client.loop_read.side_effect = lambda: callback_threads.add(threading.get_ident())
    client.loop_write.side_effect = lambda: callback_threads.add(threading.get_ident())
    client_loop = _ClientLoop(client)
    sock, peer = socket.socketpair()
    try:
        client_loop._on_socket_open(client, None, sock)
        socket_watcher = client_loop._socket_watcher
        assert socket_watcher is not None
        misc_task = client_loop._misc_task
        assert misc_task is not None

        peer.send(b"x")
        while not client.loop_read.called:
            await asyncio.sleep(0)

        client_loop._on_socket_register_write(client, None, sock)
        while not client.loop_write.called:
            await asyncio.sleep(0)
        client_loop._on_socket_unregister_write(client, None, sock)

        client_loop._on_socket_close(client, None, sock)
        assert client_loop._socket_watcher is None
        await asyncio.sleep(0)
        assert misc_task.done()
        await asyncio.to_thread(socket_watcher._thread.join, 1)
        assert not socket_watcher._thread.is_alive()
        assert callback_threads == {threading.get_ident()}
    finally:
        sock.close()
        peer.close()


async def test_socket_watcher_exits_when_socket_closed() -> None:
    # Windows may report a socket closed during select as readable, so the read
    # callback can be invoked once before the watcher exits.
    sock, peer = socket.socketpair()
    try:
        socket_watcher = _SocketWatcher(asyncio.get_running_loop(), sock, Mock())
        sock.close()
        socket_watcher.set_writer(None)
        await asyncio.to_thread(socket_watcher._thread.join, 1)
        assert not socket_watcher._thread.is_alive()
    finally:
        peer.close()


def test_socket_watcher_exits_when_loop_closed() -> None:
    loop = asyncio.new_event_loop()
    on_read = Mock()
    sock, peer = socket.socketpair()
    try:
        socket_watcher = _SocketWatcher(loop, sock, on_read)
        loop.close()
        peer.send(b"x")
        socket_watcher._thread.join(1)
        assert not socket_watcher._thread.is_alive()
        on_read.assert_not_called()

        # Waking a stopped watcher is ignored.
        socket_watcher.close()
    finally:
        sock.close()
        peer.close()


async def test_socket_watcher_dispatch_after_close_is_ignored() -> None:
    on_read = Mock()
    on_write = Mock()
    sock, peer = socket.socketpair()
    try:
        socket_watcher = _SocketWatcher(asyncio.get_running_loop(), sock, on_read)
        socket_watcher.set_writer(on_write)
        socket_watcher.close()
        await asyncio.to_thread(socket_watcher._thread.join, 1)

        socket_watcher._dispatch(True, True)

        on_read.assert_not_called()
        on_write.assert_not_called()
    finally:
        sock.close()
        peer.close()
