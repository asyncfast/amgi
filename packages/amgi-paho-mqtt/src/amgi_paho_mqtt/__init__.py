import asyncio
import logging
import sys
import threading
from asyncio import AbstractEventLoop
from asyncio import Event
from asyncio import Future
from asyncio import Task
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import suppress
from functools import cached_property
from select import select
from socket import SO_SNDBUF
from socket import socketpair
from socket import SOL_SOCKET
from types import TracebackType
from typing import Any
from typing import AsyncContextManager
from typing import TYPE_CHECKING
from weakref import WeakValueDictionary

from amgi_common import Lifespan
from amgi_common import server_serve
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from paho.mqtt.client import Client
from paho.mqtt.client import ConnectFlags
from paho.mqtt.client import DisconnectFlags
from paho.mqtt.client import error_string
from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import MQTTMessage
from paho.mqtt.client import MQTTv311
from paho.mqtt.client import MQTTv5
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

if sys.version_info >= (3, 11):  # pragma: no cover
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from paho.mqtt.client import SocketLike

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]

logger = logging.getLogger("amgi-paho-mqtt.error")

_RECONNECT_DELAY = 1.0


def run(
    app: AMGIApplication,
    topic: str,
    host: str = "localhost",
    port: int = 1883,
    client_id: str | None = None,
    message_send: _MessageSendManagerT | None = None,
    connect_timeout: float = 10.0,
) -> None:
    server = Server(
        app,
        topic,
        host,
        port,
        client_id,
        message_send=message_send,
        connect_timeout=connect_timeout,
    )
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    topic: str,
    host: str = "localhost",
    port: int = 1883,
    client_id: str | None = None,
) -> None:
    run(app, topic, host=host, port=port, client_id=client_id)


class PublishError(OSError):
    """Raised when publishing fails."""


async def _receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


def _create_properties(headers: Sequence[tuple[bytes, bytes]]) -> Properties:
    # paho-mqtt does not provide type annotations for the Properties constructor.
    properties_factory: Any = Properties
    properties: Properties = properties_factory(PacketTypes.PUBLISH)
    for key, value in headers:
        if key.lower() == b"content-type":
            properties.ContentType = value.decode()
        else:
            properties.UserProperty = (key.decode(), value.decode())
    return properties


class _SocketWatcher:
    """
    Watches a socket for readiness from a background thread, invoking the read,
    and write callbacks on the event loop. This is used when the event loop does
    not support add_reader, and add_writer, such as the proactor event loop on
    Windows. As the callbacks are invoked on the event loop, paho is only ever
    driven from the event loop thread.
    """

    def __init__(
        self,
        loop: AbstractEventLoop,
        socket: "SocketLike",
        on_read: Callable[[], Any],
    ) -> None:
        self._loop = loop
        self._socket = socket
        self._on_read = on_read
        self._on_write: Callable[[], Any] | None = None
        self._closed = False
        # Each readiness is handled by the event loop before the socket is selected
        # again, as select is level triggered.
        self._handled = threading.Event()
        self._waker_receive, self._waker_send = socketpair()
        self._waker_receive.setblocking(False)
        self._waker_send.setblocking(False)
        self._thread = threading.Thread(
            target=self._run, name="amgi-paho-mqtt-socket-watcher", daemon=True
        )
        self._thread.start()

    def set_writer(self, on_write: Callable[[], Any] | None) -> None:
        self._on_write = on_write
        self._wake()

    def close(self) -> None:
        self._closed = True
        self._handled.set()
        self._wake()

    def _wake(self) -> None:
        with suppress(OSError):
            self._waker_send.send(b"\0")

    def _run(self) -> None:
        try:
            while not self._closed:
                writers = [self._socket] if self._on_write is not None else []
                try:
                    readable, writable, _ = select(
                        [self._socket, self._waker_receive], writers, []
                    )
                except (OSError, ValueError):
                    # The socket was closed by paho.
                    return
                if self._waker_receive in readable:
                    with suppress(OSError):
                        self._waker_receive.recv(4096)
                read = self._socket in readable
                write = self._socket in writable
                if self._closed or not (read or write):
                    continue
                self._handled.clear()
                try:
                    self._loop.call_soon_threadsafe(self._dispatch, read, write)
                except RuntimeError:
                    # The event loop is closed.
                    return
                self._handled.wait()
        finally:
            self._waker_receive.close()
            self._waker_send.close()

    def _dispatch(self, read: bool, write: bool) -> None:
        try:
            if self._closed:
                return
            if read:
                self._on_read()
            on_write = self._on_write
            if write and on_write is not None and not self._closed:
                on_write()
        finally:
            self._handled.set()


class _ClientLoop:
    """
    Drives a paho client from the running event loop, so the network loop, and
    publish acknowledgements are handled asynchronously, rather than by a paho
    managed thread.
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self._disconnected_event = Event()
        self._publish_futures = WeakValueDictionary[int, Future[None]]()
        self._misc_task: Task[None] | None = None
        self._socket_watcher: _SocketWatcher | None = None
        self._reconnect_task: Task[None] | None = None
        self._closed = False

        client.on_socket_open = self._on_socket_open
        client.on_socket_close = self._on_socket_close
        client.on_socket_register_write = self._on_socket_register_write
        client.on_socket_unregister_write = self._on_socket_unregister_write
        client.on_publish = self._on_publish
        client.on_disconnect = self._on_disconnect

    @cached_property
    def loop(self) -> AbstractEventLoop:
        return asyncio.get_running_loop()

    def _on_socket_open(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        def _register() -> None:
            if self._closed:
                return
            loop = self.loop
            try:
                loop.add_reader(socket, client.loop_read)
            except NotImplementedError:
                self._socket_watcher = _SocketWatcher(loop, socket, client.loop_read)
            self._misc_task = loop.create_task(self._misc_loop(client))

        self._call_on_loop(_register)

    def _on_socket_close(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        misc_task = self._misc_task
        self._misc_task = None

        def _unregister() -> None:
            socket_watcher = self._socket_watcher
            if socket_watcher is not None:
                self._socket_watcher = None
                socket_watcher.close()
            else:
                self.loop.remove_reader(socket)
            if misc_task is not None and not misc_task.done():
                misc_task.cancel()

        self._call_on_loop(_unregister)

    def _on_socket_register_write(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        def _register_write() -> None:
            socket_watcher = self._socket_watcher
            if socket_watcher is not None:
                socket_watcher.set_writer(client.loop_write)
            else:
                self.loop.add_writer(socket, client.loop_write)

        self._call_on_loop(_register_write)

    def _on_socket_unregister_write(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        def _unregister_write() -> None:
            socket_watcher = self._socket_watcher
            if socket_watcher is not None:
                socket_watcher.set_writer(None)
            else:
                self.loop.remove_writer(socket)

        self._call_on_loop(_unregister_write)

    def _call_on_loop(self, callback: Callable[[], None]) -> None:
        """
        Invoke a callback on the event loop. paho may invoke the socket
        callbacks from a worker thread, when a blocking connect is offloaded,
        in which case the callback is marshalled back onto the event loop.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self.loop.call_soon_threadsafe(callback)
        else:
            callback()

    def _on_publish(
        self,
        client: Client,
        userdata: Any,
        mid: int,
        reason_code: ReasonCode,
        properties: Properties,
    ) -> None:
        message_future = self._publish_futures.get(mid)

        if message_future is not None and not message_future.done():
            if reason_code.is_failure:
                message_future.set_exception(PublishError(reason_code.getName()))
            else:
                message_future.set_result(None)

    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        disconnect_flags: DisconnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        self._disconnected_event.set()
        error = PublishError(str(reason_code))
        for message_future in tuple(self._publish_futures.values()):
            if not message_future.done():
                message_future.set_exception(error)

        if reason_code.value != 0:
            self._schedule_reconnect()

    async def publish(
        self,
        topic: str,
        payload: bytes | None,
        qos: int,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> None:
        if properties is None:
            mqtt_message_info = self._client.publish(
                topic, payload, qos=qos, retain=retain
            )
        else:
            mqtt_message_info = self._client.publish(
                topic, payload, qos=qos, retain=retain, properties=properties
            )

        if mqtt_message_info.rc != MQTT_ERR_SUCCESS:
            raise PublishError(
                f"Failed to publish message: {error_string(mqtt_message_info.rc)}"
            )

        message_future = self.loop.create_future()
        self._publish_futures[mqtt_message_info.mid] = message_future
        await message_future

    async def wait_disconnected(self) -> None:
        await self._disconnected_event.wait()

    async def close(self) -> None:
        """
        Stop driving the client: any in-flight reconnection is cancelled, and no
        further reconnections are scheduled.
        """
        self._closed = True

        reconnect_task = self._reconnect_task
        self._reconnect_task = None
        if reconnect_task is not None:
            reconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await reconnect_task

        misc_task = self._misc_task
        if misc_task is not None and not misc_task.done():
            misc_task.cancel()

    def _schedule_reconnect(self) -> None:
        if self._closed:
            return
        reconnect_task = self._reconnect_task
        if reconnect_task is not None and not reconnect_task.done():
            return
        self._reconnect_task = self.loop.create_task(self._reconnect())

    async def _reconnect(self) -> None:
        while True:
            await asyncio.sleep(_RECONNECT_DELAY)
            worker = asyncio.ensure_future(asyncio.to_thread(self._client.reconnect))
            try:
                rc = await asyncio.shield(worker)
            except asyncio.CancelledError:
                # The worker thread cannot be cancelled, so a connection it goes on
                # to establish must be closed once it finishes.
                worker.add_done_callback(self._disconnect_late_reconnect)
                raise
            except OSError as error:
                logger.warning("MQTT reconnection failed: %s", error)
                continue
            if rc != MQTT_ERR_SUCCESS:
                logger.warning("MQTT reconnection failed: %s", error_string(rc))
                continue
            return

    def _disconnect_late_reconnect(self, worker: "Future[int]") -> None:
        if worker.cancelled() or worker.exception() is not None:
            return
        if worker.result() == MQTT_ERR_SUCCESS:
            self._client.disconnect()

    async def _misc_loop(self, client: Client) -> None:
        while client.loop_misc() == MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break


class MessageSend:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        protocol: MQTTProtocolVersion = MQTTv311,
        connect_timeout: float = 10.0,
    ) -> None:
        self._host = host
        self._port = port
        self._protocol = protocol
        self._connect_timeout = connect_timeout
        self._client: Client | None = None
        self._client_loop: _ClientLoop | None = None
        self._connected_event = Event()
        self._connect_error: ConnectionError | None = None
        self._late_cleanups: set[Task[None]] = set()

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        connect_flags: ConnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        if reason_code.is_failure:
            self._connect_error = ConnectionError(
                f"MQTT connection failed: {reason_code}"
            )
        else:
            self._connect_error = None
        self._connected_event.set()

    async def __aenter__(self) -> Self:
        """
        Connect to the broker, and wait for the connection to be acknowledged.

        A failed or timed out connection raises, leaving the manager free to be
        entered again to start a new connection.
        """
        self._connected_event.clear()
        self._connect_error = None

        client = Client(CallbackAPIVersion.VERSION2, protocol=self._protocol)
        client.on_connect = self._on_connect
        client_loop = _ClientLoop(client)
        self._client_loop = client_loop
        # Resolve the event loop before the blocking connect is offloaded to a thread.
        client_loop.loop

        worker = asyncio.ensure_future(
            asyncio.to_thread(client.connect, self._host, self._port, 60)
        )
        try:
            await asyncio.wait_for(asyncio.shield(worker), self._connect_timeout)
        except asyncio.TimeoutError:
            self._cleanup_after_worker(worker, client, client_loop)
            await self._abort_connect(client, client_loop)
            raise TimeoutError("Timed out connecting to MQTT broker") from None
        except asyncio.CancelledError:
            self._cleanup_after_worker(worker, client, client_loop)
            await self._abort_connect(client, client_loop)
            raise
        except OSError:
            await self._abort_connect(client, client_loop)
            raise

        self._client = client
        try:
            await asyncio.wait_for(self._connected_event.wait(), self._connect_timeout)
        except asyncio.TimeoutError:
            await self._abort_connect(client, client_loop)
            raise TimeoutError("Timed out waiting for MQTT connection") from None
        except asyncio.CancelledError:
            await self._abort_connect(client, client_loop)
            raise

        connect_error = self._connect_error
        if connect_error is not None:
            await self._abort_connect(client, client_loop)
            raise connect_error

        return self

    async def __call__(self, event: MessageSendEvent) -> None:
        if self._client is None or self._client_loop is None:
            raise RuntimeError("MessageSend not initialized")

        mqtt_bindings = event.get("bindings", {}).get("mqtt", {})
        qos = mqtt_bindings.get("qos", 0)
        retain = mqtt_bindings.get("retain", False)

        properties: Properties | None = None
        headers = event.get("headers")
        if self._protocol == MQTTv5 and headers:
            # MQTT v3.1.1 has no way to represent headers, so they are dropped.
            properties = _create_properties(headers)

        await self._client_loop.publish(
            event["address"],
            event.get("payload"),
            qos,
            retain=retain,
            properties=properties,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._client is None or self._client_loop is None:
            return

        client = self._client
        client_loop = self._client_loop
        await client_loop.close()
        client.disconnect()
        await client_loop.wait_disconnected()

        self._client = None
        self._client_loop = None

    async def _abort_connect(self, client: Client, client_loop: _ClientLoop) -> None:
        self._client = None
        self._client_loop = None
        await client_loop.close()
        client.disconnect()

    def _cleanup_after_worker(
        self, worker: "Future[None]", client: Client, client_loop: _ClientLoop
    ) -> None:
        """
        The blocking connect cannot be cancelled, so a connection it goes on to
        establish after being abandoned is closed once the worker finishes.
        """

        def _cleanup(_: "Future[None]") -> None:
            if worker.cancelled() or worker.exception() is not None:
                return
            task = client_loop.loop.create_task(self._discard_late(client, client_loop))
            self._late_cleanups.add(task)
            task.add_done_callback(self._late_cleanups.discard)

        worker.add_done_callback(_cleanup)

    async def _discard_late(self, client: Client, client_loop: _ClientLoop) -> None:
        await client_loop.close()
        client.disconnect()


class _Send:
    def __init__(self, message_send: _MessageSendT) -> None:
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await self._message_send(event)


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        topic: str,
        host: str,
        port: int,
        client_id: str | None,
        protocol: MQTTProtocolVersion = MQTTv311,
        message_send: _MessageSendManagerT | None = None,
        connect_timeout: float = 10.0,
    ) -> None:
        self._app = app
        self._topic = topic
        self._host = host
        self._port = port

        self._client = Client(
            CallbackAPIVersion.VERSION2, client_id=client_id, protocol=protocol
        )
        self._client_loop = _ClientLoop(self._client)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe

        self._message_send_context = message_send or MessageSend(
            host, port, protocol=protocol, connect_timeout=connect_timeout
        )
        self._message_send: _MessageSendT | None = None

        self._subscribe_event = Event()
        self._stop_event = Event()
        self._tasks = set[Task[None]]()
        self._state: dict[str, Any] = {}

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        connect_flags: ConnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        client.subscribe(self._topic)

    def _on_message(self, client: Client, userdata: Any, message: MQTTMessage) -> None:
        task = self._client_loop.loop.create_task(self._handle_message(message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _handle_message(self, message: MQTTMessage) -> None:
        assert self._message_send is not None

        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": message.topic,
            "headers": [],
            "payload": message.payload,
            "state": self._state.copy(),
        }
        await self._app(scope, _receive, _Send(self._message_send))

    def _on_subscribe(
        self,
        client: Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[ReasonCode],
        properties: Properties | None,
    ) -> None:
        self._subscribe_event.set()

    async def serve(self) -> None:
        async with self._message_send_context as message_send:
            self._message_send = message_send

            # Resolve the event loop before the blocking connect is offloaded to a thread.
            self._client_loop.loop
            await asyncio.to_thread(self._client.connect, self._host, self._port, 60)
            self._client.socket().setsockopt(SOL_SOCKET, SO_SNDBUF, 2048)

            await self._subscribe_event.wait()

            async with Lifespan(self._app, self._state):
                await self._stop_event.wait()
                self._client.unsubscribe(self._topic)
                await asyncio.gather(*self._tasks)
            await self._client_loop.close()
            self._client.disconnect()
            await self._client_loop.wait_disconnected()

    def stop(self) -> None:
        self._stop_event.set()
