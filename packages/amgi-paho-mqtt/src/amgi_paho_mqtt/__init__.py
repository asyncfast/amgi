import asyncio
from asyncio import Event
from socket import SO_SNDBUF
from socket import SOL_SOCKET
from typing import Any
from typing import Optional
from typing import TYPE_CHECKING

from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from paho.mqtt.client import Client
from paho.mqtt.client import ConnectFlags
from paho.mqtt.client import DisconnectFlags
from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import MQTTMessage
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

if TYPE_CHECKING:
    from paho.mqtt.client import SocketLike


def run(
    app: AMGIApplication,
    topic: str,
    host: str = "localhost",
    port: int = 1883,
    client_id: Optional[str] = None,
) -> None:
    asyncio.run(_run(app, topic, host, port, client_id))


async def _run(
    app: AMGIApplication, topic: str, host: str, port: int, client_id: Optional[str]
) -> None:
    server = Server(app, topic, host, port, client_id)
    await server.serve()


class _MessageReceive:
    def __init__(self, message: MQTTMessage) -> None:
        self._message = message

    async def __call__(self) -> MessageReceiveEvent:
        return {
            "type": "message.receive",
            "id": str(self._message.mid),
            "headers": [],
            "payload": self._message.payload,
        }


class _MessageSend:
    def __init__(self, client: Client) -> None:
        self._client = client

    async def __call__(self, message: AMGISendEvent) -> None:
        if message["type"] == "message.send":
            self._client.publish(message["address"], message["payload"])


class Server:
    def __init__(
        self,
        app: AMGIApplication,
        topic: str,
        host: str,
        port: int,
        client_id: Optional[str],
    ) -> None:
        self._app = app
        self._topic = topic
        self._host = host
        self._port = port
        self._loop = asyncio.get_running_loop()

        self._client = Client(CallbackAPIVersion.VERSION2, client_id=client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._client.on_socket_open = self._on_socket_open
        self._client.on_socket_close = self._on_socket_close
        self._client.on_socket_register_write = self._on_socket_register_write
        self._client.on_socket_unregister_write = self._on_socket_unregister_write
        self._client.on_subscribe = self._on_subscribe

        self._subscribe_event = Event()
        self._disconnected_event = Event()
        self._stop_event = Event()

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        connect_flags: ConnectFlags,
        reason_code: ReasonCode,
        properties: Optional[Properties],
    ) -> None:
        client.subscribe(self._topic)

    def _on_message(self, client: Client, userdata: Any, message: MQTTMessage) -> None:
        self._loop.create_task(self._handle_message(message))

    async def _handle_message(self, message: MQTTMessage) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": message.topic,
        }
        await self._app(scope, _MessageReceive(message), _MessageSend(self._client))

    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        disconnect_flags: DisconnectFlags,
        reason_code: ReasonCode,
        properties: Optional[Properties],
    ) -> None:
        self._disconnected_event.set()

    def _on_socket_open(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        self._loop.add_reader(socket, client.loop_read)
        self._misc_task = self._loop.create_task(self._misc_loop())

    def _on_socket_close(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        self._loop.remove_reader(socket)
        self._misc_task.cancel()

    def _on_socket_register_write(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        self._loop.add_writer(socket, client.loop_write)

    def _on_socket_unregister_write(
        self, client: Client, userdata: Any, socket: "SocketLike"
    ) -> None:
        self._loop.remove_writer(socket)

    def _on_subscribe(
        self,
        client: Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[ReasonCode],
        properties: Optional[Properties],
    ) -> None:
        self._subscribe_event.set()

    async def _misc_loop(self) -> None:
        while self._client.loop_misc() == MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

    async def serve(self) -> None:
        self._client.connect(self._host, self._port, 60)
        self._client.socket().setsockopt(SOL_SOCKET, SO_SNDBUF, 2048)

        await self._subscribe_event.wait()

        async with Lifespan(self._app) as state:
            await self._stop_event.wait()
        self._client.disconnect()
        await self._disconnected_event.wait()

    def stop(self) -> None:
        self._stop_event.set()
