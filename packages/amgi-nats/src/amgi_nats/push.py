from collections.abc import Sequence

from amgi_common import server_serve
from amgi_nats._common import _MessageSendManagerT
from amgi_nats._common import _MessageSendT
from amgi_nats._common import _SendT
from amgi_nats._common import _Server
from amgi_nats._common import MessageSend
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

__all__ = ["MessageSend", "Server", "run"]


def run(
    app: AMGIApplication,
    *subjects: str,
    servers: str | list[str] = "nats://localhost:4222",
    message_send: _MessageSendManagerT | None = None,
) -> None:
    server = Server(app, *subjects, servers=servers, message_send=message_send)
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    subjects: list[str],
    servers: str = "nats://localhost:4222",
) -> None:
    run(app, *subjects, servers=servers)


class _Send:
    def __init__(self, message_send: _MessageSendT) -> None:
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await self._message_send(event)


class Server(_Server[Subscription]):
    async def _subscribe(self, connection: Client) -> Sequence[Subscription]:
        return [await connection.subscribe(subject) for subject in self._subjects]

    async def _next_message(self, subscription: Subscription) -> Msg:
        return await subscription.next_msg(timeout=None)

    def _send(self, message: Msg, message_send: _MessageSendT) -> _SendT:
        return _Send(message_send)

    def _reply(self, message: Msg) -> str:
        return message.reply or ""
