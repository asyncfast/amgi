from amgi_common import server_serve
from amgi_nats_py._base import _BaseServer
from amgi_nats_py._base import _message_send
from amgi_nats_py._base import NatsClient
from amgi_types import AMGIApplication
from amgi_types import AMGISendCallable
from amgi_types import AMGISendEvent
from nats.aio.client import Client
from nats.aio.msg import Msg


def run(app: AMGIApplication, *subjects: str, servers: str | list[str]) -> None:
    server = Server(app, *subjects, servers=servers)
    server_serve(server)


def _run_cli(
    app: AMGIApplication,
    subjects: list[str],
    servers: list[str] | None = None,
) -> None:
    if servers is None:
        servers = ["nats://localhost:4222"]
    run(app, *subjects, servers=servers)


class _Send:
    def __init__(self, client: Client, msg: Msg) -> None:
        self._client = client
        self._msg = msg

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            await _message_send(self._client, event)


class Server(_BaseServer):
    def __init__(
        self, app: AMGIApplication, *subjects: str, servers: str | list[str]
    ) -> None:
        super().__init__(app, *subjects, servers=servers)

    async def serve(self) -> None:
        client = Client()
        await client.connect(self._servers)

        await self._client_serve(client)

    def _send(self, client: NatsClient, msg: Msg) -> AMGISendCallable:
        return _Send(client, msg)
