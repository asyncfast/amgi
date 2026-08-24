from collections.abc import Sequence

from amgi_common import server_serve
from amgi_nats._common import _decode_headers
from amgi_nats._common import _MessageSendManagerT
from amgi_nats._common import _MessageSendT
from amgi_nats._common import _SendT
from amgi_nats._common import _Server
from amgi_nats._common import MessageSend as _MessageSend
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageSendEvent
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.errors import TimeoutError
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

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


def _stream_name(subject: str) -> str:
    return subject.replace(".", "_")


class MessageSend(_MessageSend):
    """Publishes through JetStream, waiting for the stream acknowledgement."""

    async def _publish(self, connection: Client, event: MessageSendEvent) -> None:
        bindings = event.get("bindings", {}).get("nats", {})
        if bindings.get("reply", ""):
            raise ValueError(
                "JetStream sends do not support the nats reply binding, the reply "
                "subject carries the stream acknowledgement"
            )

        await connection.jetstream().publish(
            event["address"],
            event.get("payload") or b"",
            headers=_decode_headers(event["headers"]),
        )


class _Send:
    def __init__(self, message: Msg, message_send: _MessageSendT) -> None:
        self._message = message
        self._message_send = message_send

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            await self._message.ack()
        elif event["type"] == "message.nack":
            await self._message.nak()
        elif event["type"] == "message.send":
            await self._message_send(event)


class Server(_Server[JetStreamContext.PullSubscription]):
    _message_send_type = MessageSend

    async def _subscribe(
        self, connection: Client
    ) -> Sequence[JetStreamContext.PullSubscription]:
        jetstream = connection.jetstream()
        return [
            await self._pull_subscribe(jetstream, subject) for subject in self._subjects
        ]

    async def _pull_subscribe(
        self,
        jetstream: JetStreamContext,
        subject: str,
    ) -> JetStreamContext.PullSubscription:
        try:
            stream = await jetstream.find_stream_name_by_subject(subject)
        except NotFoundError:
            stream = _stream_name(subject)
            await jetstream.add_stream(name=stream, subjects=[subject])
        return await jetstream.pull_subscribe(subject, durable=stream, stream=stream)

    async def _next_message(
        self, subscription: JetStreamContext.PullSubscription
    ) -> Msg:
        while True:
            try:
                return (await subscription.fetch(batch=1, timeout=1))[0]
            except TimeoutError:
                pass

    def _send(self, message: Msg, message_send: _MessageSendT) -> _SendT:
        return _Send(message, message_send)
