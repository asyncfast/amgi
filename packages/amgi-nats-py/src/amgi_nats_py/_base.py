import asyncio
from abc import ABC
from abc import abstractmethod
from asyncio import Event
from functools import partial
from typing import Any
from typing import Union

from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendCallable
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext


NatsClient = Union[Client, JetStreamContext]


class _Receive:
    def __init__(self, msg: Msg) -> None:
        self._msg = msg

    async def __call__(self) -> MessageReceiveEvent:
        return {
            "type": "message.receive",
            "id": self._msg.reply or "",
            "headers": [
                (key.encode(), value.encode())
                for key, value in (self._msg.headers or {}).items()
            ],
            "payload": self._msg.data,
        }


async def _message_send(client: NatsClient, event: MessageSendEvent) -> None:
    await client.publish(
        event["address"],
        event.get("payload") or b"",
        headers={key.decode(): value.decode() for key, value in event["headers"]},
    )


class _BaseServer(ABC):
    def __init__(
        self, app: AMGIApplication, *subjects: str, servers: str | list[str]
    ) -> None:
        self._app = app
        self._subjects = subjects
        self._servers = servers

        self._stop_event = Event()
        self._state: dict[str, Any] = {}

    async def _client_serve(self, client: NatsClient) -> None:
        client_subscribe_cb = partial(self._subscribe_cb, client)
        subscriptions = await asyncio.gather(
            *(
                client.subscribe(subject, cb=client_subscribe_cb)
                for subject in self._subjects
            )
        )

        async with Lifespan(self._app, self._state):
            await self._stop_event.wait()

            for subscription in subscriptions:
                await subscription.unsubscribe()

    async def _subscribe_cb(self, client: NatsClient, msg: Msg) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": msg.subject,
            "state": self._state.copy(),
        }
        await self._app(scope, _Receive(msg), self._send(client, msg))

    def stop(self) -> None:
        self._stop_event.set()

    @abstractmethod
    def _send(self, client: NatsClient, msg: Msg) -> AMGISendCallable:
        raise NotImplementedError  # pragma: no cover
