import asyncio
from asyncio import Event
from functools import partial
from typing import Any

import nats
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGISendEvent
from amgi_types import MessageReceiveEvent
from amgi_types import MessageScope
from nats.aio.client import Client
from nats.aio.msg import Msg


class _Receive:
    def __init__(self, msg: Msg) -> None:
        self._msg = msg

    async def __call__(self) -> MessageReceiveEvent:
        return {
            "type": "message.receive",
            "id": "",
            "headers": [
                (key.encode(), value.encode())
                for key, value in (self._msg.headers or {}).items()
            ],
            "payload": self._msg.data,
        }


class _Send:
    def __init__(self, client: Client, msg: Msg) -> None:
        self._client = client
        self._msg = msg

    def _check_reply(self) -> bool:
        return self._msg.reply is None or self._msg.reply == ""

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            if not self._check_reply():
                await self._msg.ack()
        if event["type"] == "message.nack":
            if not self._check_reply():
                await self._msg.nak()
        if event["type"] == "message.send":
            await self._client.publish(
                event["address"],
                event.get("payload") or b"",
                headers={
                    key.decode(): value.decode() for key, value in event["headers"]
                },
            )


class Server:
    def __init__(
        self, app: AMGIApplication, *subjects: str, servers: str | list[str]
    ) -> None:
        self._app = app
        self._subjects = subjects
        self._servers = servers

        self._stop_event = Event()
        self._state: dict[str, Any] = {}

    async def serve(self) -> None:
        client = await nats.connect(self._servers)

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

    async def _subscribe_cb(self, client: Client, msg: Msg) -> None:
        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "1.0", "spec_version": "1.0"},
            "address": msg.subject,
            "state": self._state.copy(),
        }
        await self._app(scope, _Receive(msg), _Send(client, msg))

    def stop(self) -> None:
        self._stop_event.set()
