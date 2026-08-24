from typing import cast

from amgi_nats.pull import Server
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.errors import TimeoutError
from nats.js.client import JetStreamContext
from test_utils import MockApp


class _TimeoutOnceSubscription:
    def __init__(self, message: Msg) -> None:
        self._message = message
        self.fetches = 0

    async def fetch(self, batch: int = 1, timeout: float | None = 5) -> list[Msg]:
        self.fetches += 1
        if self.fetches == 1:
            raise TimeoutError
        return [self._message]


async def test_next_message_retries_after_timeout() -> None:
    server = Server(MockApp(), "subject", servers="nats://localhost:4222")
    message = Msg(cast(Client, None), subject="subject", data=b"value")
    subscription = _TimeoutOnceSubscription(message)

    fetched = await server._next_message(
        cast(JetStreamContext.PullSubscription, subscription)
    )

    assert fetched is message
    assert subscription.fetches == 2
