from collections.abc import AsyncGenerator
from dataclasses import dataclass

from asyncfast import AsyncFast
from asyncfast import Message
from asyncfast import Reply

app = AsyncFast()


@dataclass
class Pong(Message, address="pong"):
    payload: str


@app.channel("ping", reply=Reply())
async def ping(payload: str) -> AsyncGenerator[Pong, None]:
    yield Pong(payload="pong")
