from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Message
from asyncfast import Reply

app = AsyncFast()


@dataclass
class Ping:
    value: str


@dataclass
class Pong(Message, address="pong"):
    value: str


@app.channel("ping")
async def ping(payload: Ping) -> Annotated[Pong, Reply()]:
    return Pong(value=payload.value)
