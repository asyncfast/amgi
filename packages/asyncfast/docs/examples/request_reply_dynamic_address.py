from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Message
from asyncfast import Reply
from asyncfast import ReplyTo

app = AsyncFast()


@dataclass
class Pong(Message, address="{reply_to}"):
    reply_to: str
    payload: str


@app.channel("ping", reply=Reply())
async def ping(
    payload: str,
    reply_to: Annotated[str, ReplyTo(alias="replyTo")],
) -> AsyncGenerator[Pong, None]:
    yield Pong(reply_to=reply_to, payload="pong")
