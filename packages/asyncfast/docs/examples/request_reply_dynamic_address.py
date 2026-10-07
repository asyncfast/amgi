from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import Reply
from asyncfast import ReplyAddress

app = AsyncFast()


@dataclass
class Pong(Message, address="{reply_to}"):
    reply_to: str
    payload: str


@app.channel(
    "ping",
    reply=Reply(
        address=ReplyAddress(
            "$message.header#/replyTo",
            description=(
                "The response destination is dynamically set according to the "
                "replyTo field in the request header"
            ),
        )
    ),
)
async def ping(
    payload: str,
    reply_to: Annotated[str, Header(alias="replyTo")],
) -> AsyncGenerator[Pong, None]:
    yield Pong(reply_to=reply_to, payload="pong")
