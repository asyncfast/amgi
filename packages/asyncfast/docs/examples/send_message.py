from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message

app = AsyncFast()


@dataclass
class OutputMessage(Message, address="output_channel"):
    id: Annotated[int, Header()]
    payload: str


@app.channel("input_channel")
async def input_channel_handler() -> AsyncGenerator[OutputMessage, None]:
    yield OutputMessage(id=1, payload="Hello")
