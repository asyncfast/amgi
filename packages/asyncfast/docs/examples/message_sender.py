from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import MessageSender

app = AsyncFast()


@dataclass
class OutputMessage(Message, address="output_channel"):
    id: Annotated[int, Header()]
    payload: str


@app.channel("input_channel")
async def input_channel_handler(message_sender: MessageSender[OutputMessage]) -> None:
    await message_sender.send(OutputMessage(id=1, payload="Hello"))
