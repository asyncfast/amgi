from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from pydantic import BaseModel


class Payload(BaseModel):
    id: str


class State(BaseModel):
    state: str
    id: str


@dataclass
class OutputMessage(Message, address="output_channel"):
    id: Annotated[str, Header()]
    state: State


app = AsyncFast()


@app.channel("input_channel")
async def input_topic_handler(payload: Payload) -> AsyncGenerator[OutputMessage, None]:
    print("received message", payload)
    yield OutputMessage(id=payload.id, state=State(state="processed", id=payload.id))
