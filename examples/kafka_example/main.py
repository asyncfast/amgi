import os
from dataclasses import dataclass
from typing import Annotated
from typing import AsyncGenerator

import amgi_aiokafka
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
class OutputMessage(Message, address="output_topic"):
    id: Annotated[str, Header()]
    state: State


app = AsyncFast()


@app.channel("input_topic")
async def input_topic_handler(payload: Payload) -> AsyncGenerator[OutputMessage, None]:
    print("received message", payload)
    yield OutputMessage(id=payload.id, state=State(state="processed", id=payload.id))


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    amgi_aiokafka.run(app, "input_topic", bootstrap_servers=bootstrap_servers)
