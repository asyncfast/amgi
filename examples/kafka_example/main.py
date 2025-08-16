import json
import os
from dataclasses import dataclass
from typing import AsyncGenerator
from typing import Iterable
from typing import Tuple

import amgi_aiokafka
from asyncfast import AsyncFast
from pydantic import BaseModel


class Payload(BaseModel):
    id: str


@dataclass
class Message:
    address: str
    headers: Iterable[Tuple[bytes, bytes]]
    payload: bytes


app = AsyncFast()


@app.channel("input_topic")
async def input_topic_handler(payload: Payload) -> AsyncGenerator[Message, None]:
    print("received message", payload)
    yield Message(
        address="output_topic",
        headers=[(b"id", str(payload.id).encode())],
        payload=json.dumps({"state": "processed", "id": payload.id}).encode(),
    )


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    amgi_aiokafka.run(app, "input_topic", bootstrap_servers=bootstrap_servers)
