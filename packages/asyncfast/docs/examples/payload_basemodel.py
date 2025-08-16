from asyncfast import AsyncFast
from pydantic import BaseModel

app = AsyncFast()


class Payload(BaseModel):
    id: str
    name: str


@app.channel("channel")
async def handle_channel(payload: Payload) -> None:
    print(payload)
