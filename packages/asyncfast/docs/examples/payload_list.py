from asyncfast import AsyncFast
from pydantic import BaseModel

app = AsyncFast()


class Item(BaseModel):
    id: str
    name: str


@app.channel("channel")
async def handle_channel(items: list[Item]) -> None:
    print(items)
