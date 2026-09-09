from typing import Annotated

from asyncfast import AsyncFast
from asyncfast_avro import AvroPayload
from pydantic import BaseModel

app = AsyncFast()


class Order(BaseModel):
    id: str
    skus: list[str]


@app.channel("order")
async def handle_order(order: Annotated[Order, AvroPayload()]) -> None:
    print(order)
