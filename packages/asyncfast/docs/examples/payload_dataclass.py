from dataclasses import dataclass

from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    id: str
    skus: list[str]


@app.channel("order")
async def handle_order(order: Order) -> None:
    print(order)
