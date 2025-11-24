from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict
from dataclasses import dataclass
from typing import Annotated

import redis.asyncio as redis
from asyncfast import AsyncFast
from asyncfast.bindings import KafkaKey

client = redis.Redis()


@asynccontextmanager
async def redis_lifespan(app: AsyncFast) -> AsyncGenerator[None, None]:
    yield
    await client.aclose()


app = AsyncFast(lifespan=redis_lifespan)


@dataclass
class Item:
    sku_id: str
    amount: int


@dataclass
class Order:
    items: list[Item]
    status: str


@app.channel("order")
async def handle_order(order_id: Annotated[str, KafkaKey()], order: Order) -> None:
    await client.json().set(f"order:{order_id}", "$", asdict(order))
