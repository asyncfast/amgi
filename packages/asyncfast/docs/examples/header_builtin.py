from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header

app = AsyncFast()


@app.channel("order")
async def handle_order(
    idempotency_key: Annotated[str, Header()],
) -> None:
    print(idempotency_key)
