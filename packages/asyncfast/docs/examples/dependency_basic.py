from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Depends
from asyncfast import Header

app = AsyncFast()


def get_context(
    request_id: Annotated[str, Header(alias="request-id")],
) -> dict[str, str]:
    return {"request_id": request_id}


@app.channel("orders.created")
async def handle_orders(
    context: Annotated[dict[str, str], Depends(get_context)],
) -> None:
    print(context["request_id"])
