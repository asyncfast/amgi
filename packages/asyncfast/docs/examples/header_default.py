from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header

app = AsyncFast()


@app.channel("notification_channel")
async def notification_channel_handler(
    request_id: Annotated[int, Header()] = 0,
) -> None:
    print(f"request_id: {request_id}")
