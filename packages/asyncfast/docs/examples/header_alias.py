from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header

app = AsyncFast()


@app.channel("topic")
async def topic_handler(
    etag: Annotated[str, Header(alias="ETag")],
) -> None:
    print(etag)
