from collections.abc import AsyncGenerator
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Depends

app = AsyncFast()


async def get_resource() -> AsyncGenerator[str, None]:
    resource = "connected"
    try:
        yield resource
    finally:
        # Cleanup happens after the handler returns.
        pass


@app.channel("ping")
async def handle_ping(
    resource: Annotated[str, Depends(get_resource)],
) -> None:
    print(resource)
