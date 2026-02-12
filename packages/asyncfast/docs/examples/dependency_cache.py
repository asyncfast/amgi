from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Depends
from asyncfast import Header

app = AsyncFast()


def get_request_id(request_id: Annotated[str, Header(alias="request-id")]) -> str:
    return request_id


@app.channel("events")
async def handle_events(
    request_id: Annotated[str, Depends(get_request_id, use_cache=False)],
    request_id_again: Annotated[str, Depends(get_request_id, use_cache=False)],
) -> None:
    print(request_id, request_id_again)
