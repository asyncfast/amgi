from collections.abc import AsyncGenerator
from typing import Any

from asyncfast import AsyncFast

app = AsyncFast()


@app.channel("input_channel")
async def input_channel_handler() -> AsyncGenerator[dict[str, Any], None]:
    yield {"address": "output_channel", "payload": b"Hello", "headers": [(b"Id", b"1")]}
