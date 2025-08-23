from typing import Any
from typing import AsyncGenerator
from typing import Dict

from asyncfast import AsyncFast

app = AsyncFast()


@app.channel("input_channel")
async def input_channel_handler() -> AsyncGenerator[Dict[str, Any], None]:
    yield {"address": "output_channel", "payload": b"Hello", "headers": [(b"Id", b"1")]}
