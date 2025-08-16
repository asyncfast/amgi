from asyncfast import AsyncFast

app = AsyncFast()


@app.channel("channel")
async def handle_channel(payload: int) -> None:
    print(payload)
