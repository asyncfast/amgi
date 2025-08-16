from asyncfast import AsyncFast

app = AsyncFast()


@app.channel("register.{user_id}")
async def handle_register(user_id: str) -> None:
    print(user_id)
