# mypy: ignore-errors


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                ...  # Do some startup here!
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                ...  # Do some shutdown here!
                await send({"type": "lifespan.shutdown.complete"})
                return
    else:
        pass  # Handle other types
