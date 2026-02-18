# mypy: ignore-errors


async def app(scope, receive, send):
    if scope["type"] == "message":
        try:
            headers = scope["headers"]
            payload = scope.get("payload")
            bindings = scope.get("bindings", {})
            ...  # Do some message handling here!
            await send(
                {
                    "type": "message.ack",
                }
            )
        except Exception as e:
            await send(
                {
                    "type": "message.nack",
                    "message": str(e),
                }
            )
    else:
        pass  # Handle other types
