from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageAckEvent
from amgi_types import MessageNackEvent
from amgi_types import Scope


class ServerErrorMiddleware:
    def __init__(self, app: AMGIApplication) -> None:
        self.app = app

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] != "message":
            await self.app(scope, receive, send)
            return

        try:
            await self.app(scope, receive, send)

            message_ack_event: MessageAckEvent = {
                "type": "message.ack",
            }
            await send(message_ack_event)
        except Exception as e:
            message_nack_event: MessageNackEvent = {
                "type": "message.nack",
                "message": str(e),
            }
            await send(message_nack_event)
