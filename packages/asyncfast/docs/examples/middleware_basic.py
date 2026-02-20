from time import monotonic

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import Scope
from asyncfast import AsyncFast


class TimingMiddleware:
    def __init__(self, app: AMGIApplication) -> None:
        self._app = app

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        start = monotonic()
        await self._app(scope, receive, send)
        duration_ms = (monotonic() - start) * 1000
        print(f"{scope['type']} handled in {duration_ms:.2f}ms")


app = AsyncFast()
app.add_middleware(TimingMiddleware)


@app.channel("orders")
async def handle_order(order_id: int) -> None:
    print(f"processing order {order_id}")
