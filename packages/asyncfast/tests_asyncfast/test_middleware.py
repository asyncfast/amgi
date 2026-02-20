from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageScope
from amgi_types import Scope
from asyncfast import AsyncFast
from asyncfast import Middleware


class RecordingMiddleware:
    def __init__(self, app: AMGIApplication, mock: Mock) -> None:
        self._app = app
        self._mock = mock

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        self._mock.before()
        await self._app(scope, receive, send)
        self._mock.after()


async def test_middleware_with_init() -> None:
    parent = Mock()

    app = AsyncFast(middleware=[Middleware(RecordingMiddleware, parent.recorder)])

    @app.channel("channel")
    def handler() -> None:
        parent.handler()

    scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "channel",
        "headers": [],
    }
    await app(scope, AsyncMock(), AsyncMock())

    assert parent.mock_calls == [
        call.recorder.before(),
        call.handler(),
        call.recorder.after(),
    ]


async def test_middleware_with_add_middleware() -> None:
    parent = Mock()

    app = AsyncFast()
    app.add_middleware(RecordingMiddleware, parent.recorder)

    @app.channel("channel")
    def handler() -> None:
        parent.handler()

    scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "channel",
        "headers": [],
    }
    await app(scope, AsyncMock(), AsyncMock())

    assert parent.mock_calls == [
        call.recorder.before(),
        call.handler(),
        call.recorder.after(),
    ]


async def test_multiple_middleware_order() -> None:
    parent = Mock()

    app = AsyncFast()
    app.add_middleware(RecordingMiddleware, parent.first)
    app.add_middleware(RecordingMiddleware, parent.second)

    @app.channel("channel")
    def handler() -> None:
        parent.handler()

    scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "channel",
        "headers": [],
    }
    await app(scope, AsyncMock(), AsyncMock())

    assert parent.mock_calls == [
        call.second.before(),
        call.first.before(),
        call.handler(),
        call.first.after(),
        call.second.after(),
    ]
