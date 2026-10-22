import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from amgi_common import Lifespan
from amgi_types import AMGIReceiveEvent
from test_utils import MockApp


@pytest.mark.parametrize("fails", [False, True])
async def test_lifespan_reports_server_exit_without_shutdown(fails: bool) -> None:
    app = MockApp()
    stopped = asyncio.Event()

    async def serve() -> None:
        receive_queue = asyncio.Queue[AMGIReceiveEvent]()
        receive_queue.put_nowait({"type": "lifespan.startup"})
        app_task = asyncio.create_task(
            app(
                {
                    "type": "lifespan",
                    "amgi": {"version": "2.0", "spec_version": "2.0"},
                    "state": {},
                },
                receive_queue.get,
                AsyncMock(),
            )
        )
        try:
            await stopped.wait()
            if fails:
                raise ValueError("Server failed")
        finally:
            app_task.cancel()
            with suppress(asyncio.CancelledError):
                await app_task

    server = Mock(serve=AsyncMock(side_effect=serve), stop=stopped.set)
    exception = ValueError if fails else RuntimeError
    message = (
        "Server failed" if fails else "Server exited without sending lifespan.shutdown"
    )
    with pytest.raises(exception, match=message):
        async with app.lifespan(server=server):
            pass
    server.serve.assert_awaited_once()


async def test_lifespan_shuts_down_server_when_test_fails() -> None:
    app = MockApp()
    stopped = asyncio.Event()
    finished = asyncio.Event()

    async def serve() -> None:
        async with Lifespan(app):
            await stopped.wait()
        finished.set()

    server = Mock(serve=AsyncMock(side_effect=serve), stop=stopped.set)
    with pytest.raises(ValueError, match="Test failed"):
        async with app.lifespan(server=server):
            raise ValueError("Test failed")
    assert finished.is_set()
