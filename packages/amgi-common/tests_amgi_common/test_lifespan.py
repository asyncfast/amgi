import asyncio
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from amgi_common import Lifespan
from amgi_common import LifespanFailureError
from test_utils import MockApp


async def _lifespan_coroutine(lifespan: Lifespan) -> None:
    async with lifespan:
        pass


async def test_lifespan() -> None:
    app = MockApp()

    loop = asyncio.get_event_loop()

    lifespan = Lifespan(app)

    aenter_task = loop.create_task(lifespan.__aenter__())

    async with app.call() as (scope, receive, send):
        assert scope == {
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "state": {},
            "type": "lifespan",
        }
        mock_item = Mock()
        scope["state"]["item"] = mock_item

        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})

        state = await aenter_task
        assert state == {"item": mock_item}

        aexit_task = loop.create_task(lifespan.__aexit__(None, None, None))

        assert await receive() == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.complete"})
        await aexit_task


async def test_lifespan_should_use_supplied_state() -> None:
    state: dict[str, Any] = {}
    app = MockApp()

    loop = asyncio.get_event_loop()

    lifespan = Lifespan(app, state)

    aenter_task = loop.create_task(lifespan.__aenter__())

    async with app.call() as (scope, receive, send):
        assert scope == {
            "amgi": {"spec_version": "1.0", "version": "1.0"},
            "state": {},
            "type": "lifespan",
        }
        assert id(scope["state"]) == id(state)

        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})

        await aenter_task

        aexit_task = loop.create_task(lifespan.__aexit__(None, None, None))

        assert await receive() == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.complete"})
        await aexit_task


async def test_lifespan_startup_failed() -> None:
    app = MockApp()

    loop = asyncio.get_event_loop()

    lifespan = Lifespan(app)

    lifespan_task = loop.create_task(_lifespan_coroutine(lifespan))

    async with app.call() as (scope, receive, send):
        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.failed", "message": "Startup failed"})

        with pytest.raises(LifespanFailureError, match="Startup failed"):
            await lifespan_task


async def test_lifespan_shutdown_failed() -> None:
    app = MockApp()

    loop = asyncio.get_event_loop()

    lifespan = Lifespan(app)

    lifespan_task = loop.create_task(_lifespan_coroutine(lifespan))

    async with app.call() as (scope, receive, send):
        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})

        assert await receive() == {"type": "lifespan.shutdown"}
        await send({"type": "lifespan.shutdown.failed", "message": "Shutdown failed"})

        with pytest.raises(LifespanFailureError, match="Shutdown failed"):
            await lifespan_task


async def test_lifespan_app_doesnt_support_lifespan() -> None:
    app = AsyncMock(side_effect=Exception("Doesnt support lifespan"))

    async with Lifespan(app):
        pass
