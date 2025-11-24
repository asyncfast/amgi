import asyncio
from typing import Any
from unittest.mock import Mock

from amgi_common import Lifespan
from test_utils import MockApp


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
