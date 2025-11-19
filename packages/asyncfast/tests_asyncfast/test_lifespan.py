from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock

from amgi_types import LifespanScope
from asyncfast import AsyncFast


async def test_lifespan() -> None:
    app = AsyncFast()

    parent_mock = Mock()
    receive_mock = AsyncMock(
        side_effect=[{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    )
    send_mock = AsyncMock()
    parent_mock.attach_mock(receive_mock, "receive")
    parent_mock.attach_mock(send_mock, "send")

    lifespan_scope: LifespanScope = {
        "type": "lifespan",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
    }
    await app(
        lifespan_scope,
        receive_mock,
        send_mock,
    )

    parent_mock.assert_has_calls(
        [
            call.receive(),
            call.send({"type": "lifespan.startup.complete"}),
            call.receive(),
            call.send({"type": "lifespan.shutdown.complete"}),
        ]
    )


async def test_lifespan_context_manager() -> None:
    lifespan_mock = AsyncMock()
    app = AsyncFast(lifespan=lifespan_mock)

    parent_mock = Mock()
    receive_mock = AsyncMock(
        side_effect=[{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    )
    send_mock = AsyncMock()
    parent_mock.attach_mock(receive_mock, "receive")
    parent_mock.attach_mock(send_mock, "send")
    parent_mock.attach_mock(lifespan_mock, "lifespan")

    lifespan_scope: LifespanScope = {
        "type": "lifespan",
        "amgi": {"version": "1.0", "spec_version": "1.0"},
    }
    await app(
        lifespan_scope,
        receive_mock,
        send_mock,
    )

    parent_mock.assert_has_calls(
        [
            call.receive(),
            call.lifespan.__aenter__(),
            call.send({"type": "lifespan.startup.complete"}),
            call.receive(),
            call.lifespan.__aexit__(None, None, None),
            call.send({"type": "lifespan.shutdown.complete"}),
        ]
    )
