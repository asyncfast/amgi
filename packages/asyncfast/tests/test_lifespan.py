from unittest.mock import Mock, AsyncMock, call

import pytest

from asyncfast import AsyncFast


@pytest.mark.asyncio
async def test_lifespan():
    app = AsyncFast()

    app.channel("test")(Mock())

    parent_mock = Mock()
    receive_mock = AsyncMock(
        side_effect=[{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    )
    send_mock = AsyncMock()
    parent_mock.attach_mock(receive_mock, "receive")
    parent_mock.attach_mock(send_mock, "send")

    await app(
        {"type": "lifespan", "acgi": {"version": "1.0", "spec_version": "1.0"}},
        receive_mock,
        send_mock,
    )

    parent_mock.assert_has_calls(
        [
            call.receive(),
            call.send(
                {
                    "type": "lifespan.startup.complete",
                    "subscriptions": [{"address": "test"}],
                }
            ),
            call.receive(),
            call.send({"type": "lifespan.shutdown.complete"}),
        ]
    )
