from unittest.mock import AsyncMock
from unittest.mock import Mock

from asyncfast import AsyncFast
from pydantic import BaseModel


async def test_message() -> None:
    app = AsyncFast()

    class Payload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("test")
    async def topic_handler(payload: Payload) -> None:
        test_mock(payload)

    await app(
        {
            "type": "message",
            "acgi": {"version": "1.0", "spec_version": "1.0"},
            "address": "test",
            "headers": [],
            "payload": b'{"id":1}',
        },
        AsyncMock(),
        AsyncMock(),
    )

    test_mock.assert_called_once_with(Payload(id=1))
