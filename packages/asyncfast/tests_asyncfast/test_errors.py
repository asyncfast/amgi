import pytest
from asyncfast import AsyncFast
from asyncfast import InvalidChannelDefinitionError


async def test_only_one_payload() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("topic")
        async def topic_handler(payload1: int, payload2: int) -> None:
            pass  # pragma: no cover
