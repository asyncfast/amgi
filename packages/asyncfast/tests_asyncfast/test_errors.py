from typing import Any

import pytest
from asyncfast import AsyncFast
from asyncfast import InvalidChannelDefinitionError
from asyncfast import MessageSender


async def test_only_one_payload() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("topic")
        async def topic_handler(payload1: int, payload2: int) -> None:
            pass  # pragma: no cover


async def test_only_one_message_sender() -> None:
    app = AsyncFast()

    with pytest.raises(InvalidChannelDefinitionError):

        @app.channel("topic")
        async def topic_handler(
            message_sender1: MessageSender[dict[str, Any]],
            message_sender2: MessageSender[dict[str, Any]],
        ) -> None:
            pass  # pragma: no cover
