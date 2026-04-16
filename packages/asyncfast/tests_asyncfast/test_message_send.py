from unittest.mock import AsyncMock

from asyncfast.message_send import MessageSendRouter


async def test_message_send_router_default() -> None:
    mock_default_message_send_manager = AsyncMock()
    mock_default_message_send = (
        mock_default_message_send_manager.__aenter__.return_value
    )

    message_send_router = MessageSendRouter(default=mock_default_message_send_manager)

    async with message_send_router as message_send:
        await message_send(
            {
                "type": "message.send",
                "address": "address",
                "headers": [],
                "payload": b"test",
            }
        )

    mock_default_message_send.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "address",
            "headers": [],
            "payload": b"test",
        }
    )


async def test_message_send_router() -> None:
    mock_route_message_send_manager = AsyncMock()
    mock_route_message_send = mock_route_message_send_manager.__aenter__.return_value

    message_send_router = MessageSendRouter()

    message_send_router.add_route("channel.{id}", mock_route_message_send_manager)

    async with message_send_router as message_send:
        await message_send(
            {
                "type": "message.send",
                "address": "channel.de320b42-2a98-11f1-badd-db379e7beed5",
                "headers": [],
                "payload": b"test",
            }
        )

    mock_route_message_send.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "channel.de320b42-2a98-11f1-badd-db379e7beed5",
            "headers": [],
            "payload": b"test",
        }
    )
