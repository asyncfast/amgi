import asyncio

import pytest
from amgi_pulsar import MessageSend


class _FakeClient:
    def __init__(self) -> None:
        self.created_topics: list[str] = []

    async def create_producer(self, topic: str) -> object:
        self.created_topics.append(topic)
        await asyncio.sleep(0)
        return object()


async def test_message_send_requires_context_manager() -> None:
    message_send = MessageSend("pulsar://localhost:6650")

    with pytest.raises(RuntimeError, match="MessageSend not initialized"):
        await message_send({"type": "message.send", "address": "topic", "headers": []})


async def test_message_send_creates_one_producer_per_topic() -> None:
    message_send = MessageSend("pulsar://localhost:6650")
    fake_client = _FakeClient()
    message_send._client = fake_client

    first, second = await asyncio.gather(
        message_send._get_producer("topic"),
        message_send._get_producer("topic"),
    )
    third = await message_send._get_producer("topic")

    assert first is second is third
    assert fake_client.created_topics == ["topic"]
