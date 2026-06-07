import json

import httpx
import pytest
from amgi_cloudevents import MessageSend
from amgi_cloudevents._message_send import ContentMode
from httpx import AsyncClient
from httpx import MockTransport
from httpx import Request
from httpx import Response


async def _send(
    *,
    headers: list[tuple[bytes, bytes]],
    content_mode: ContentMode = "structured",
) -> Request:
    requests: list[Request] = []

    async def handler(request: Request) -> Response:
        requests.append(request)
        return Response(202)

    async with AsyncClient(transport=MockTransport(handler)) as client:
        message_send = MessageSend(
            "http://testserver/events",
            source="/test-source",
            content_mode=content_mode,
            client=client,
        )

        await message_send(
            {
                "type": "message.send",
                "address": "com.example.event",
                "headers": headers,
                "payload": b"payload",
            }
        )

    assert len(requests) == 1
    return requests[0]


async def test_message_send_posts_structured_cloud_event() -> None:
    request = await _send(headers=[(b"id", b"event-1"), (b"subject", b"subject-1")])

    assert request.method == "POST"
    assert str(request.url) == "http://testserver/events"
    assert request.headers["content-type"] == "application/cloudevents+json"

    body = json.loads(request.content)
    assert body["id"] == "event-1"
    assert body["source"] == "/test-source"
    assert body["type"] == "com.example.event"
    assert body["subject"] == "subject-1"
    assert body["data_base64"] == "cGF5bG9hZA=="


async def test_message_send_allows_source_header_override() -> None:
    request = await _send(
        headers=[(b"source", b"/header-source"), (b"type", b"ignored")],
        content_mode="binary",
    )

    assert request.headers["ce-source"] == "/header-source"
    assert request.headers["ce-type"] == "com.example.event"


async def test_message_send_posts_binary_cloud_event() -> None:
    request = await _send(
        headers=[(b"id", b"event-1"), (b"subject", b"subject-1")],
        content_mode="binary",
    )

    assert request.method == "POST"
    assert str(request.url) == "http://testserver/events"
    assert request.headers["ce-id"] == "event-1"
    assert request.headers["ce-source"] == "/test-source"
    assert request.headers["ce-type"] == "com.example.event"
    assert request.headers["ce-subject"] == "subject-1"
    assert request.content == b"payload"


async def test_message_send_context_manager_closes_owned_client() -> None:
    async with MessageSend("http://testserver/events") as message_send:
        client = message_send._client
        assert not client.is_closed

    assert client.is_closed


async def test_message_send_raises_for_error_response() -> None:
    async def handler(request: Request) -> Response:
        return Response(500)

    async with AsyncClient(transport=MockTransport(handler)) as client:
        message_send = MessageSend("http://testserver/events", client=client)

        with pytest.raises(httpx.HTTPStatusError):
            await message_send(
                {
                    "type": "message.send",
                    "address": "com.example.event",
                    "headers": [],
                    "payload": b"payload",
                }
            )


async def test_message_send_requires_utf8_headers() -> None:
    async with AsyncClient(
        transport=MockTransport(lambda request: Response(202))
    ) as client:
        message_send = MessageSend("http://testserver/events", client=client)

        with pytest.raises(ValueError, match="UTF-8"):
            await message_send(
                {
                    "type": "message.send",
                    "address": "com.example.event",
                    "headers": [(b"id", b"\xff")],
                    "payload": b"payload",
                }
            )
