import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import nullcontext
from unittest.mock import AsyncMock

import httpx
import pytest
from amgi_cloudevents import Server
from httpx import ASGITransport
from httpx import AsyncClient
from test_utils import MockApp


class _LifespanServer:
    def __init__(self, server: Server) -> None:
        self._server = server
        self._stop_event = asyncio.Event()

    async def serve(self) -> None:
        async with self._server.router.lifespan_context(self._server):
            await self._stop_event.wait()

    def stop(self) -> None:
        self._stop_event.set()


@pytest.fixture
def app() -> MockApp:
    return MockApp()


@pytest.fixture
def server(app: MockApp) -> Server:
    return Server(app)


@pytest.fixture
def transport(server: Server) -> ASGITransport:
    return ASGITransport(app=server)


@pytest.fixture
async def client(transport: ASGITransport) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


async def _post_cloud_event(
    client: AsyncClient, headers: dict[str, str] | None = None
) -> httpx.Response:
    headers = headers or {}
    return await client.post(
        "/event",
        content=b"payload",
        headers={
            "ce-specversion": "1.0",
            "ce-id": "event-1",
            "ce-source": "/source",
            "ce-type": "com.example.event",
            "ce-subject": "subject-1",
            "ce-time": "2026-06-07T10:00:00Z",
            **headers,
        },
    )


async def test_route_message_shape(app: MockApp, client: AsyncClient) -> None:
    response_task = asyncio.create_task(_post_cloud_event(client))

    async with app.call() as (scope, receive, send):
        assert scope == {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": "com.example.event",
            "headers": [
                (b"specversion", b"1.0"),
                (b"id", b"event-1"),
                (b"source", b"/source"),
                (b"type", b"com.example.event"),
                (b"subject", b"subject-1"),
                (b"time", b"2026-06-07T10:00:00Z"),
            ],
            "payload": b"payload",
            "state": {},
        }

    response = await response_task

    assert response.status_code == 204


async def test_route_message_shape_includes_lifespan_state(
    app: MockApp, server: Server, client: AsyncClient
) -> None:
    async with app.lifespan({"item": "value"}, _LifespanServer(server)):
        response_task = asyncio.create_task(_post_cloud_event(client))

        async with app.call() as (scope, receive, send):
            assert scope["state"] == {"item": "value"}

        response = await response_task

        assert response.status_code == 204


async def test_route_returns_bad_request_for_invalid_cloud_event(
    client: AsyncClient,
) -> None:
    response = await client.post("/event", content=b"payload")

    assert response.status_code == 400


async def test_route_returns_bad_request_for_non_string_cloud_event_type(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/event",
        json={
            "specversion": "1.0",
            "id": "event-1",
            "source": "/source",
            "type": 1,
        },
        headers={"content-type": "application/cloudevents+json"},
    )

    assert response.status_code == 400
    assert response.text == "CloudEvent type attribute must be a string"


async def test_route_receive_raises(app: MockApp, client: AsyncClient) -> None:
    response_task = asyncio.create_task(_post_cloud_event(client))

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()

    response = await response_task
    assert response.status_code == 204


async def test_route_returns_no_content_when_app_acks(
    app: MockApp, client: AsyncClient
) -> None:
    response_task = asyncio.create_task(_post_cloud_event(client))

    async with app.call() as (scope, receive, send):
        await send({"type": "message.ack"})

    response = await response_task

    assert response.status_code == 204


async def test_route_returns_server_error_when_app_nacks(
    app: MockApp, client: AsyncClient
) -> None:
    response_task = asyncio.create_task(_post_cloud_event(client))

    async with app.call() as (scope, receive, send):
        await send({"type": "message.nack", "message": "rejected"})

    response = await response_task

    assert response.status_code == 500
    assert response.text == "rejected"


async def test_route_errors_when_message_send_is_not_configured(
    app: MockApp, client: AsyncClient
) -> None:
    response_task = asyncio.create_task(_post_cloud_event(client))

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="message.send is not configured"):
            await send(
                {
                    "type": "message.send",
                    "address": "com.example.output",
                    "headers": [],
                    "payload": b"output",
                }
            )

    response = await response_task
    assert response.status_code == 204


async def test_route_logs_missing_message_send_when_configured(
    app: MockApp, caplog: pytest.LogCaptureFixture
) -> None:
    transport = ASGITransport(app=Server(app, message_send_log_missing=True))
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response_task = asyncio.create_task(_post_cloud_event(client))

        with caplog.at_level(logging.WARNING, logger="amgi-cloudevents.error"):
            async with app.call() as (scope, receive, send):
                await send(
                    {
                        "type": "message.send",
                        "address": "com.example.output",
                        "headers": [],
                        "payload": b"output",
                    }
                )

        response = await response_task

    assert response.status_code == 204
    assert caplog.messages == [
        "CloudEvents message.send is not configured. Pass message_send to Server "
        "to allow AMGI handlers to send follow-up messages."
    ]


async def test_route_uses_configured_message_send(app: MockApp) -> None:
    message_send = AsyncMock()
    transport = ASGITransport(app=Server(app, message_send=nullcontext(message_send)))
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response_task = asyncio.create_task(_post_cloud_event(client))

        async with app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": "com.example.output",
                    "headers": [(b"id", b"output-1")],
                    "payload": b"output",
                }
            )

        response = await response_task

    assert response.status_code == 204
    message_send.assert_awaited_once_with(
        {
            "type": "message.send",
            "address": "com.example.output",
            "headers": [(b"id", b"output-1")],
            "payload": b"output",
        }
    )


async def test_route_uses_address_attribute(app: MockApp) -> None:
    transport = ASGITransport(app=Server(app, address_attribute="eventtype"))

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response_task = asyncio.create_task(
            _post_cloud_event(client, {"ce-eventtype": "com.example.eventtype"})
        )

        async with app.call() as (scope, receive, send):
            assert scope == {
                "address": "com.example.eventtype",
                "amgi": {"spec_version": "2.0", "version": "2.0"},
                "headers": [
                    (b"specversion", b"1.0"),
                    (b"id", b"event-1"),
                    (b"source", b"/source"),
                    (b"type", b"com.example.event"),
                    (b"subject", b"subject-1"),
                    (b"time", b"2026-06-07T10:00:00Z"),
                    (b"eventtype", b"com.example.eventtype"),
                ],
                "payload": b"payload",
                "state": {},
                "type": "message",
            }

    response = await response_task

    assert response.status_code == 204


async def test_route_fails_if_cant_extract_address_attribute(app: MockApp) -> None:
    transport = ASGITransport(app=Server(app, address_attribute="eventtype"))

    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await _post_cloud_event(client)

    assert response.status_code == 400
