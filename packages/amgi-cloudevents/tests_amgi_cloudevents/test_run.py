from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import patch

from amgi_cloudevents import MessageSend
from amgi_cloudevents import Server
from amgi_cloudevents._run import run
from starlette.routing import Route


def test_run_uses_uvicorn() -> None:
    with patch("uvicorn.run") as mock_run:
        run(AsyncMock(), host="127.0.0.1", port=9000, path="/events")

    server = mock_run.call_args.args[0]
    assert isinstance(server, Server)
    route = server.routes[0]
    assert isinstance(route, Route)
    assert route.path == "/events"
    assert mock_run.mock_calls == [call(server, host="127.0.0.1", port=9000)]


def test_run_wires_message_send_when_endpoint_is_provided() -> None:
    with patch("uvicorn.run") as mock_run:
        run(
            AsyncMock(),
            message_send_endpoint="http://testserver/events",
            message_send_source="/test-source",
            message_send_content_mode="binary",
        )

    server = mock_run.call_args.args[0]
    assert isinstance(server, Server)
    assert isinstance(server._message_send_context, MessageSend)
    assert server._message_send_context._event_endpoint == "http://testserver/events"
    assert server._message_send_context._source == "/test-source"
    assert server._message_send_context._content_mode == "binary"
