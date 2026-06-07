from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any
from typing import AsyncContextManager

from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from amgi_types import MessageSendEvent
from cloudevents.v1.exceptions import GenericException as CloudEventException
from cloudevents.v1.http import from_http
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.status import HTTP_204_NO_CONTENT
from starlette.status import HTTP_400_BAD_REQUEST
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


class MissingMessageSend:
    async def __aenter__(self) -> _MessageSendT:
        return self._send

    async def _send(self, event: MessageSendEvent) -> None:
        raise RuntimeError(
            "CloudEvents message.send is not configured. Pass message_send to Server "
            "to allow AMGI handlers to send follow-up messages."
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


class Send:
    def __init__(self, message_send: _MessageSendT) -> None:
        self._message_send = message_send
        self.acknowledged = False
        self.nack_message: str | None = None

    async def __call__(self, event: AMGISendEvent) -> None:
        if event["type"] == "message.ack":
            self.acknowledged = True
        elif event["type"] == "message.nack":
            self.nack_message = event["message"]
        elif event["type"] == "message.send":
            await self._message_send(event)


async def receive() -> AMGIReceiveEvent:
    raise RuntimeError("Receive should not be called")


def bytes_unmarshaller(content: None | str | bytes) -> bytes | None:
    if content is None:
        return None
    if isinstance(content, bytes):
        return content
    return content.encode()


def _encode_header_value(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _encode_headers(attributes: Mapping[str, Any]) -> list[tuple[bytes, bytes]]:
    return [
        (name.encode(), _encode_header_value(value))
        for name, value in attributes.items()
    ]


class Server(Starlette):
    def __init__(
        self,
        app: AMGIApplication,
        path: str = "/event",
        message_send: _MessageSendManagerT | None = None,
    ) -> None:
        self._app = app
        self._state: dict[str, Any] = {}
        self._message_send_context = message_send or MissingMessageSend()
        self._message_send: _MessageSendT | None = None
        super().__init__(
            routes=[Route(path, self._route, methods=["POST"])], lifespan=self._lifespan
        )

    @asynccontextmanager
    async def _lifespan(self, _server: Starlette) -> AsyncIterator[None]:
        async with self._message_send_context as message_send:
            self._message_send = message_send
            async with Lifespan(self._app, self._state):
                yield
            self._message_send = None

    async def _call_app(self, scope: MessageScope) -> Send:
        if self._message_send is None:
            async with self._message_send_context as message_send:
                send = Send(message_send)
                await self._app(scope, receive, send)
                return send

        send = Send(self._message_send)
        await self._app(scope, receive, send)
        return send

    def _response(self, send: Send) -> Response:
        if send.nack_message is not None:
            return Response(
                send.nack_message, status_code=HTTP_500_INTERNAL_SERVER_ERROR
            )
        if send.acknowledged:
            return Response(status_code=HTTP_204_NO_CONTENT)
        return Response(status_code=HTTP_204_NO_CONTENT)

    async def _route(self, request: Request) -> Response:
        try:
            cloud_event = from_http(
                request.headers,
                await request.body(),
                data_unmarshaller=bytes_unmarshaller,
            )
        except CloudEventException as exc:
            return Response(str(exc), status_code=HTTP_400_BAD_REQUEST)

        attributes = cloud_event.get_attributes()
        address = attributes["type"]
        if not isinstance(address, str):
            return Response(
                "CloudEvent type attribute must be a string",
                status_code=HTTP_400_BAD_REQUEST,
            )

        scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": address,
            "headers": _encode_headers(attributes),
            "payload": cloud_event.get_data(),
            "state": self._state.copy(),
        }
        send = await self._call_app(scope)
        return self._response(send)
