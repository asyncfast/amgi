from __future__ import annotations

import json as json_module
import re
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import TypeAlias

import pytest
from amgi_common import Lifespan
from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveEvent
from amgi_types import AMGISendEvent
from amgi_types import AMGIVersions
from amgi_types import MessageScope
from amgi_types import MessageSendEvent

AMGI_CLIENT_SCOPE: AMGIVersions = {"version": "2.0", "spec_version": "2.0"}


JsonType: TypeAlias = (
    None | bool | int | float | str | Sequence["JsonType"] | Mapping[str, "JsonType"]
)

HeaderType: TypeAlias = (
    Sequence[tuple[str | bytes, str | bytes]] | Mapping[str, str | bytes]
)


def encode_bytes(data: str | bytes) -> bytes:
    if isinstance(data, str):
        return data.encode()
    return data


def encode_headers(headers: HeaderType) -> Sequence[tuple[bytes, bytes]]:
    header_items = headers.items() if isinstance(headers, Mapping) else headers
    return [(encode_bytes(key), encode_bytes(value)) for key, value in header_items]


class JsonMatcher:
    def __init__(self, expected: JsonType) -> None:
        self.expected = expected

    def __eq__(self, other: object) -> bool:
        return isinstance(other, bytes) and json_module.loads(other) == self.expected


class HeadersMatcher:
    def __init__(self, expected: HeaderType) -> None:
        self.expected = expected

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Sequence):
            return False

        actual = list(other)

        if isinstance(self.expected, Mapping):
            expected = [
                (encode_bytes(key), encode_bytes(value))
                for key, value in self.expected.items()
            ]
            return sorted(actual) == sorted(expected)

        expected = [
            (encode_bytes(key), encode_bytes(value)) for key, value in self.expected
        ]
        return actual == expected


class Message:
    def __init__(
        self,
        address: str,
        headers: HeaderType = (),
        payload: str | bytes | None = None,
        json: JsonType = None,
        bindings: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._address = address
        self._header_matcher = HeadersMatcher(headers)
        self._bindings = {} if bindings is None else bindings

        payload_matcher: bytes | JsonMatcher | None = None
        if payload is not None:
            payload_matcher = encode_bytes(payload)
        elif json is not None:
            payload_matcher = JsonMatcher(json)

        self._payload_matcher = payload_matcher

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Mapping):
            return False

        message_send = {
            **other,
            "payload": other.get("payload"),
            "bindings": other.get("bindings", {}),
        }

        expected_message_send = {
            "type": "message.send",
            "address": self._address,
            "headers": self._header_matcher,
            "payload": self._payload_matcher,
            "bindings": self._bindings,
        }

        return message_send == expected_message_send


@dataclass(frozen=True)
class AMGIMessageResult:
    _events: Sequence[AMGISendEvent]

    @property
    def acked(self) -> bool:
        return any(event["type"] == "message.ack" for event in self._events)

    @property
    def nack_message(self) -> str | None:
        for event in self._events:
            if event["type"] == "message.nack":
                return event["message"]
        return None

    @property
    def message_sends(self) -> Sequence[MessageSendEvent]:
        return [event for event in self._events if event["type"] == "message.send"]

    def assert_acked(self) -> None:
        assert self.acked

    def assert_nacked(self, *, match: str | None = None) -> None:
        assert self.nack_message is not None
        if match is not None:

            assert re.search(match, self.nack_message) is not None

    def assert_has_message_sends(self, message_sends: Sequence[Message]) -> None:
        actual_message_sends = self.message_sends

        assert len(actual_message_sends) == len(message_sends)
        assert all(
            expected == actual
            for actual, expected in zip(actual_message_sends, message_sends)
        )

    def assert_has_message_send(
        self,
        address: str,
        headers: HeaderType = (),
        payload: str | bytes | None = None,
        json: JsonType = None,
        bindings: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        expected_message = Message(address, headers, payload, json, bindings)

        assert any(message == expected_message for message in self.message_sends)


class AMGIProducer:
    def __init__(
        self,
        app: AMGIApplication,
        *,
        state: dict[str, Any],
    ) -> None:
        self._app = app
        self._state = state

    async def send(
        self,
        address: str,
        *,
        payload: str | bytes | None = None,
        json: JsonType = None,
        headers: HeaderType | None = None,
        bindings: dict[str, dict[str, Any]] | None = None,
        extensions: dict[str, dict[str, Any]] | None = None,
    ) -> AMGIMessageResult:
        scope: MessageScope = {
            "type": "message",
            "amgi": AMGI_CLIENT_SCOPE,
            "address": address,
            "headers": [] if headers is None else encode_headers(headers),
            "state": self._state.copy(),
        }
        if json is not None:
            payload = json_module.dumps(json)
        if payload is not None:
            scope["payload"] = encode_bytes(payload)
        if bindings is not None:
            scope["bindings"] = bindings
        if extensions is not None:
            scope["extensions"] = extensions

        events: list[AMGISendEvent] = []

        async def receive() -> AMGIReceiveEvent:
            raise RuntimeError("Receive should not be called for message scopes")

        async def send(event: AMGISendEvent) -> None:
            events.append(event)

        await self._app(scope, receive, send)
        return AMGIMessageResult(events)


AMGIProducerFactory = Callable[
    [AMGIApplication],
    Awaitable[AMGIProducer],
]


@pytest.fixture
async def amgi_producer() -> AsyncGenerator[AMGIProducerFactory, None]:
    async with AsyncExitStack() as exit_stack:

        async def factory(app: AMGIApplication) -> AMGIProducer:
            state = await exit_stack.enter_async_context(Lifespan(app))
            return AMGIProducer(app, state=state)

        yield factory
