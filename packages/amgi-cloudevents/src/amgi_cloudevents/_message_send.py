import sys
from types import TracebackType
from typing import Literal

import httpx
from amgi_types import MessageSendEvent
from cloudevents.v1.conversion import to_binary
from cloudevents.v1.conversion import to_structured
from cloudevents.v1.http import CloudEvent

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

ContentMode = Literal["binary", "structured"]


def _decode_headers(headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
    try:
        return {key.decode(): value.decode() for key, value in headers}
    except UnicodeDecodeError as exc:
        raise ValueError("CloudEvent attributes must be UTF-8 encoded") from exc


class MessageSend:
    def __init__(
        self,
        event_endpoint: str,
        *,
        source: str = "/amgi-cloudevents",
        content_mode: ContentMode = "structured",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._client = client or httpx.AsyncClient()
        self._close_client = client is None
        self._event_endpoint = event_endpoint
        self._source = source
        self._content_mode = content_mode

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._close_client:
            await self._client.aclose()

    async def __call__(self, event: MessageSendEvent) -> None:
        attributes = _decode_headers(list(event["headers"]))
        attributes["source"] = attributes.get("source", self._source)
        attributes["type"] = event["address"]

        cloud_event = CloudEvent(attributes, event.get("payload"))
        if self._content_mode == "binary":
            headers, body = to_binary(cloud_event)
        else:
            headers, body = to_structured(cloud_event)

        response = await self._client.post(
            self._event_endpoint, headers=headers, content=body
        )
        response.raise_for_status()
