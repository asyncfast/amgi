# amgi-cloudevents

CloudEvents HTTP adapter for AMGI applications.

## Installation

```shell
pip install "amgi-cloudevents[uvicorn]==0.39.0"
```

## Receiving CloudEvents

```python
from amgi_cloudevents import run
from asyncfast import AsyncFast

app = AsyncFast()


@app.channel("com.example.event")
async def handle_event(payload: bytes) -> None: ...


if __name__ == "__main__":
    run(app, host="0.0.0.0", port=8000)
```

You can also run an app through the AsyncFast CLI entry point:

```shell
asyncfast run amgi-cloudevents-uvicorn main:app --host 0.0.0.0 --port 8000
```

If the app sends follow-up messages, provide an outbound CloudEvents endpoint:

```shell
asyncfast run amgi-cloudevents-uvicorn main:app \
  --message-send-endpoint https://example.com/events \
  --message-send-source /orders \
  --message-send-content-mode binary
```

`Server` accepts binary or structured CloudEvents on `POST /event` by default.
CloudEvent attributes and extensions are exposed as AMGI headers. For example,
`ce-id: event-1` becomes `(b"id", b"event-1")`.

AMGI `message.ack` returns `204 No Content`. AMGI `message.nack` returns `500`
with the nack message as the response body. Invalid CloudEvents return `400`.

## Sending CloudEvents

```python
from amgi_cloudevents import MessageSend
from amgi_cloudevents import Server

message_send = MessageSend(
    "https://example.com/events",
    source="/orders",
    content_mode="binary",
)
server = Server(app, message_send=message_send)
```

`MessageSend` maps AMGI `message.send` events to CloudEvents:

- `event["address"]` becomes the CloudEvent `type`
- `event["payload"]` becomes the CloudEvent data
- `event["headers"]` become CloudEvent attributes/extensions
- `source` defaults to `/amgi-cloudevents`, unless the AMGI headers include
  `source`

`content_mode` can be `"structured"` or `"binary"`. Structured mode is the
default.

If `Server` receives an AMGI `message.send` event without a configured
`message_send`, it raises an error explaining that outbound sending must be
wired explicitly.
