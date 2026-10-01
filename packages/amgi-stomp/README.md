# amgi-stomp

amgi-stomp is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server to run AMGI applications against
[STOMP](https://stomp.github.io/) brokers using [stompman](https://pypi.org/project/stompman/).

amgi-stomp requires Python 3.11 or greater, as this is the minimum version supported by `stompman`.

## Installation

```
pip install amgi-stomp==0.47.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_stomp import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-queue")
async def order_queue(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order-queue")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-stomp main:app order-queue
```

Messages are consumed with `client-individual` acknowledgement mode. Sending `message.ack` or `message.nack` events
acknowledges or rejects the current STOMP message. Whether messages that are never acknowledged or are nacked are
redelivered, dead-lettered, or discarded depends on the broker's configuration.

If an application raises an exception while handling a message, the exception is logged and the message is nacked; what the
broker does with a nacked message (redeliver, dead-letter, or discard) is broker-specific.

The STOMP protocol metadata of a received message is available in the AMGI message scope:
`message["bindings"]["stomp"]["message_id"]` and `message["bindings"]["stomp"]["subscription"]`. When using AsyncFast,
these can be received directly with the `asyncfast.StompMessageId` and `asyncfast.StompSubscription` bindings.

By default, messages are sent without waiting for a broker receipt. Pass a `receipt_timeout` in seconds to `run`,
`Server`, or `MessageSend` to wait for a receipt confirming the broker processed the SEND frame, at the cost of a broker round-trip. A receipt does
not by itself make a message durable; persistence is broker-specific. Pass
`ssl=True` or an `ssl.SSLContext` via the `ssl` parameter of `run` or `Server` to connect over TLS. The `heartbeat`,
`connect_retry_attempts`, and `connect_retry_interval` parameters expose stompman's heartbeat and connection retry
options without changing its defaults.

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
