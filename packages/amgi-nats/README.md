# amgi-nats

amgi-nats is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server project, supporting running AMGI
applications against [NATS](https://nats.io/) using either push or pull subscriptions.

- Push subscriptions use core NATS messaging. See [amgi_nats.push](src/amgi_nats/push.py).
- Pull subscriptions use [JetStream](https://docs.nats.io/nats-concepts/jetstream) consumers.
  See [amgi_nats.pull](src/amgi_nats/pull.py).

## Installation

```
pip install amgi-nats==0.46.0
```

## Push Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_nats.push import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order.subject")
async def order_subject(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order.subject")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-nats-push main:app order.subject
```

## Pull Example

The pull server is run the same way, but consumes from a JetStream pull consumer. The stream and consumer are created
if they do not exist:

```python
from dataclasses import dataclass

from amgi_nats.pull import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order.subject")
async def order_subject(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order.subject")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-nats-pull main:app order.subject
```

Messages sent by the application are published through JetStream and wait for the stream acknowledgement, so a send
that no stream accepts raises `nats.js.errors.NoStreamResponseError`. The `nats` reply binding is not supported for
these sends, as the reply subject carries the acknowledgement. Push server sends use core NATS publishing, which is
fire and forget.

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
