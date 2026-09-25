# amgi-pulsar

amgi-pulsar is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server project, supporting running AMGI
applications against [Apache Pulsar](https://pulsar.apache.org/) topics.

The server consumes messages from a shared subscription, which is created if it does not exist. Messages are only
acknowledged when the application sends a `message.ack` event; a `message.nack` event triggers a redelivery.

## Installation

```
pip install amgi-pulsar==0.47.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_pulsar import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order.topic")
async def order_topic(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order.topic")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-pulsar main:app order.topic
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
