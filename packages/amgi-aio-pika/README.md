# amgi-aio-pika

amgi-aio-pika is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server to run AMGI applications against
AMQP (RabbitMQ) using `aio-pika`.

## Installation

```
pip install amgi-aio-pika==0.44.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_aio_pika import run
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
asyncfast run amgi-aio-pika main:app order-queue
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
