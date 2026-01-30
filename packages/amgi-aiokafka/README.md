# amgi-aiokafka

amgi-aiokafka is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server to run AMGI applications against
[Kafka](https://kafka.apache.org/).

## Installation

```
pip install amgi-aiokafka==0.32.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_aiokafka import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-topic")
async def order_topic(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order-topic")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-aiokafka main:app order-topic
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
