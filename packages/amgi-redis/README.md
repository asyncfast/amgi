# amgi-redis

amgi-redis is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server to run AMGI applications against
[Redis](https://redis.io/).

## Installation

```
pip install amgi-redis==0.32.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_redis import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-channel")
async def order_channel(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order-channel")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-redis main:app order-channel
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
