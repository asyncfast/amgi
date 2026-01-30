# amgi-aiobotocore

amgi-aiobotocore is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server project, currently supporting
running AMGI applications against [SQS](https://aws.amazon.com/sqs/).

## Installation

```
pip install amgi-aiobotocore==0.32.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_aiobotocore.sqs import run
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
asyncfast run amgi-aiobotocore-sqs main:app order-queue
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
