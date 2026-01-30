# amgi-sqs-event-source-mapping

amgi-sqs-event-source-mapping is an adaptor for [AMGI](https://amgi.readthedocs.io/en/latest/) applications to run in an
SQS event source mapped Lambda.

## Installation

```
pip install amgi-sqs-event-source-mapping==0.32.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_sqs_event_source_mapping import SqsEventSourceMappingHandler
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-queue")
async def order_queue(order: Order) -> None:
    # Makes an order
    ...


handler = SqsEventSourceMappingHandler(app)
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
