# amgi-sqs-event-source-mapping

amgi-sqs-event-source-mapping is an adaptor for [AMGI](https://amgi.readthedocs.io/en/latest/) to run in a SQS event
source mapped Lambda.

## Installation

```
pip install amgi-sqs-event-source-mapping==0.21.0
```

## Example

```python
from dataclasses import dataclass

from amgi_sqs_event_source_mapping import SqsHandler
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-queue")
async def order_queue(order: Order) -> None:
    # Makes an order
    ...


handler = SqsHandler(app)
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
