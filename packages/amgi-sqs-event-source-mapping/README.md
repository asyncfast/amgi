# amgi-sqs-event-source-mapping

amgi-sqs-event-source-mapping is an adaptor for [AMGI](https://amgi.readthedocs.io/en/latest/) applications to run in an
SQS event source mapped Lambda.

## Installation

```
pip install amgi-sqs-event-source-mapping==0.36.0
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

## What it does

- Converts SQS batch events into AMGI `message.receive` events
- Uses the SQS queue name as the AMGI message address
- Supports partial batch failures so only failed messages are retried
- Sends outbound messages back to SQS efficiently using batching
- Optionally manages application startup and shutdown via AMGI lifespan
- Verifies message integrity using the SQS-provided MD5 checksum and retries corrupted messages

## Record handling

- Record bodies are passed to your app as bytes
- SQS record attributes become AMGI headers
- Records are only acknowledged when your app emits `message.ack`
- Records that are not acknowledged are treated as failures and will be retried
- Corrupted messages are detected automatically and retried

## Lifespan

Lifespan support is enabled by default.

- Startup runs once per Lambda execution environment
- Shutdown is attempted when the environment is terminated

Shutdown handling relies on `signal.SIGTERM`, which is supported by Python 3.12 and later Lambda runtimes.

To use fully stateless, per-invocation behavior, disable lifespan:

```python
handler = SqsEventSourceMappingHandler(app, lifespan=False)
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
