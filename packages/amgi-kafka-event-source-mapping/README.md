# amgi-kafka-event-source-mapping

amgi-kafka-event-source-mapping is an adaptor for [AMGI](https://amgi.readthedocs.io/en/latest/) applications to run in
a Kafka event source mapped environment.

## Installation

```bash
pip install amgi-kafka-event-source-mapping==0.33.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_kafka_event_source_mapping import KafkaEventSourceMappingHandler
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("orders")
async def orders(order: Order) -> None:
    # Makes an order
    ...


handler = KafkaEventSourceMappingHandler(app)
```

## What it does

- Converts Kafka batch events into AMGI `message.receive` events
- Uses the Kafka topic name as the AMGI message address
- Supports partial batch failures so only failed records are reported
- Sends outbound messages to Kafka using an async producer
- Outbound messages are sent via the same Kafka broker (bootstrap servers) that the records were received from
- Optionally manages application startup and shutdown via AMGI lifespan

## Record handling

- Record values and keys are passed to your app as bytes
- Kafka record headers become AMGI headers
- Records are only acknowledged when your app emits `message.ack`
- Records that emit `message.nack` or are not acknowledged are treated as failures

## Nack handling

By default, records that are negatively acknowledged, or not acknowledged are logged:

```python
handler = KafkaEventSourceMappingHandler(app, on_nack="log")
```

To fail the invocation when any record is nacked, configure the handler to raise an error instead:

```python
handler = KafkaEventSourceMappingHandler(app, on_nack="error")
```

This is useful when running in environments where a failed invocation should trigger a retry, or alert.

When using this mode, handlers **must be idempotent**. Kafka event source mappings may re-deliver records after
failures, restarts, or rebalances, and your application logic should be safe to execute more than once for the same
record.

## Lifespan

Lifespan support is enabled by default.

- Startup runs once per Lambda execution environment
- Shutdown is attempted when the environment is terminated

Shutdown handling relies on `signal.SIGTERM`, which is supported by Python 3.12 and later Lambda runtimes.

To use fully stateless, per-invocation behavior, disable lifespan:

```python
handler = KafkaEventSourceMappingHandler(app, lifespan=False)
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2026 AMGI
