# pytest-amgi

Pytest helpers for driving AMGI applications in-process.

## Installation

```
pip install pytest-amgi==0.40.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from asyncfast import AsyncFast
from pytest_amgi import AMGIProducerFactory


async def test_message(amgi_producer: AMGIProducerFactory) -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def handler(payload: int) -> None:
        assert payload == 1

    producer = await amgi_producer(app)
    response = await producer.send("topic", json=1)

    response.assert_acked()
```

The `amgi_producer` fixture starts lifespan before returning the producer and
shuts it down when the test finishes.

## Message Sends

Use `assert_has_message_send` to assert that the application sent a follow-up
message:

```python
from dataclasses import dataclass

from asyncfast import AsyncFast
from asyncfast import Message
from asyncfast import MessageSender
from pytest_amgi import AMGIProducerFactory


@dataclass
class ProcessOrder(Message, address="order.process"):
    payload: dict[str, int]


async def test_message_send(amgi_producer: AMGIProducerFactory) -> None:
    app = AsyncFast()

    @app.channel("order.created")
    async def handle_order_created(
        message_sender: MessageSender[ProcessOrder],
    ) -> None:
        await message_sender.send(ProcessOrder(payload={"id": 1}))

    producer = await amgi_producer(app)
    response = await producer.send("order.created")

    response.assert_acked()
    response.assert_has_message_send("order.process", json={"id": 1})
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2026 AMGI
