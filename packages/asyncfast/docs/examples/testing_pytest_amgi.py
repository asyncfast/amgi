from pytest_amgi import AMGIProducerFactory
from pytest_amgi import Message

from .testing_app import app


async def test_message(amgi_producer: AMGIProducerFactory) -> None:
    producer = await amgi_producer(app)
    result = await producer.send("orders", json=1)

    result.assert_acked()


async def test_message_header(amgi_producer: AMGIProducerFactory) -> None:
    producer = await amgi_producer(app)
    result = await producer.send("orders.with-header", headers={"trace-id": "trace-1"})

    result.assert_acked()


async def test_message_send(amgi_producer: AMGIProducerFactory) -> None:
    producer = await amgi_producer(app)
    result = await producer.send("orders.created")

    result.assert_acked()
    result.assert_has_message_send("orders.process", json={"id": 1})


async def test_multiple_message_sends(amgi_producer: AMGIProducerFactory) -> None:
    producer = await amgi_producer(app)
    result = await producer.send("orders.created.multiple")

    result.assert_acked()
    result.assert_has_message_sends(
        [
            Message("inventory.reserve", json={"id": 1}),
            Message("email.receipt", json={"id": 1}),
        ]
    )


async def test_message_nack(amgi_producer: AMGIProducerFactory) -> None:
    producer = await amgi_producer(app)
    result = await producer.send("orders.invalid")

    result.assert_nacked(match="invalid order")
