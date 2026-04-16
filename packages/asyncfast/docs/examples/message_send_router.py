import os
from dataclasses import dataclass
from typing import Annotated

from amgi_aiobotocore.sqs import MessageSend as SQSMessageSend
from amgi_aiokafka import MessageSend as KafkaMessageSend
from amgi_aiokafka import run
from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import MessageSender
from asyncfast.message_send import MessageSendRouter

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

app = AsyncFast()


@dataclass
class Item:
    sku_id: str
    amount: int


@dataclass
class Order:
    items: list[Item]
    status: str


@dataclass
class EmailProcessingOrder(Message, address="order-email-processing-order"):
    order_id: Annotated[str, Header()]
    order: Order


@dataclass
class CancelShipping(Message, address="cancel-shipping"):
    order_id: Annotated[str, Header()]


@app.channel("orders")
async def handle_order(
    order: Order,
    order_id: Annotated[str, Header()],
    message_sender: MessageSender[EmailProcessingOrder | CancelShipping],
) -> None:
    if order.status == "processing":
        await message_sender.send(EmailProcessingOrder(order_id=order_id, order=order))

    if order.status == "cancelled":
        await message_sender.send(CancelShipping(order_id=order_id))


message_send_router = MessageSendRouter()

message_send_router.add_route(
    "order-email-processing-order",
    KafkaMessageSend(bootstrap_servers=BOOTSTRAP_SERVERS),
)
message_send_router.add_route("cancel-shipping", SQSMessageSend())

if __name__ == "__main__":
    run(
        app,
        "orders",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        message_send=message_send_router,
    )
