from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import MessageSender

app = AsyncFast()


@app.channel("orders")
async def orders(payload: int) -> None:
    assert payload == 1


@app.channel("orders.with-header")
async def orders_with_header(trace_id: Annotated[str, Header()]) -> None:
    assert trace_id == "trace-1"


# process-order-start
@dataclass
class ProcessOrder(Message, address="orders.process"):
    payload: dict[str, int]


@app.channel("orders.created")
async def orders_created(
    message_sender: MessageSender[ProcessOrder],
) -> None:
    await message_sender.send(ProcessOrder(payload={"id": 1}))


# multiple-sends-start
@dataclass
class ReserveInventory(Message, address="inventory.reserve"):
    payload: dict[str, int]


@dataclass
class SendReceipt(Message, address="email.receipt"):
    payload: dict[str, int]


@app.channel("orders.created.multiple")
async def orders_created_multiple(
    message_sender: MessageSender[ReserveInventory | SendReceipt],
) -> None:
    await message_sender.send(ReserveInventory(payload={"id": 1}))
    await message_sender.send(SendReceipt(payload={"id": 1}))


@app.channel("orders.invalid")
async def orders_invalid() -> None:
    raise RuntimeError("invalid order")
