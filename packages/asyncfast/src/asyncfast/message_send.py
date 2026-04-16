import sys
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import AsyncExitStack
from types import TracebackType
from typing import AsyncContextManager

from amgi_types import MessageSendEvent
from asyncfast._utils import Router

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


_MessageSendT = Callable[[MessageSendEvent], Awaitable[None]]
_MessageSendManagerT = AsyncContextManager[_MessageSendT]


class MessageSendRouter:
    def __init__(self, *, default: _MessageSendManagerT | None = None) -> None:
        self._default_manager = default
        self._route_managers: list[tuple[str, _MessageSendManagerT]] = []

        self._default_send: _MessageSendT | None = None
        self._route_sends = Router[_MessageSendT]()

        self._exit_stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        if self._default_manager is not None:
            self._default_send = await self._exit_stack.enter_async_context(
                self._default_manager,
            )

        for address, message_send_manager in self._route_managers:
            message_send = await self._exit_stack.enter_async_context(
                message_send_manager
            )
            self._route_sends.add_route(address, message_send)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._exit_stack.aclose()

    def add_route(self, address: str, message_send: _MessageSendManagerT) -> None:
        self._route_managers.append((address, message_send))

    async def __call__(self, event: MessageSendEvent) -> None:
        _, message_send = self._route_sends.get(
            event["address"],
            default=self._default_send or Router.MISSING,
        )

        await message_send(event)
