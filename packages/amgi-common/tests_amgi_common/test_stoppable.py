import asyncio
from asyncio import Event
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from amgi_common import Stoppable


async def test_stoppable() -> None:
    stoppable = Stoppable()
    mock_function = AsyncMock()
    mock_arg = Mock()
    mock_kwarg = Mock()

    async for result in stoppable.call(mock_function, mock_arg, kwarg=mock_kwarg):
        assert result == mock_function.return_value
        stoppable.stop()

    mock_function.assert_awaited_once_with(mock_arg, kwarg=mock_kwarg)


async def test_stoppable_cancel_task() -> None:
    call_event = Event()
    hold_event = Event()

    async def _function() -> None:
        call_event.set()
        await hold_event.wait()

    stoppable = Stoppable()

    aiter_call = aiter(stoppable.call(_function))

    anext_task = asyncio.create_task(anext(aiter_call))
    await call_event.wait()
    stoppable.stop()

    with pytest.raises(StopAsyncIteration):
        await anext_task


async def test_stoppable_does_not_call_if_stopped() -> None:
    stoppable = Stoppable()
    mock_function = AsyncMock()

    aiter_call = aiter(stoppable.call(mock_function))
    stoppable.stop()
    with pytest.raises(StopAsyncIteration):
        await anext(aiter_call)

    mock_function.assert_not_awaited()
