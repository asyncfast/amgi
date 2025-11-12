import asyncio
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from amgi_common import OperationBatcher


async def test_operation_batcher() -> None:
    mock_function = AsyncMock()
    mock_result = Mock()
    mock_function.return_value = [mock_result]

    batcher = OperationBatcher(mock_function)

    result = await batcher.enqueue(Mock())

    assert result == mock_result


async def test_operation_batcher_exception() -> None:
    class _TestException(Exception):
        pass

    mock_function = AsyncMock()
    mock_function.return_value = [_TestException()]

    batcher = OperationBatcher(mock_function)

    with pytest.raises(_TestException):
        await batcher.enqueue(Mock())


async def test_operation_batcher_batch() -> None:
    mock_function = AsyncMock()
    mock_result1 = Mock()
    mock_result2 = Mock()

    mock_function.return_value = [mock_result1, mock_result2]

    batcher = OperationBatcher(mock_function)

    mock_item1 = Mock()
    mock_item2 = Mock()
    result1, result2 = await asyncio.gather(
        batcher.enqueue(mock_item1), batcher.enqueue(mock_item2)
    )

    assert result1 == mock_result1
    assert result2 == mock_result2
    mock_function.assert_awaited_once_with((mock_item1, mock_item2))
