from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from amgi_common import OperationCacher


async def test_operation_cacher() -> None:
    mock_function = AsyncMock()
    mock_key = Mock()

    operation_cacher = OperationCacher(mock_function)

    result = await operation_cacher.get(mock_key)

    mock_function.assert_awaited_once_with(mock_key)
    assert result == mock_function.return_value


async def test_operation_cacher_awaited_once_per_item() -> None:
    mock_function = AsyncMock()
    mock_key = Mock()

    operation_cacher = OperationCacher(mock_function)

    result1 = await operation_cacher.get(mock_key)
    result2 = await operation_cacher.get(mock_key)

    mock_function.assert_awaited_once_with(mock_key)
    assert result1 == mock_function.return_value
    assert result2 == mock_function.return_value


async def test_operation_cacher_not_cached_on_exception() -> None:
    class _TestException(Exception):
        pass

    mock_return = Mock()
    mock_function = AsyncMock(side_effect=[_TestException(), mock_return])
    mock_key = Mock()

    operation_cacher = OperationCacher(mock_function)

    with pytest.raises(_TestException):
        await operation_cacher.get(mock_key)
    result = await operation_cacher.get(mock_key)

    assert mock_function.await_count == 2
    assert result == mock_return
