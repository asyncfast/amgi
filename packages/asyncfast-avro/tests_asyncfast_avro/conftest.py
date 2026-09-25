from typing import Any
from typing import Callable
from typing import cast

import pytest
from asyncfast import Message
from pytest_benchmark.fixture import BenchmarkFixture

MessageBenchmark = Callable[[Message], dict[str, Any]]


@pytest.fixture
def message_benchmark(benchmark: BenchmarkFixture) -> MessageBenchmark:
    def _message_benchmark(
        message: Message,
    ) -> dict[str, Any]:
        result = benchmark(lambda: dict(message))
        return cast(dict[str, Any], result)

    return _message_benchmark
