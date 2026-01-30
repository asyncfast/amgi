# amgi-common

This package includes some useful helpers for writing AMGI servers.

## Installation

```
pip install amgi-common==0.32.0
```

## Constructs

### Lifespan

This class handles the AMGI lifespan protocol.

```python
from amgi_common import Lifespan


async def main_loop(app, state):
    """Handle event batches"""


async def serve(app):
    async with Lifespan(app) as state:
        # handle app calls
        await main_loop(app, state)
```

## Stoppable

This class should help with graceful shutdowns. Whenever you have a call to something with a timeout, it cancels that task. .
For example, if you have a client that has a method `fetch_messages`, which has a timeout you could loop like so:

```python
from amgi_common import Stoppable


class Server:
    def __init__(self, app):
        self._app = app
        self._stoppable = Stoppable()
        self._client = Client()
        self._running = True

    async def main_loop(self, app, state):
        while self._running:
            messages = await self._client.fetch_messages(timeout=10_000)
            # Handle messages

    def stop(self):
        self._running = False
```

There are several ways to deal with this:

1. The above example, where on stop you will have to wait for the timeout plus the time to handle messages
1. Run the main loop in a task, and cancel it on stop

Both have their own problems. In the first case, you could be waiting a long time. In the second case, when you receive
messages, you should probably process them before shutting down.

The class `Stoppable` is there to help you. It will return the results of the callable iterably, but if the stoppable has
been told to stop, it will cancel any tasks it has running in the background, and stop any current iterations.

```python
from amgi_common import Stoppable


class Server:
    def __init__(self, app):
        self._app = app
        self._stoppable = Stoppable()
        self._client = Client()

    async def main_loop(self, app, state):
        async for messages in self._stoppable.call(
            self._client.fetch_messages, timeout=10_000
        ):
            # Handle messages
            pass

    def stop(self):
        self._stoppable.stop()
```

## OperationBatcher

This class can simplify batch operations, for example, publishing multiple messages simultaneous. The implementation
allows you to enqueue an item to run the operation on, and the operation will be scheduled in batches:

```python
from amgi_common import OperationBatcher


async def batch_operation(items):
    # Do a batch operation to get the batch result
    return [item for item in batch_result]


operation_batcher = OperationBatcher(batch_operation)

# You can enqueue an item, it will be processed in the background automatically
result = await operation_batcher.enqueue({"an": "item"})
```

The batch operation should return an iterable of results, or exceptions. This allows for per item errors. The batch
sizes can also be limited, which is usefully when APIs have a hard limit.

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
