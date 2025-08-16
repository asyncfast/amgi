# AsyncFast

AsyncFast is a modern, event framework for building APIs with Python based on standard Python type hints.

Key Features:

- **Portable**: Following AMGI should allow for implementations of any protocol
- **Standards-based**: Based on [AsyncAPI] and [JSON Schema]

## Terminology

Terminology throughout this documentation will follow [AsyncAPI] where possible, this means using terms like `channel`
instead of `topic` etc.

## AMGI

AMGI (Asynchronous Messaging Gateway Interface) is the spiritual sibling of [ASGI]. While the focus of [ASGI] is HTTP,
the focus here is event-based applications.

```{warning}
Given this project is in the very early stages treat AMGI as unstable
```

## Installation

```
pip install asyncfast
```

## Basic Usage

The simplest AsyncFast could be:

```{eval-rst}
.. async-fast-example:: examples/payload_basemodel.py
```

### Running

To run the app install an AMGI server (at the moment there is only `amgi-aiokafka`) then run:

```
$ asyncfast run amgi-aiokafka main:app channel
```

### AsyncAPI Generation

```
$ asyncfast asyncapi main:app
```

## Requirements

This project stands on the shoulders of giants:

- [Pydantic] for the data parts.

Taking ideas from:

- [FastAPI] for the typed API parts.
- [ASGI] for the portability.

```{toctree}
:hidden:
message_payload
message_headers
channel_parameters
```

[asgi]: https://asgi.readthedocs.io/en/latest/
[asyncapi]: https://www.asyncapi.com/
[fastapi]: https://fastapi.tiangolo.com/
[json schema]: https://json-schema.org/
[pydantic]: https://docs.pydantic.dev/
