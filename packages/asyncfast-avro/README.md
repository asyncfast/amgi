# asyncfast-avro

asyncfast-avro adds [Avro](https://avro.apache.org/) payload support to
[AsyncFast](https://pypi.org/project/asyncfast/). Annotating a payload argument with `AvroPayload` parses it as Avro
binary, with the schema derived from the same type hints used for JSON payloads.

The schema is published in the generated AsyncAPI document as a
[Multi Format Schema Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#multiFormatSchemaObject), so
consumers in other languages can be generated from it.

## Installation

```
pip install asyncfast-avro==0.47.0
```

## Example

```python
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast_avro import AvroPayload
from pydantic import BaseModel

app = AsyncFast()


class Order(BaseModel):
    id: str
    skus: list[str]


@app.channel("order")
async def handle_order(order: Annotated[Order, AvroPayload()]) -> None:
    print(order)
```

Payloads are encoded as plain Avro binary, not the Confluent wire format, so no schema registry is involved. The schema
in the AsyncAPI document is both the writer and the reader schema: a single self-contained schema per payload, with no
schema evolution and no reader/writer distinction.

Naive `datetime` values (with no timezone) are encoded as UTC. Field defaults are included in the schema, except for
fields of nested models, where a default would require expanding the defaults of all of the nested fields.

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
