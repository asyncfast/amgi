# amgi-paho-mqtt

amgi-paho-mqtt is an [AMGI](https://amgi.readthedocs.io/en/latest/) compatible server to run AMGI applications against
[MQTT](https://mqtt.org/).

## Installation

```
pip install amgi-paho-mqtt==0.32.0
```

## Example

This example uses [AsyncFast](https://pypi.org/project/asyncfast/):

```python
from dataclasses import dataclass

from amgi_paho_mqtt import run
from asyncfast import AsyncFast

app = AsyncFast()


@dataclass
class Order:
    item_ids: list[str]


@app.channel("order-topic")
async def order_topic(order: Order) -> None:
    # Makes an order
    ...


if __name__ == "__main__":
    run(app, "order-topic")
```

Or the application could be run via the commandline:

```commandline
asyncfast run amgi-paho-mqtt main:app order-topic
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
