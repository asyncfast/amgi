# amgi-aiopika-amqp

:construction: This package is currently under development :construction:

AMGI server for AMQP using aio-pika.

## Installation

```
pip install amgi-aiopika-amqp==0.21.0
```

## Usage

```python
from asyncfast import AsyncFast
from amgi_aiopika_amqp import run

app = AsyncFast()

@app.channel("my_queue")
async def handle_message(payload: str) -> None:
    print(f"Received: {payload}")

if __name__ == "__main__":
    run(app, "my_queue", url="amqp://guest:guest@localhost/")
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
