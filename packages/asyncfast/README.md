# AsyncFast

AsyncFast is a modern, event framework for building APIs with Python based on standard Python type hints.

## Installation

```
pip install asyncfast==0.32.0
```

## Example

Create a file `main.py` with:

```python
from asyncfast import AsyncFast
from pydantic import BaseModel

app = AsyncFast()


class Payload(BaseModel):
    id: str
    name: str


@app.channel("topic")
async def on_topic(payload: Payload) -> None:
    print(payload)
```

### Running

To run the app install an AMGI server (at the moment there is only `amgi-aiokafka`) then run:

```
$ asyncfast run amgi-aiokafka main:app topic
```

### AsyncAPI Generation

```
$ asyncfast asyncapi main:app
{
  "asyncapi": "3.0.0",
  "info": {
    "title": "AsyncFast",
    "version": "0.1.0"
  },
  "channels": {
    "OnTopic": {
      "address": "topic",
      "messages": {
        "OnTopicMessage": {
          "$ref": "#/components/messages/OnTopicMessage"
        }
      }
    }
  },
  "operations": {
    "receiveOnTopic": {
      "action": "receive",
      "channel": {
        "$ref": "#/channels/OnTopic"
      }
    }
  },
  "components": {
    "messages": {
      "OnTopicMessage": {
        "payload": {
          "$ref": "#/components/schemas/Payload"
        }
      }
    },
    "schemas": {
      "Payload": {
        "properties": {
          "id": {
            "title": "Id",
            "type": "string"
          },
          "name": {
            "title": "Name",
            "type": "string"
          }
        },
        "required": [
          "id",
          "name"
        ],
        "title": "Payload",
        "type": "object"
      }
    }
  }
}
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2025 AMGI
