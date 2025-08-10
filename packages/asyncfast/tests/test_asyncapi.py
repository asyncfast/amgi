from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from pydantic import BaseModel


def test_asyncapi_header() -> None:
    app = AsyncFast()

    @app.channel("hello")
    async def on_hello(request_id: Annotated[int, Header()]) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OnHello": {
                "address": "hello",
                "messages": {
                    "OnHelloMessage": {"$ref": "#/components/messages/OnHelloMessage"}
                },
            }
        },
        "components": {
            "messages": {
                "OnHelloMessage": {
                    "headers": {"$ref": "#/components/schemas/OnHelloHeaders"}
                }
            },
            "schemas": {
                "OnHelloHeaders": {
                    "properties": {
                        "request-id": {"title": "Request-Id", "type": "integer"}
                    },
                    "title": "OnHelloHeaders",
                    "type": "object",
                }
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnHello": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnHello"},
            }
        },
    }


def test_asyncapi_payload() -> None:
    app = AsyncFast()

    class Payload(BaseModel):
        id: int
        name: str

    @app.channel("hello")
    async def on_hello(payload: Payload) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OnHello": {
                "address": "hello",
                "messages": {
                    "OnHelloMessage": {"$ref": "#/components/messages/OnHelloMessage"}
                },
            }
        },
        "components": {
            "messages": {
                "OnHelloMessage": {"payload": {"$ref": "#/components/schemas/Payload"}}
            },
            "schemas": {
                "Payload": {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "name": {"title": "Name", "type": "string"},
                    },
                    "required": ["id", "name"],
                    "title": "Payload",
                    "type": "object",
                }
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnHello": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnHello"},
            }
        },
    }


def test_asyncapi_payload_dataclass() -> None:
    app = AsyncFast()

    @dataclass
    class Payload:
        id: int
        name: str

    @app.channel("hello")
    async def on_hello(payload: Payload) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OnHello": {
                "address": "hello",
                "messages": {
                    "OnHelloMessage": {"$ref": "#/components/messages/OnHelloMessage"}
                },
            }
        },
        "components": {
            "messages": {
                "OnHelloMessage": {"payload": {"$ref": "#/components/schemas/Payload"}}
            },
            "schemas": {
                "Payload": {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "name": {"title": "Name", "type": "string"},
                    },
                    "required": ["id", "name"],
                    "title": "Payload",
                    "type": "object",
                }
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnHello": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnHello"},
            }
        },
    }
