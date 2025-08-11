from dataclasses import dataclass
from typing import Annotated

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Payload
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
                    "required": ["request-id"],
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


def test_asyncapi_header_description() -> None:
    app = AsyncFast()

    @app.channel("hello")
    async def on_hello(
        request_id: Annotated[int, Header(description="Id to correlate the request")],
    ) -> None:
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
                        "request-id": {
                            "description": "Id to correlate the request",
                            "title": "Request-Id",
                            "type": "integer",
                        }
                    },
                    "required": ["request-id"],
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

    class MessagePayload(BaseModel):
        id: int
        name: str

    @app.channel("hello")
    async def on_hello(payload: MessagePayload) -> None:
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
                    "payload": {"$ref": "#/components/schemas/MessagePayload"}
                }
            },
            "schemas": {
                "MessagePayload": {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "name": {"title": "Name", "type": "string"},
                    },
                    "required": ["id", "name"],
                    "title": "MessagePayload",
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
    class MessagePayload:
        id: int
        name: str

    @app.channel("hello")
    async def on_hello(payload: MessagePayload) -> None:
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
                    "payload": {"$ref": "#/components/schemas/MessagePayload"}
                }
            },
            "schemas": {
                "MessagePayload": {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "name": {"title": "Name", "type": "string"},
                    },
                    "required": ["id", "name"],
                    "title": "MessagePayload",
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


def test_asyncapi_payload_simple() -> None:
    app = AsyncFast()

    @app.channel("hello")
    async def on_hello(message: Annotated[str, Payload()]) -> None:
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
            "messages": {"OnHelloMessage": {"payload": {"type": "string"}}},
            "schemas": {},
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnHello": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnHello"},
            }
        },
    }


def test_asyncapi_payload_nested() -> None:
    app = AsyncFast()

    class Address(BaseModel):
        number: int
        street: str
        town: str

    class Person(BaseModel):
        name: str
        address: Address

    @app.channel("register")
    async def on_register(person: Person) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OnRegister": {
                "address": "register",
                "messages": {
                    "OnRegisterMessage": {
                        "$ref": "#/components/messages/OnRegisterMessage"
                    }
                },
            }
        },
        "components": {
            "messages": {
                "OnRegisterMessage": {
                    "payload": {"$ref": "#/components/schemas/Person"}
                }
            },
            "schemas": {
                "Address": {
                    "properties": {
                        "number": {"title": "Number", "type": "integer"},
                        "street": {"title": "Street", "type": "string"},
                        "town": {"title": "Town", "type": "string"},
                    },
                    "required": ["number", "street", "town"],
                    "title": "Address",
                    "type": "object",
                },
                "Person": {
                    "properties": {
                        "address": {"$ref": "#/components/schemas/Address"},
                        "name": {"title": "Name", "type": "string"},
                    },
                    "required": ["name", "address"],
                    "title": "Person",
                    "type": "object",
                },
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOnRegister": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OnRegister"},
            }
        },
    }
