from collections.abc import AsyncGenerator
from collections.abc import Generator
from dataclasses import dataclass
from typing import Annotated
from typing import Union
from uuid import UUID

from asyncfast import AsyncFast
from asyncfast import Header
from asyncfast import Message
from asyncfast import Payload
from asyncfast.bindings import KafkaKey
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


def test_asyncapi_header_sync() -> None:
    app = AsyncFast()

    @app.channel("hello")
    def on_hello(request_id: Annotated[int, Header()]) -> None:
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


def test_asyncapi_address_parameter() -> None:
    app = AsyncFast()

    @app.channel("order.{user_id}")
    async def order_handler(user_id: str) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "OrderHandler": {
                "address": "order.{user_id}",
                "messages": {
                    "OrderHandlerMessage": {
                        "$ref": "#/components/messages/OrderHandlerMessage"
                    }
                },
                "parameters": {"user_id": {}},
            }
        },
        "components": {
            "messages": {"OrderHandlerMessage": {}},
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveOrderHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/OrderHandler"},
            }
        },
    }


def test_asyncapi_send() -> None:
    app = AsyncFast()

    @dataclass
    class Send(Message, address="send"):
        id: int

    @app.channel("receive")
    async def receive_handler(id: int) -> AsyncGenerator[Send, None]:
        yield Send(id=id)

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "ReceiveHandler": {
                "address": "receive",
                "messages": {
                    "ReceiveHandlerMessage": {
                        "$ref": "#/components/messages/ReceiveHandlerMessage"
                    }
                },
            },
            "Send": {
                "address": "send",
                "messages": {"Send": {"$ref": "#/components/messages/Send"}},
            },
        },
        "components": {
            "messages": {
                "ReceiveHandlerMessage": {"payload": {"type": "integer"}},
                "Send": {"payload": {"type": "integer"}},
            }
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveReceiveHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/ReceiveHandler"},
            },
            "sendSend": {"action": "send", "channel": {"$ref": "#/channels/Send"}},
        },
    }


def test_asyncapi_send_sync() -> None:
    app = AsyncFast()

    @dataclass
    class Send(Message, address="send"):
        id: int

    @app.channel("receive")
    def receive_handler(id: int) -> Generator[Send, None, None]:
        yield Send(id=id)

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "ReceiveHandler": {
                "address": "receive",
                "messages": {
                    "ReceiveHandlerMessage": {
                        "$ref": "#/components/messages/ReceiveHandlerMessage"
                    }
                },
            },
            "Send": {
                "address": "send",
                "messages": {"Send": {"$ref": "#/components/messages/Send"}},
            },
        },
        "components": {
            "messages": {
                "ReceiveHandlerMessage": {"payload": {"type": "integer"}},
                "Send": {"payload": {"type": "integer"}},
            }
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveReceiveHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/ReceiveHandler"},
            },
            "sendSend": {"action": "send", "channel": {"$ref": "#/channels/Send"}},
        },
    }


def test_asyncapi_send_payload() -> None:
    app = AsyncFast()

    class SendPayload(BaseModel):
        id: int

    @dataclass
    class Send(Message, address="send"):
        payload: SendPayload

    @app.channel("receive")
    async def receive_handler(id: int) -> AsyncGenerator[Send, None]:
        yield Send(payload=SendPayload(id=id))

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "ReceiveHandler": {
                "address": "receive",
                "messages": {
                    "ReceiveHandlerMessage": {
                        "$ref": "#/components/messages/ReceiveHandlerMessage"
                    }
                },
            },
            "Send": {
                "address": "send",
                "messages": {"Send": {"$ref": "#/components/messages/Send"}},
            },
        },
        "components": {
            "messages": {
                "ReceiveHandlerMessage": {"payload": {"type": "integer"}},
                "Send": {"payload": {"$ref": "#/components/schemas/SendPayload"}},
            },
            "schemas": {
                "SendPayload": {
                    "properties": {"id": {"title": "Id", "type": "integer"}},
                    "required": ["id"],
                    "title": "SendPayload",
                    "type": "object",
                }
            },
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveReceiveHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/ReceiveHandler"},
            },
            "sendSend": {"action": "send", "channel": {"$ref": "#/channels/Send"}},
        },
    }


def test_asyncapi_send_multiple() -> None:
    app = AsyncFast()

    @dataclass
    class SendA(Message, address="send_a"):
        id: int

    @dataclass
    class SendB(Message, address="send_b"):
        id: int

    @app.channel("receive")
    async def receive_handler(id: int) -> AsyncGenerator[Union[SendA, SendB], None]:
        yield SendA(id=id)

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "ReceiveHandler": {
                "address": "receive",
                "messages": {
                    "ReceiveHandlerMessage": {
                        "$ref": "#/components/messages/ReceiveHandlerMessage"
                    }
                },
            },
            "SendA": {
                "address": "send_a",
                "messages": {"SendA": {"$ref": "#/components/messages/SendA"}},
            },
            "SendB": {
                "address": "send_b",
                "messages": {"SendB": {"$ref": "#/components/messages/SendB"}},
            },
        },
        "components": {
            "messages": {
                "ReceiveHandlerMessage": {"payload": {"type": "integer"}},
                "SendA": {"payload": {"type": "integer"}},
                "SendB": {"payload": {"type": "integer"}},
            }
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveReceiveHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/ReceiveHandler"},
            },
            "sendSendA": {"action": "send", "channel": {"$ref": "#/channels/SendA"}},
            "sendSendB": {"action": "send", "channel": {"$ref": "#/channels/SendB"}},
        },
    }


async def test_asyncapi_binding_kafka_key() -> None:
    app = AsyncFast()

    @app.channel("topic")
    async def topic_handler(key: Annotated[UUID, KafkaKey()]) -> None:
        pass

    assert app.asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {
            "TopicHandler": {
                "address": "topic",
                "messages": {
                    "TopicHandlerMessage": {
                        "$ref": "#/components/messages/TopicHandlerMessage"
                    }
                },
            }
        },
        "components": {
            "messages": {
                "TopicHandlerMessage": {
                    "bindings": {"kafka": {"key": {"format": "uuid", "type": "string"}}}
                }
            }
        },
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {
            "receiveTopicHandler": {
                "action": "receive",
                "channel": {"$ref": "#/channels/TopicHandler"},
            }
        },
    }
