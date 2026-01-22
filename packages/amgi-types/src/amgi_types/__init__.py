import sys
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any
from typing import Literal
from typing import TypedDict
from typing import Union

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class AMGIVersions(TypedDict):
    """
    :var version: Version of the AMGI spec
    :var spec_version: Version of the AMGI message spec this server understands-
    """

    spec_version: str
    version: Literal["1.0"]


class MessageScope(TypedDict):
    """
    :var address: The address of the batch of messages, for example, in Kafka this would be the topic
    :var state:
        A copy of the namespace passed into the lifespan corresponding to this batch. Optional; if missing the server
        does not support this feature.
    :var extensions:
        Extensions allow AMGI servers to advertise optional capabilities to applications. Extensions are provided via
        scope and are opt-in: applications MUST assume an extension is unsupported unless it is explicitly present.
    """

    type: Literal["message"]
    amgi: AMGIVersions
    address: str
    state: NotRequired[dict[str, Any]]
    extensions: NotRequired[dict[str, dict[str, Any]]]


class LifespanScope(TypedDict):
    """
    :var state:
        An empty namespace where the application can persist state to be used when handling subsequent requests.
        Optional; if missing the server does not support this feature.
    """

    type: Literal["lifespan"]
    amgi: AMGIVersions
    state: NotRequired[dict[str, Any]]


class LifespanStartupEvent(TypedDict):
    type: Literal["lifespan.startup"]


class LifespanShutdownEvent(TypedDict):
    type: Literal["lifespan.shutdown"]


class LifespanStartupCompleteEvent(TypedDict):
    type: Literal["lifespan.startup.complete"]


class LifespanStartupFailedEvent(TypedDict):
    type: Literal["lifespan.startup.failed"]
    message: str


class LifespanShutdownCompleteEvent(TypedDict):
    type: Literal["lifespan.shutdown.complete"]


class LifespanShutdownFailedEvent(TypedDict):
    type: Literal["lifespan.shutdown.failed"]
    message: str


class MessageReceiveEvent(TypedDict):
    """
    :var id: A unique id for the message, used to ack, or nack the message
    :var headers: Includes the headers of the message
    :var payload:
        Payload of the message, which can be :py:obj:`None` or :py:obj:`bytes`. If missing, it defaults to
        :py:obj:`None`
    :var bindings:
        Protocol specific bindings, for example, when receiving a Kafka message the bindings could include the key:
        ``{"kafka": {"key": b"key"}}``
    :var more_messages:
        Indicates there are more messages to process in the batch. The application should keep receiving until it
        receives :py:obj:`False`. If missing it defaults to :py:obj:`False`
    """

    type: Literal["message.receive"]
    id: str
    headers: Iterable[tuple[bytes, bytes]]
    payload: NotRequired[bytes | None]
    bindings: NotRequired[dict[str, dict[str, Any]]]
    more_messages: NotRequired[bool]


class MessageAckEvent(TypedDict):
    """
    :var id: The unique id of the message
    """

    type: Literal["message.ack"]
    id: str


class MessageNackEvent(TypedDict):
    """
    :var id: The unique id of the message
    :var message: A message indicating why the message could not be processed
    """

    type: Literal["message.nack"]
    id: str
    message: str


class MessageSendEvent(TypedDict):
    """
    :var address: Address to send the message to
    :var headers: Headers of the message
    :var payload:
        Payload of the message, which can be :py:obj:`None`, or :py:obj:`bytes`. If missing, it defaults to
        :py:obj:`None`.
    :var bindings:
        Protocol specific bindings to send. This can be bindings for multiple protocols, allowing the server to decide
        to handle them, or ignore them.
    """

    type: Literal["message.send"]
    address: str
    headers: Iterable[tuple[bytes, bytes]]
    payload: NotRequired[bytes | None]
    bindings: NotRequired[dict[str, dict[str, Any]]]


Scope = Union[MessageScope, LifespanScope]

AMGIReceiveEvent = Union[
    LifespanStartupEvent, LifespanShutdownEvent, MessageReceiveEvent
]
AMGISendEvent = Union[
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent,
    MessageAckEvent,
    MessageNackEvent,
    MessageSendEvent,
]

AMGIReceiveCallable = Callable[[], Awaitable[AMGIReceiveEvent]]
AMGISendCallable = Callable[[AMGISendEvent], Awaitable[None]]

AMGIApplication = Callable[
    [
        Scope,
        AMGIReceiveCallable,
        AMGISendCallable,
    ],
    Awaitable[None],
]
