import sys
from collections.abc import Awaitable
from collections.abc import Iterable
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional
from typing import TypedDict
from typing import Union

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class AMGIVersions(TypedDict):
    spec_version: str
    version: Union[Literal["1.0"]]


class MessageScope(TypedDict):
    type: Literal["message"]
    amgi: AMGIVersions
    address: str
    state: NotRequired[dict[str, Any]]


class LifespanScope(TypedDict):
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
    type: Literal["message.receive"]
    id: str
    headers: Iterable[tuple[bytes, bytes]]
    payload: NotRequired[Optional[bytes]]
    bindings: NotRequired[dict[str, dict[str, Any]]]
    more_messages: NotRequired[bool]


class MessageAckEvent(TypedDict):
    type: Literal["message.ack"]
    id: str


class MessageNackEvent(TypedDict):
    type: Literal["message.nack"]
    id: str
    message: str


class MessageSendEvent(TypedDict):
    type: Literal["message.send"]
    address: str
    headers: Iterable[tuple[bytes, bytes]]
    payload: NotRequired[Optional[bytes]]
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
