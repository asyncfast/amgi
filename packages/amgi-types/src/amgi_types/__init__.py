import sys
from typing import Awaitable
from typing import Callable
from typing import Iterable
from typing import Literal
from typing import Optional
from typing import Tuple
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
    headers: Iterable[Tuple[bytes, bytes]]
    payload: NotRequired[Optional[bytes]]


class LifespanScope(TypedDict):
    type: Literal["lifespan"]
    amgi: AMGIVersions


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


class MessageAcknowledgeEvent(TypedDict):
    type: Literal["message.acknowledge"]


class MessageSendEvent(TypedDict):
    type: Literal["message.send"]
    address: str
    headers: Iterable[Tuple[bytes, bytes]]
    payload: NotRequired[Optional[bytes]]


Scope = Union[MessageScope, LifespanScope]

AMGIReceiveEvent = Union[LifespanStartupEvent, LifespanShutdownEvent]
AMGISendEvent = Union[
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent,
    MessageAcknowledgeEvent,
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
