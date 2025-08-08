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


class ACGIVersions(TypedDict):
    spec_version: str
    version: Union[Literal["1.0"]]


class MessageScope(TypedDict):
    type: Literal["message"]
    acgi: ACGIVersions
    address: str
    headers: Iterable[Tuple[bytes, bytes]]
    payload: NotRequired[Optional[bytes]]


class LifespanScope(TypedDict):
    type: Literal["lifespan"]
    acgi: ACGIVersions


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

ACGIReceiveEvent = Union[LifespanStartupEvent, LifespanShutdownEvent]
ACGISendEvent = Union[
    LifespanStartupCompleteEvent,
    LifespanStartupFailedEvent,
    LifespanShutdownCompleteEvent,
    LifespanShutdownFailedEvent,
    MessageAcknowledgeEvent,
    MessageSendEvent,
]

ACGIReceiveCallable = Callable[[], Awaitable[ACGIReceiveEvent]]
ACGISendCallable = Callable[[ACGISendEvent], Awaitable[None]]

ACGIApplication = Callable[
    [
        Scope,
        ACGIReceiveCallable,
        ACGISendCallable,
    ],
    Awaitable[None],
]
