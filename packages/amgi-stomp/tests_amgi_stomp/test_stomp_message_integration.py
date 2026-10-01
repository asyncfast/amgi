import asyncio
import time
from collections.abc import AsyncGenerator
from types import TracebackType
from typing import Any
from typing import Generator
from uuid import uuid4

import pytest
from amgi_common import LifespanFailureError
from amgi_stomp import _MessageSendT
from amgi_stomp import _run_cli
from amgi_stomp import run
from amgi_stomp import Server
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import MessageSendEvent
from amgi_types import Scope
from stompman import AckableMessageFrame
from stompman import Client
from stompman import ConnectionParameters
from stompman import MessageFrame
from test_utils import assert_run_can_terminate
from test_utils import MockApp
from testcontainers.core.container import DockerContainer

_LOGIN = "artemis"
_PASSCODE = "artemis"


async def _wait_for_stomp(connection_parameters: ConnectionParameters) -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            async with Client(
                servers=[connection_parameters],
                connect_retry_attempts=1,
                connect_retry_interval=0,
            ):
                return
        except Exception:  # pragma: no cover
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.5)


async def _send(
    client: Client, body: bytes, destination: str, headers: dict[str, str] | None = None
) -> None:
    # Broker writes can stall indefinitely on a dropped port-forwarded socket, so
    # bound the write to keep the failure visible instead of hanging the run
    await asyncio.wait_for(client.send(body, destination, headers=headers), 5)


@pytest.fixture(scope="module")
def artemis_container() -> Generator[DockerContainer, None, None]:
    with DockerContainer(image="apache/activemq-artemis:2.44.0").with_exposed_ports(
        61613
    ) as container:
        asyncio.run(
            _wait_for_stomp(
                ConnectionParameters(
                    host=container.get_container_host_ip(),
                    port=int(container.get_exposed_port(61613)),
                    login=_LOGIN,
                    passcode=_PASSCODE,
                )
            )
        )
        yield container


@pytest.fixture
def destination() -> str:
    return f"/queue/queue-{uuid4()}"


@pytest.fixture
def connection_parameters(
    artemis_container: DockerContainer,
) -> ConnectionParameters:
    return ConnectionParameters(
        host=artemis_container.get_container_host_ip(),
        port=int(artemis_container.get_exposed_port(61613)),
        login=_LOGIN,
        passcode=_PASSCODE,
    )


@pytest.fixture
async def client(
    connection_parameters: ConnectionParameters,
) -> AsyncGenerator[Client, None]:
    async with Client(servers=[connection_parameters]) as stomp_client:
        yield stomp_client


def _make_server(
    app: MockApp,
    destination: str,
    connection_parameters: ConnectionParameters,
) -> Server:
    return Server(
        app,
        destination,
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
    )


@pytest.fixture
async def app(
    connection_parameters: ConnectionParameters, destination: str
) -> AsyncGenerator[MockApp, None]:
    app = MockApp()
    async with app.lifespan(
        server=_make_server(app, destination, connection_parameters)
    ):
        yield app


@pytest.fixture
async def app_with_state(
    connection_parameters: ConnectionParameters, destination: str
) -> AsyncGenerator[tuple[MockApp, dict[str, Any]], None]:
    app = MockApp()
    state = {"item": uuid4()}
    async with app.lifespan(
        state, server=_make_server(app, destination, connection_parameters)
    ):
        yield app, state


@pytest.mark.integration
async def test_message(app: MockApp, destination: str, client: Client) -> None:
    await _send(client, b"value", destination, headers={"custom": "value"})

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["address"] == destination
        assert scope["payload"] == b"value"
        assert dict(scope["headers"])[b"custom"] == b"value"
        assert isinstance(scope["bindings"]["stomp"]["message_id"], str)
        assert scope["bindings"]["stomp"]["message_id"]
        assert isinstance(scope["bindings"]["stomp"]["subscription"], str)
        assert scope["bindings"]["stomp"]["subscription"]
        assert scope["state"] == {}
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_message_send(app: MockApp, destination: str, client: Client) -> None:
    send_destination = f"/queue/send-{uuid4()}"
    messages: asyncio.Queue[MessageFrame] = asyncio.Queue()

    async def handler(frame: AckableMessageFrame) -> None:
        await messages.put(frame)

    subscription = await client.subscribe_with_manual_ack(send_destination, handler)

    await _send(client, b"", destination)

    async with app.call() as (scope, receive, send):
        await send(
            {
                "type": "message.send",
                "address": send_destination,
                "headers": [(b"key", b"value")],
                "payload": b"test",
            }
        )
        await send({"type": "message.ack"})

    frame = await asyncio.wait_for(messages.get(), 5)
    assert frame.body == b"test"
    assert frame.headers["destination"] == send_destination
    assert frame.headers.get("key") == "value"

    await subscription.unsubscribe()


@pytest.mark.integration
async def test_message_send_defaults_empty_payload_and_headers(
    app: MockApp, destination: str, client: Client
) -> None:
    send_destination = f"/queue/send-{uuid4()}"
    messages: asyncio.Queue[MessageFrame] = asyncio.Queue()

    async def handler(frame: AckableMessageFrame) -> None:
        await messages.put(frame)

    subscription = await client.subscribe_with_manual_ack(send_destination, handler)

    await _send(client, b"", destination)

    async with app.call() as (scope, receive, send):
        await send({"type": "message.send", "address": send_destination, "headers": []})
        await send({"type": "message.ack"})

    frame = await asyncio.wait_for(messages.get(), 5)
    assert frame.body == b""
    assert frame.headers.get("key") is None

    await subscription.unsubscribe()


@pytest.mark.integration
@pytest.mark.timeout(30)
async def test_message_send_receipt(
    connection_parameters: ConnectionParameters, destination: str, client: Client
) -> None:
    send_destination = f"/queue/receipt-{uuid4()}"
    messages: asyncio.Queue[MessageFrame] = asyncio.Queue()

    async def handler(frame: AckableMessageFrame) -> None:
        await messages.put(frame)

    subscription = await client.subscribe_with_manual_ack(send_destination, handler)

    receipt_app = MockApp()
    async with receipt_app.lifespan(
        server=Server(
            receipt_app,
            destination,
            host=connection_parameters.host,
            port=connection_parameters.port,
            login=connection_parameters.login,
            passcode=connection_parameters.passcode,
            receipt_timeout=5,
        )
    ):
        await _send(client, b"", destination)

        async with receipt_app.call() as (scope, receive, send):
            await send(
                {
                    "type": "message.send",
                    "address": send_destination,
                    "headers": [],
                    "payload": b"receipt",
                }
            )
            await send({"type": "message.ack"})

    frame = await asyncio.wait_for(messages.get(), 5)
    assert frame.body == b"receipt"

    await subscription.unsubscribe()


@pytest.mark.integration
async def test_message_ack(app: MockApp, destination: str, client: Client) -> None:
    await _send(client, b"value", destination)

    async with app.call() as (scope, receive, send):
        await send({"type": "message.ack"})

    messages: asyncio.Queue[MessageFrame] = asyncio.Queue()

    async def handler(frame: AckableMessageFrame) -> None:
        # Only reached if the broker wrongly redelivers an acknowledged message
        await messages.put(frame)  # pragma: no cover

    subscription = await client.subscribe_with_manual_ack(destination, handler)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(messages.get(), 1)

    await subscription.unsubscribe()


@pytest.mark.integration
async def test_message_nack(app: MockApp, destination: str, client: Client) -> None:
    await _send(client, b"value", destination)

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["payload"] == b"value"
        await send({"type": "message.nack", "message": "reject"})

    # The broker does not redeliver the nacked message, but the subscription stays
    # healthy and processes the next message
    await _send(client, b"second", destination)

    async with app.call() as (scope, receive, send):
        assert scope["type"] == "message"
        assert scope["payload"] == b"second"
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_lifespan(
    app_with_state: tuple[MockApp, dict[str, Any]],
    destination: str,
    client: Client,
) -> None:
    app, state = app_with_state

    await _send(client, b"", destination)

    async with app.call() as (scope, receive, send):
        assert scope["state"] == state
        await send({"type": "message.ack"})


@pytest.mark.integration
async def test_message_receive_not_callable(
    app: MockApp, destination: str, client: Client
) -> None:
    await _send(client, b"test", destination)

    async with app.call() as (scope, receive, send):
        with pytest.raises(RuntimeError, match="Receive should not be called"):
            await receive()
        await send({"type": "message.ack"})


@pytest.mark.integration
def test_run(destination: str, connection_parameters: ConnectionParameters) -> None:
    assert_run_can_terminate(
        run,
        destination,
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
    )


@pytest.mark.integration
def test_run_cli(destination: str, connection_parameters: ConnectionParameters) -> None:
    assert_run_can_terminate(
        _run_cli,
        [destination],
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
    )


async def _failing_lifespan_app(
    scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
) -> None:
    assert scope["type"] == "lifespan"
    event = await receive()
    assert event == {"type": "lifespan.startup"}
    await send({"type": "lifespan.startup.failed", "message": "startup failed"})


class _StubMessageSend:
    def __init__(self, suppress: bool = False, exit_delay: float = 0) -> None:
        self._suppress = suppress
        self._exit_delay = exit_delay

    async def __aenter__(self) -> _MessageSendT:
        async def message_send(event: MessageSendEvent) -> None:
            return None  # pragma: no cover

        return message_send

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        await asyncio.sleep(self._exit_delay)
        return self._suppress


@pytest.mark.integration
async def test_serve_lifespan_startup_failure(
    connection_parameters: ConnectionParameters, destination: str
) -> None:
    server = Server(
        _failing_lifespan_app,
        destination,
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
    )
    serve_task = asyncio.create_task(server.serve())

    with pytest.raises(LifespanFailureError):
        await asyncio.wait_for(serve_task, 5)


@pytest.mark.integration
async def test_serve_message_send_suppresses_exception(
    connection_parameters: ConnectionParameters, destination: str
) -> None:
    server = Server(
        _failing_lifespan_app,
        destination,
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
        message_send=_StubMessageSend(suppress=True),
    )
    serve_task = asyncio.create_task(server.serve())

    await asyncio.wait_for(serve_task, 5)


@pytest.mark.integration
@pytest.mark.timeout(30)
async def test_serve_message_send_exit_timeout(
    connection_parameters: ConnectionParameters, destination: str
) -> None:
    server = Server(
        _failing_lifespan_app,
        destination,
        host=connection_parameters.host,
        port=connection_parameters.port,
        login=connection_parameters.login,
        passcode=connection_parameters.passcode,
        message_send=_StubMessageSend(exit_delay=60),
    )
    serve_task = asyncio.create_task(server.serve())

    with pytest.raises(LifespanFailureError):
        await asyncio.wait_for(serve_task, 30)
