from collections.abc import Generator

import pytest
from nats.aio.client import Client
from testcontainers.nats import NatsContainer


@pytest.fixture(scope="package")
def nats_container() -> Generator[NatsContainer, None, None]:
    with NatsContainer() as nats_container:
        yield nats_container


@pytest.fixture
async def client(nats_container: NatsContainer) -> Client:
    client = Client()
    await client.connect(nats_container.nats_uri())
    return client
