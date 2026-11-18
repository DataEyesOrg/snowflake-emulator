from __future__ import annotations

from collections.abc import Iterator

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from sfemu.testing import RunningServer, running_server


@pytest.fixture
def server() -> Iterator[RunningServer]:
    with running_server() as running:
        yield running


@pytest.fixture
def conn(server: RunningServer) -> Iterator[SnowflakeConnection]:
    connection = snowflake.connector.connect(
        **server.connection_parameters(database="testdb", schema="public", warehouse="wh")
    )
    yield connection
    connection.close()
