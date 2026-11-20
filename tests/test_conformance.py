"""Runs the conformance corpus in tests/conformance against the emulator.

Each ``.sql`` file there is a list of statements with the rows Snowflake
returns for them; the file format is described at the top of each file.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from sfemu.testing import running_server

CORPUS = Path(__file__).parent / "conformance"
EXPECT = "-->"
# The connector's type codes for VARIANT, OBJECT and ARRAY, the same in every version.
SEMI_STRUCTURED = {5, 9, 10}


def blocks(path: Path) -> Iterator[tuple[str, Any]]:
    """The (statement, expected rows or None) pairs of a corpus file."""
    for block in path.read_text().split("\n\n"):
        lines = [line for line in block.splitlines() if line.strip()]
        expected = [line[len(EXPECT) :] for line in lines if line.startswith(EXPECT)]
        sql = "\n".join(line for line in lines if not line.startswith("--")).strip()
        if sql:
            yield sql.rstrip(";"), json.loads(expected[0]) if expected else None


FILES = sorted(CORPUS.glob("*.sql"))
QUERIES = [
    pytest.param(path.name, sql, expected, id=f"{path.stem}:{number}")
    for path in FILES
    for number, (sql, expected) in enumerate(blocks(path), start=1)
    if expected is not None
]


@pytest.fixture(scope="module", params=["ARROW", "JSON"])
def connections(request) -> Iterator[dict[str, SnowflakeConnection]]:
    """One connection per corpus file, each to a database with that file's setup done."""
    with running_server() as server:
        found = {}
        for path in FILES:
            connection = snowflake.connector.connect(
                **server.connection_parameters(database=path.stem, schema="public")
            )
            cursor = connection.cursor()
            cursor.execute(
                f"alter session set python_connector_query_result_format = '{request.param}'"
            )
            for sql, expected in blocks(path):
                if expected is None:
                    cursor.execute(sql)
            found[path.name] = connection
        yield found
        for connection in found.values():
            connection.close()


def plain(value: Any, type_code: int) -> Any:
    """A fetched value in the form the corpus writes it in."""
    if value is None:
        return None
    if type_code in SEMI_STRUCTURED:
        return json.loads(value)
    if isinstance(value, Decimal):
        return float(value) if value % 1 else int(value)
    if isinstance(value, dt.date | dt.datetime | dt.time):
        return value.isoformat()
    return value


@pytest.mark.parametrize(("file", "sql", "expected"), QUERIES)
def test_conformance(connections, file: str, sql: str, expected: list[list[Any]]):
    cursor = connections[file].cursor()
    cursor.execute(sql)
    codes = [column.type_code for column in cursor.description]
    rows = [[plain(value, code) for value, code in zip(row, codes, strict=True)] for row in cursor]
    assert rows == expected
