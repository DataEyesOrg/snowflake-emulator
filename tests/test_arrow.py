"""Arrow results, pandas and large results downloaded as chunks."""

from __future__ import annotations

import datetime as dt
import json
import urllib.request
from collections.abc import Iterator
from decimal import Decimal

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from sfemu.testing import RunningServer

ARROW = {"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "arrow"}


@pytest.fixture
def arrow_conn(server: RunningServer) -> Iterator[SnowflakeConnection]:
    parameters = server.connection_parameters(database="testdb", session_parameters=ARROW)
    connection = snowflake.connector.connect(**parameters)
    yield connection
    connection.close()


@pytest.fixture
def chunked_conn(server: RunningServer) -> Iterator[SnowflakeConnection]:
    parameters = server.connection_parameters(
        database="testdb",
        session_parameters={**ARROW, "CLIENT_RESULT_CHUNK_SIZE": 2},
    )
    connection = snowflake.connector.connect(**parameters)
    yield connection
    connection.close()


def test_arrow_results_match_the_json_format(arrow_conn):
    cur = arrow_conn.cursor()
    cur.execute(
        """
        select
            12345678901234567890 as big,
            1.5::number(10, 2) as amount,
            2.5::float as real,
            null as nothing,
            true as flag,
            date '2024-01-02' as d,
            time '12:34:56.789' as t,
            timestamp '2024-01-02 03:04:05.123456' as ts,
            '2024-01-02 03:04:05.5'::timestamp_ltz as ltz,
            to_binary('6869', 'hex') as raw,
            [1, 2] as arr
        """
    )
    assert cur.fetchone() == (
        12345678901234567890,
        Decimal("1.50"),
        2.5,
        None,
        True,
        dt.date(2024, 1, 2),
        dt.time(12, 34, 56, 789000),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
        dt.datetime(2024, 1, 2, 3, 4, 5, 500000, tzinfo=dt.UTC),
        bytearray(b"hi"),
        "[\n  1,\n  2\n]",
    )


def test_arrow_metadata_matches_the_json_format(arrow_conn):
    cur = arrow_conn.cursor()
    cur.execute("select 1::number(10, 2) amount, 'x' label, 'x'::variant v")
    amount, label, variant = cur.description
    assert (amount.precision, amount.scale) == (10, 2)
    assert label.internal_size == 16_777_216
    assert variant.type_code == 5  # VARIANT


def test_empty_arrow_result(arrow_conn):
    cur = arrow_conn.cursor()
    cur.execute("create table e (id int)")
    assert cur.execute("select * from e").fetchall() == []
    assert cur.execute("select * from e").description[0].name == "ID"


def test_empty_arrow_result_in_pandas_and_batches(arrow_conn):
    # Connectors 3.x / 4.x crash on a zero-row record batch, so an empty result
    # must travel as an empty rowsetBase64, like on Snowflake.
    cur = arrow_conn.cursor()
    cur.execute("create table e (id int, name varchar)")
    frame = cur.execute("select * from e").fetch_pandas_all()
    assert frame.shape == (0, 2)
    assert list(frame.columns) == ["ID", "NAME"]
    batches = cur.execute("select * from e").get_result_batches()
    assert sum(batch.rowcount for batch in batches) == 0


def test_fetch_pandas_all(arrow_conn):
    cur = arrow_conn.cursor()
    cur.execute(
        "create table events as select seq4() id, 'e' || seq4() name "
        "from table(generator(rowcount => 1000))"
    )
    frame = cur.execute("select * from events order by id").fetch_pandas_all()
    assert len(frame) == 1000
    assert frame["ID"].sum() == 499_500
    assert frame["NAME"][999] == "e999"


def test_integer_columns_are_integers_in_pandas(arrow_conn):
    # Integer types are NUMBER(38, 0); values that fit must still reach pandas
    # as int64 and not as Decimal objects.
    cur = arrow_conn.cursor()
    cur.execute("create table n (id int, big bigint)")
    cur.execute("insert into n values (1, 3000000000), (2, 4000000000)")
    frame = cur.execute("select * from n order by id").fetch_pandas_all()
    assert [str(dtype) for dtype in frame.dtypes] == ["int64", "int64"]
    assert frame["BIG"].tolist() == [3000000000, 4000000000]

    cur.execute("insert into n values (3, 99999999999999999999)")  # beyond int64
    assert cur.execute("select big from n where id = 3").fetchone() == (99999999999999999999,)


def test_large_arrow_results_arrive_in_chunks(chunked_conn):
    cur = chunked_conn.cursor()
    cur.execute("create table t as select seq4() id from table(generator(rowcount => 5))")

    assert cur.execute("select * from t order by id").fetchall() == [(i,) for i in range(5)]

    batches = cur.execute("select * from t order by id").get_result_batches()
    assert [batch.rowcount for batch in batches] == [2, 2, 1]
    # Every batch past the first comes from a chunk download.
    assert all(batch.compressed_size is not None for batch in batches[1:])
    assert list(batches[2].create_iter()) == [(4,)]


def test_large_json_results_arrive_in_chunks(server):
    parameters = server.connection_parameters(
        database="testdb",
        session_parameters={
            "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json",
            "CLIENT_RESULT_CHUNK_SIZE": 3,
        },
    )
    with snowflake.connector.connect(**parameters) as conn:
        cur = conn.cursor()
        cur.execute(
            "create table t as select seq4() id, 'x' || seq4() s "
            "from table(generator(rowcount => 10))"
        )
        assert cur.execute("select * from t order by id").fetchall() == [
            (i, f"x{i}") for i in range(10)
        ]
        batches = cur.execute("select * from t order by id").get_result_batches()
        assert [batch.rowcount for batch in batches] == [3, 3, 3, 1]
        assert list(batches[3].create_iter()) == [(9, "x9")]


def test_timezones_before_1970_survive_the_arrow_epoch_split(arrow_conn):
    cur = arrow_conn.cursor()
    cur.execute("select timestamp '1969-12-31 23:59:59.5' before_epoch")
    assert cur.fetchone() == (dt.datetime(1969, 12, 31, 23, 59, 59, 500000),)


def test_login_applies_and_echoes_session_parameters(server):
    body = {
        "data": {
            "LOGIN_NAME": "t",
            "PASSWORD": "t",
            "ACCOUNT_NAME": "t",
            "SESSION_PARAMETERS": {**ARROW, "QUERY_TAG": "nightly", "TIMEZONE": "Europe/Rome"},
        }
    }
    request = urllib.request.Request(
        f"http://{server.host}:{server.port}/session/v1/login-request",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request) as response:
        reply = json.load(response)["data"]
    echoed = {parameter["name"]: parameter["value"] for parameter in reply["parameters"]}
    assert echoed["PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"] == "arrow"
    assert echoed["QUERY_TAG"] == "nightly"
    assert echoed["TIMEZONE"] == "Europe/Rome"
