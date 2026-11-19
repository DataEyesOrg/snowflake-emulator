"""Connecting, session context and the driver-facing protocol."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from decimal import Decimal

import pytest
import snowflake.connector
from snowflake.connector import DictCursor, ProgrammingError, SnowflakeConnection

from sfemu.testing import running_server


def test_login_sets_session_context(conn):
    assert (conn.database, conn.schema, conn.warehouse) == ("TESTDB", "PUBLIC", "WH")
    row = (
        conn.cursor()
        .execute("select current_database(), current_schema(), current_warehouse(), current_user()")
        .fetchone()
    )
    assert row == ("TESTDB", "PUBLIC", "WH", "TEST")


def test_login_without_database_has_no_namespace(server):
    with snowflake.connector.connect(**server.connection_parameters()) as conn:
        cur = conn.cursor()
        assert cur.execute("select current_database(), 1 + 1").fetchone() == (None, 2)
        with pytest.raises(ProgrammingError) as failure:
            cur.execute("create table t (id int)")
        assert failure.value.errno == 90105
        assert "does not have a current database" in failure.value.msg


def test_login_does_not_create_namespace_when_auto_create_is_off():
    with running_server(auto_create=False) as server:
        parameters = server.connection_parameters(database="missing")
        with snowflake.connector.connect(**parameters) as conn:
            cur = conn.cursor()
            assert cur.execute("select current_database()").fetchone() == (None,)
            with pytest.raises(ProgrammingError):
                cur.execute("use database missing")


def test_use_switches_database_and_schema(conn):
    cur = conn.cursor()
    cur.execute("create database other")
    assert conn.database == "OTHER" and conn.schema == "PUBLIC"
    cur.execute("create schema staging")
    assert conn.schema == "STAGING"
    cur.execute("use database testdb")
    assert (conn.database, conn.schema) == ("TESTDB", "PUBLIC")
    cur.execute("use schema other.staging")
    assert (conn.database, conn.schema) == ("OTHER", "STAGING")
    cur.execute("use warehouse big_wh")
    assert conn.warehouse == "BIG_WH"

    with pytest.raises(ProgrammingError) as failure:
        cur.execute("use database nope")
    assert failure.value.errno == 2043


def test_sessions_share_data_but_not_context(server):
    parameters = server.connection_parameters(database="shared")
    with (
        snowflake.connector.connect(**parameters) as first,
        snowflake.connector.connect(**parameters) as second,
    ):
        first.cursor().execute("create table t as select 42 answer")
        first.cursor().execute("create schema elsewhere")
        assert first.schema == "ELSEWHERE"
        assert second.schema == "PUBLIC"
        assert second.cursor().execute("select answer from t").fetchone() == (42,)


def test_alter_session_is_accepted(conn):
    cur = conn.cursor()
    cur.execute("alter session set timezone = 'Europe/Rome', query_tag = 'nightly'")
    assert cur.fetchone() == ("Statement executed successfully.",)
    cur.execute("alter session unset query_tag")


def test_queries_fail_after_logout(server):
    conn = snowflake.connector.connect(**server.connection_parameters())
    cur = conn.cursor()
    conn.close()
    with pytest.raises(snowflake.connector.Error):
        cur.execute("select 1")


def test_dict_cursor(conn):
    row = conn.cursor(DictCursor).execute("select 1 as id, 'a' as name").fetchone()
    assert row == {"ID": 1, "NAME": "a"}


def test_qmark_binding(server):
    snowflake.connector.paramstyle = "qmark"
    try:
        with snowflake.connector.connect(**server.connection_parameters()) as conn:
            row = conn.cursor().execute("select ?, ?, ?", (7, "seven", 7.5)).fetchone()
            assert row == (7, "seven", 7.5)
    finally:
        snowflake.connector.paramstyle = "pyformat"


def test_bare_use_names_a_database(conn):
    cur = conn.cursor()
    cur.execute("create database other")
    cur.execute("create schema staging")
    cur.execute("use testdb")
    assert (conn.database, conn.schema) == ("TESTDB", "PUBLIC")
    cur.execute("use other.staging")  # qualified, so it is a schema
    assert (conn.database, conn.schema) == ("OTHER", "STAGING")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("use staging")  # a schema, not a database
    assert failure.value.errno == 2043


def test_unset_timezone_restores_the_default(conn):
    cur = conn.cursor()
    noon = "select '2024-06-01 12:00:00'::timestamp_ltz"
    cur.execute("alter session set timezone = 'Europe/Rome'")
    assert cur.execute(noon).fetchone()[0].utcoffset().total_seconds() == 2 * 3600
    cur.execute("alter session unset timezone")
    assert cur.execute(noon).fetchone()[0].utcoffset().total_seconds() == 0


@pytest.fixture
def qmark_conn(server) -> Iterator[SnowflakeConnection]:
    snowflake.connector.paramstyle = "qmark"
    try:
        with snowflake.connector.connect(**server.connection_parameters(database="d")) as conn:
            yield conn
    finally:
        snowflake.connector.paramstyle = "pyformat"


def test_qmark_binding_of_dates_times_and_binary(qmark_conn):
    cur = qmark_conn.cursor()
    cur.execute(
        "create table b (d date, t time, n timestamp_ntz, l timestamp_ltz, x binary, m number(9,2))"
    )
    rome = dt.timezone(dt.timedelta(hours=2))
    values = (
        dt.date(1960, 5, 6),
        dt.time(10, 11, 12, 123456),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
        dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=rome),
        b"ab",
        Decimal("1.50"),
    )
    cur.execute("insert into b values (?, ?, ?, ?, ?, ?)", values)
    d, t, n, ltz, x, m = cur.execute("select * from b").fetchone()
    assert (d, t, n, bytes(x), m) == (values[0], values[1], values[2], b"ab", Decimal("1.50"))
    assert ltz == values[3]  # the same instant, shown in the session time zone


def test_executemany_with_qmark(qmark_conn):
    cur = qmark_conn.cursor()
    cur.execute("create table q (id int, name varchar)")
    cur.executemany("insert into q values (?, ?)", [(1, "a"), (2, None), (3, "c")])
    assert cur.rowcount == 3
    cur.executemany("update q set name = ? where id = ?", [("x", 1), ("y", 2)])
    assert cur.execute("select * from q order by id").fetchall() == [(1, "x"), (2, "y"), (3, "c")]


def test_executemany_inserts_all_rows_or_none(qmark_conn):
    cur = qmark_conn.cursor()
    cur.execute("create table q (id int)")
    with pytest.raises(ProgrammingError):
        cur.executemany("insert into q values (?)", [(1,), ("not a number",)])
    assert cur.execute("select count(*) from q").fetchone() == (0,)
