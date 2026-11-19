"""DDL, DML and query behaviour as seen through the Python connector."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

import pytest
from snowflake.connector import ProgrammingError


def test_create_insert_select_update_delete(conn):
    cur = conn.cursor()
    cur.execute("create table customers (id int, name varchar, balance number(10, 2))")
    assert cur.fetchone() == ("Table CUSTOMERS successfully created.",)

    cur.execute("insert into customers values (1, 'Ada', 10.50), (2, 'Bob', 0)")
    assert cur.fetchone() == (2,)
    assert cur.rowcount == 2

    cur.execute("update customers set balance = balance + 1 where id = 1")
    assert cur.rowcount == 1

    cur.execute("select id, name, balance from customers order by id")
    assert cur.fetchall() == [(1, "Ada", Decimal("11.50")), (2, "Bob", Decimal("0.00"))]

    cur.execute("delete from customers where id = 2")
    assert cur.rowcount == 1
    assert cur.execute("select count(*) from customers").fetchone() == (1,)


def test_unquoted_identifiers_fold_to_upper_case(conn):
    cur = conn.cursor()
    cur.execute('create table t (lower_col int, "MixedCol" int)')
    cur.execute("insert into t values (1, 2)")
    cur.execute("select * from T")
    assert [column.name for column in cur.description] == ["LOWER_COL", "MixedCol"]
    assert cur.execute('select Lower_Col, "MixedCol" from testdb.public.t').fetchone() == (1, 2)


def test_unaliased_expressions_are_named_after_their_sql(conn):
    cur = conn.cursor()
    cur.execute("select count(*), 1 + 1, current_database() from (select 1 x)")
    assert [column.name for column in cur.description] == [
        "COUNT(*)",
        "1 + 1",
        "CURRENT_DATABASE()",
    ]


def test_result_metadata(conn):
    cur = conn.cursor()
    cur.execute("select 1::number(10, 2) amount, 'x' label, true flag")
    amount, label, flag = cur.description
    assert (amount.precision, amount.scale) == (10, 2)
    assert label.internal_size == 16_777_216
    assert flag.type_code == 13  # BOOLEAN


def test_scalar_types_round_trip(conn):
    cur = conn.cursor()
    cur.execute(
        """
        select
            12345678901234567890 as big,
            1.5::float as real,
            null as nothing,
            date '2024-01-02' as d,
            time '12:34:56.789' as t,
            timestamp '2024-01-02 03:04:05.123456' as ts,
            timestamp '1969-12-31 23:59:59.5' as before_epoch,
            to_binary('6869', 'hex') as raw
        """
    )
    assert cur.fetchone() == (
        12345678901234567890,
        1.5,
        None,
        dt.date(2024, 1, 2),
        dt.time(12, 34, 56, 789000),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
        dt.datetime(1969, 12, 31, 23, 59, 59, 500000),
        b"hi",
    )


def test_semi_structured_values_come_back_as_json(conn):
    cur = conn.cursor()
    cur.execute("select [1, 2] arr, {'key': 'v', 'Nested': {'n': 1}} obj")
    arr, obj = cur.fetchone()
    assert json.loads(arr) == [1, 2]
    assert json.loads(obj) == {"key": "v", "Nested": {"n": 1}}


def test_create_variants(conn):
    cur = conn.cursor()
    cur.execute("create table t (id int)")
    cur.execute("create table if not exists t (id int)")
    assert cur.fetchone() == ("T already exists, statement succeeded.",)

    with pytest.raises(ProgrammingError) as failure:
        cur.execute("create table t (id int)")
    assert (failure.value.errno, failure.value.sqlstate) == (2002, "42710")

    cur.execute("create or replace table t as select 1 a, 2 b")
    assert cur.execute("select a, b from t").fetchone() == (1, 2)

    cur.execute("create view v as select a + b total from t")
    assert cur.fetchone() == ("View V successfully created.",)
    assert cur.execute("select total from v").fetchone() == (3,)


def test_drop_variants(conn):
    cur = conn.cursor()
    cur.execute("create table t (id int)")
    assert cur.execute("drop table t").fetchone() == ("T successfully dropped.",)
    cur.execute("drop table if exists t")
    assert cur.fetchone() == ("Drop statement executed successfully (T already dropped).",)

    with pytest.raises(ProgrammingError) as failure:
        cur.execute("drop table t")
    assert (failure.value.errno, failure.value.sqlstate) == (2003, "42S02")


def test_database_and_schema_lifecycle(conn):
    cur = conn.cursor()
    cur.execute("create database analytics")
    assert cur.fetchone() == ("Database ANALYTICS successfully created.",)
    cur.execute("create schema analytics.raw")
    cur.execute("create table analytics.raw.events (id int)")
    cur.execute("insert into analytics.raw.events values (1)")
    assert cur.execute("select count(*) from analytics.raw.events").fetchone() == (1,)

    cur.execute("drop schema analytics.raw")  # takes its tables with it
    with pytest.raises(ProgrammingError):
        cur.execute("select * from analytics.raw.events")

    cur.execute("drop database analytics")
    assert cur.execute("select current_database()").fetchone() == (None,)
    cur.execute("drop database if exists analytics")


def test_truncate(conn):
    cur = conn.cursor()
    cur.execute("create table t as select 1 id")
    cur.execute("truncate table t")
    assert cur.execute("select count(*) from t").fetchone() == (0,)


def test_transactions(conn):
    cur = conn.cursor()
    cur.execute("create table t (id int)")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("rollback")
    assert cur.execute("select count(*) from t").fetchone() == (0,)

    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("commit")
    assert cur.execute("select count(*) from t").fetchone() == (1,)

    cur.execute("commit")  # nothing open: Snowflake accepts it


def test_missing_table(conn):
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute("select * from nope")
    assert (failure.value.errno, failure.value.sqlstate) == (2003, "42S02")
    assert "Object 'NOPE' does not exist or not authorized." in failure.value.msg


def test_syntax_error(conn):
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute("selec 1")
    assert (failure.value.errno, failure.value.sqlstate) == (1003, "42000")


@pytest.mark.parametrize(
    "sql",
    [
        "show tables",
        "create warehouse wh",
        "grant select on table t to role r",
        "put file:///tmp/data.csv @stage",
        "merge into t using s on t.id = s.id when matched then delete",
    ],
)
def test_unsupported_statements_are_rejected_not_guessed(conn, sql):
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute(sql)
    assert (failure.value.errno, failure.value.sqlstate) == (2, "0A000")


def test_multiple_statements_are_rejected(conn):
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute("select 1; select 2")
    assert failure.value.errno == 8


@pytest.mark.parametrize("sql", ["select 'abc", 'select "abc', "select 1 /* open"])
def test_unterminated_tokens_are_syntax_errors(conn, sql):
    # The tokenizer fails before the parser does; it must not surface as HTTP 500.
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute(sql)
    assert failure.value.errno == 1003
    assert failure.value.sqlstate == "42000"


def test_create_or_replace_schema(conn):
    cur = conn.cursor()
    cur.execute("create or replace schema staging")
    assert cur.fetchone() == ("Schema STAGING successfully created.",)
    cur.execute("create table t (id int)")

    cur.execute("create or replace schema staging")
    assert cur.fetchone() == ("Schema STAGING successfully created.",)
    assert conn.schema == "STAGING"
    with pytest.raises(ProgrammingError) as failure:  # the old contents are gone
        cur.execute("select * from t")
    assert failure.value.errno == 2003

    with pytest.raises(ProgrammingError) as failure:
        cur.execute("create schema staging")
    assert failure.value.errno == 2002


@pytest.mark.parametrize(
    "sql",
    [
        "select list_value(1, 2)",  # a DuckDB function
        "select hash(1)",  # exists on both sides, with different results
        "select * from range(3)",
        "select * from unnest([1, 2])",
        "select * from duckdb_tables()",
        "select * from read_csv('/etc/hosts')",
        "select * from read_text('/etc/hosts')",
        "create table leak as select * from read_text('/etc/hosts')",
        "select * from information_schema.tables",
    ],
)
def test_duckdb_only_sql_is_rejected(conn, sql):
    with pytest.raises(ProgrammingError) as failure:
        conn.cursor().execute(sql)
    assert (failure.value.errno, failure.value.sqlstate) == (2, "0A000")


def test_generator_is_supported(conn):
    cur = conn.cursor()
    cur.execute("select count(*) from table(generator(rowcount => 5))")
    assert cur.fetchone() == (5,)


def test_relation_names_are_case_sensitive(conn):
    cur = conn.cursor()
    cur.execute('create table "MiXed" (id int)')
    cur.execute("create table plain (id int)")
    assert cur.execute('select * from "MiXed"').fetchall() == []
    assert cur.execute("select * from PLAIN").fetchall() == []
    assert cur.execute("select * from testdb.public.plain").fetchall() == []
    for sql in (
        "select * from mixed",
        "insert into mixed values (1)",
        'select * from "plain"',
        'select * from testdb."public".plain',
        'select * from "system"."main"."duckdb_tables"',
    ):
        with pytest.raises(ProgrammingError) as failure:
            cur.execute(sql)
        assert failure.value.errno == 2003, sql


def test_cte_and_temporary_names_resolve(conn):
    cur = conn.cursor()
    cur.execute("create table t (id int)")
    assert cur.execute("with t as (select 1 a) select * from t").fetchall() == [(1,)]
    cur.execute("create temporary table scratch (id int)")
    cur.execute("insert into scratch values (1)")
    assert cur.execute("select * from scratch").fetchall() == [(1,)]


def test_a_transaction_sees_the_tables_it_created(conn):
    cur = conn.cursor()
    cur.execute("begin")
    cur.execute("create table fresh (id int)")
    cur.execute("insert into fresh values (1)")
    assert cur.execute("select * from fresh").fetchall() == [(1,)]
    cur.execute("rollback")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select * from fresh")
    assert failure.value.errno == 2003


@pytest.mark.parametrize("name", ["int", "integer", "bigint", "smallint", "tinyint", "byteint"])
def test_integer_types_are_number_38_0(conn, name):
    # In Snowflake these are all synonyms of NUMBER(38, 0), not machine integers.
    cur = conn.cursor()
    huge = 10**37 + 1
    cur.execute(f"create table n (id {name})")
    cur.execute(f"insert into n values (3000000000), ({huge})")
    cur.execute("select id, id + 1 from n order by id")
    assert cur.fetchall() == [(3000000000, 3000000001), (huge, huge + 1)]
    column = cur.description[0]
    assert (column.precision, column.scale) == (38, 0)
    assert cur.execute(f"select 3000000000::{name}").fetchone() == (3000000000,)
