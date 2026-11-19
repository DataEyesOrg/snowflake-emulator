"""SHOW, DESCRIBE, INFORMATION_SCHEMA and the DDL that changes what they report."""

from __future__ import annotations

import datetime as dt

import pytest
from snowflake.connector import DictCursor, ProgrammingError


@pytest.fixture
def cur(conn):
    cur = conn.cursor()
    cur.execute("create table orders (id int not null, note varchar default 'n/a', at timestamp)")
    cur.execute("create view open_orders as select id from orders")
    return cur


def names(cur, sql: str) -> list[str]:
    return [row["name"] for row in cur.connection.cursor(DictCursor).execute(sql)]


def test_show_objects_tables_and_views(cur):
    assert names(cur, "show objects") == ["OPEN_ORDERS", "ORDERS"]
    assert names(cur, "show tables") == ["ORDERS"]
    assert names(cur, "show views in schema testdb.public") == ["OPEN_ORDERS"]

    row = cur.connection.cursor(DictCursor).execute("show objects like 'ord%'").fetchone()
    assert (row["database_name"], row["schema_name"], row["kind"]) == ("TESTDB", "PUBLIC", "TABLE")
    assert isinstance(row["created_on"], dt.datetime)
    assert (row["is_dynamic"], row["is_iceberg"]) == ("N", "N")

    cur.execute("show terse objects in testdb.public")
    assert [column.name for column in cur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]


def test_show_narrows_with_like_starts_with_and_limit(cur):
    cur.execute("create table other (id int)")
    assert names(cur, "show objects like '%ORDERS'") == ["OPEN_ORDERS", "ORDERS"]
    assert names(cur, "show objects starts with 'O' limit 2") == ["OPEN_ORDERS", "ORDERS"]
    assert names(cur, "show objects limit 10 from 'OPEN_ORDERS'") == ["ORDERS", "OTHER"]
    assert names(cur, "show objects starts with 'o'") == []  # STARTS WITH is case-sensitive


def test_show_schemas_and_databases(cur):
    cur.execute("create schema staging")
    assert names(cur, "show terse schemas in database testdb") == [
        "INFORMATION_SCHEMA",
        "PUBLIC",
        "STAGING",
    ]
    cur.execute("create database other")
    assert names(cur, "show databases") == ["OTHER", "TESTDB"]
    assert names(cur, "show schemas in account") == [
        "INFORMATION_SCHEMA",
        "PUBLIC",
        "INFORMATION_SCHEMA",
        "PUBLIC",
        "STAGING",
    ]


def test_show_in_a_missing_schema_fails_like_snowflake(cur):
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("show objects in testdb.nowhere")
    assert failure.value.errno == 2043  # dbt relies on this to detect a missing schema


def test_describe_table(cur):
    rows = cur.connection.cursor(DictCursor).execute("describe table orders").fetchall()
    assert [(r["name"], r["type"], r["kind"], r["null?"], r["default"]) for r in rows] == [
        ("ID", "NUMBER(38,0)", "COLUMN", "N", None),
        ("NOTE", "VARCHAR(16777216)", "COLUMN", "Y", "'n/a'"),
        ("AT", "TIMESTAMP_NTZ(9)", "COLUMN", "Y", None),
    ]
    assert [row[0] for row in cur.execute("desc view open_orders")] == ["ID"]
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("describe table missing")
    assert failure.value.errno == 2003
    with pytest.raises(ProgrammingError):
        cur.execute("describe view orders")  # a table is not a view


def test_information_schema(cur):
    cur.execute("select table_name, table_type from information_schema.tables order by 1")
    assert cur.fetchall() == [("OPEN_ORDERS", "VIEW"), ("ORDERS", "BASE TABLE")]

    cur.execute(
        "select column_name, data_type, is_nullable, numeric_precision, numeric_scale "
        "from testdb.information_schema.columns c "
        "where c.table_name = 'ORDERS' order by ordinal_position"
    )
    assert cur.fetchall() == [
        ("ID", "NUMBER", "NO", 38, 0),
        ("NOTE", "TEXT", "YES", None, None),
        ("AT", "TIMESTAMP_NTZ", "YES", None, None),
    ]

    cur.execute("select schema_name from information_schema.schemata order by 1")
    assert cur.fetchall() == [("INFORMATION_SCHEMA",), ("PUBLIC",)]
    cur.execute("select table_name from information_schema.views")
    assert cur.fetchall() == [("OPEN_ORDERS",)]

    cur.execute("create database other")
    cur.execute("select count(*) from other.information_schema.tables")
    assert cur.fetchone() == (0,)
    cur.execute("select count(*) from testdb.information_schema.tables")
    assert cur.fetchone() == (2,)


def test_alter_table(cur):
    cur.execute("alter table orders add column qty int, flag boolean")
    cur.execute("alter table orders rename column note to remark")
    cur.execute("alter table orders drop column at")
    cur.execute("alter table orders alter qty set data type number(10, 2)")
    described = [(row[0], row[1]) for row in cur.execute("describe table orders")]
    assert described == [
        ("ID", "NUMBER(38,0)"),
        ("REMARK", "VARCHAR(16777216)"),
        ("QTY", "NUMBER(10,2)"),
        ("FLAG", "BOOLEAN"),
    ]
    cur.execute("alter table if exists missing add column x int")  # accepted
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("alter table missing add column x int")
    assert failure.value.errno == 2003


def test_alter_is_all_or_nothing(cur):
    with pytest.raises(ProgrammingError):
        cur.execute("alter table orders add column qty int, id int")  # ID exists
    assert [row[0] for row in cur.execute("describe table orders")] == ["ID", "NOTE", "AT"]


def test_rename_table_and_view(cur):
    cur.execute("drop view open_orders")
    cur.execute("alter table orders rename to testdb.public.purchases")
    cur.execute("create view open_orders as select id from purchases")
    cur.execute("alter view open_orders rename to open_purchases")
    assert names(cur, "show objects") == ["OPEN_PURCHASES", "PURCHASES"]
    cur.execute("create table spare (id int)")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("alter table spare rename to purchases")
    assert failure.value.errno == 2002


def test_comments(cur):
    cur.execute("comment on table orders is 'All orders'")
    cur.execute("comment on column orders.id is 'The key'")
    cur.execute("alter table orders alter note comment $$A note$$, column at comment 'When'")
    cur.execute("alter view open_orders set comment = 'Still open'")
    rows = cur.connection.cursor(DictCursor).execute("show objects").fetchall()
    assert {row["name"]: row["comment"] for row in rows} == {
        "ORDERS": "All orders",
        "OPEN_ORDERS": "Still open",
    }
    comments = {row[0]: row[9] for row in cur.execute("describe table orders")}
    assert comments == {"ID": "The key", "NOTE": "A note", "AT": "When"}
    cur.execute("comment if exists on table missing is 'x'")  # accepted
    with pytest.raises(ProgrammingError):
        cur.execute("comment on table missing is 'x'")


def test_merge(cur):
    cur.execute("insert into orders (id, note) values (1, 'old'), (2, 'old'), (3, 'old')")
    cur.execute("create table changes (id int, note varchar, gone boolean)")
    cur.execute(
        "insert into changes values (2, 'new', false), (3, null, true), (4, 'added', false)"
    )
    cur.execute(
        "merge into orders o using changes c on o.id = c.id "
        "when matched and c.gone then delete "
        "when matched then update set o.note = c.note "
        "when not matched then insert (id, note) values (c.id, c.note)"
    )
    assert [column.name for column in cur.description] == [
        "number of rows inserted",
        "number of rows updated",
        "number of rows deleted",
    ]
    assert cur.fetchone() == (1, 1, 1)
    assert cur.rowcount == 3
    cur.execute("select id, note from orders order by id")
    assert cur.fetchall() == [(1, "old"), (2, "new"), (4, "added")]


def test_grants_are_accepted_and_not_recorded(cur):
    cur.execute("grant select on table orders to role reporter")
    cur.execute("revoke select on table orders from role reporter")
    cur.execute("grant ownership on table orders to role owner copy current grants")
    assert cur.execute("show grants on table orders").fetchall() == []


def test_drop_table_leaves_dependent_views(cur):
    # CASCADE in Snowflake is about foreign keys, not views.
    cur.execute("drop table if exists orders cascade")
    assert names(cur, "show objects") == ["OPEN_ORDERS"]


def test_transient_and_replace(cur):
    cur.execute("create or replace transient table orders as (select 1 as id)")
    assert cur.execute("select * from orders").fetchall() == [(1,)]
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("create or replace view orders as select 1 as id")
    assert failure.value.errno == 2002
    assert "is of type 'TABLE'" in failure.value.msg


def test_temporary_tables_belong_to_a_schema_and_a_session(server, cur):
    import snowflake.connector

    parameters = server.connection_parameters(database="testdb")
    with snowflake.connector.connect(**parameters) as other:
        temp = other.cursor()
        temp.execute("create temporary table testdb.public.scratch as select 1 as id")
        assert temp.execute("select * from testdb.public.scratch").fetchall() == [(1,)]
        assert [row[0] for row in temp.execute("describe table scratch")] == ["ID"]
    # The session that made it is gone, and so is the table.
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select * from scratch")
    assert failure.value.errno == 2003


def test_to_timestamp_of_an_expression(cur):
    cur.execute("select to_timestamp_ntz('2024-05-06 07:08:09'::varchar || '')")
    assert cur.fetchone() == (dt.datetime(2024, 5, 6, 7, 8, 9),)
    cur.execute("select to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) is not null")
    assert cur.fetchone() == (True,)
