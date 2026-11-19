"""SHOW, DESCRIBE, INFORMATION_SCHEMA and the DDL that changes what they report."""

from __future__ import annotations

import datetime as dt
import json

import pytest
from snowflake.connector import DatabaseError, DictCursor, ProgrammingError


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


def test_show_columns(cur):
    rows = cur.connection.cursor(DictCursor).execute("show columns in table orders").fetchall()
    assert [(r["column_name"], r["null?"], r["kind"]) for r in rows] == [
        ("ID", "false", "COLUMN"),
        ("NOTE", "true", "COLUMN"),
        ("AT", "true", "COLUMN"),
    ]
    assert [json.loads(r["data_type"]) for r in rows] == [
        {"type": "FIXED", "precision": 38, "scale": 0, "nullable": False},
        {
            "type": "TEXT",
            "length": 16777216,
            "byteLength": 16777216,
            "nullable": True,
            "fixed": False,
        },
        {"type": "TIMESTAMP_NTZ", "precision": 0, "scale": 9, "nullable": True},
    ]
    assert (rows[1]["default"], rows[0]["table_name"], rows[0]["database_name"]) == (
        "'n/a'",
        "ORDERS",
        "TESTDB",
    )
    # Without a table: every column of the current schema, views included.
    cur.execute("show columns")
    assert [(row[0], row[2]) for row in cur] == [
        ("OPEN_ORDERS", "ID"),
        ("ORDERS", "ID"),
        ("ORDERS", "NOTE"),
        ("ORDERS", "AT"),
    ]
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("show columns in table missing")
    assert failure.value.errno == 2003


def test_show_parameters(cur):
    cur.execute("alter session set timezone = 'Europe/Rome', query_tag = 'nightly'")
    rows = cur.connection.cursor(DictCursor).execute("show parameters like 'timez%'").fetchall()
    assert [(r["key"], r["value"], r["default"], r["level"], r["type"]) for r in rows] == [
        ("TIMEZONE", "Europe/Rome", "UTC", "SESSION", "STRING")
    ]
    shown = {row[0]: row[1:4] for row in cur.execute("show parameters in session")}
    assert shown["AUTOCOMMIT"] == ("true", "true", "")
    assert shown["QUERY_TAG"] == ("nightly", "", "SESSION")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("show parameters in account")
    assert failure.value.errno == 2


def test_primary_keys_are_recorded_and_not_enforced(cur):
    cur.execute(
        "create table items (id int primary key, sku varchar unique, "
        "order_id int references orders (id))"
    )
    cur.execute("insert into items values (1, 'a', 99), (1, 'a', 99)")  # no constraint fires
    with pytest.raises(DatabaseError) as failure:
        cur.execute("insert into items (sku) values ('b')")  # but a key column is NOT NULL
    assert failure.value.errno == 100072

    cur.execute(
        "create table lines (order_id int, line int, "
        "constraint lines_pk primary key (order_id, line))"
    )
    keys = cur.connection.cursor(DictCursor).execute("show primary keys in table lines").fetchall()
    assert [(k["column_name"], k["key_sequence"], k["constraint_name"]) for k in keys] == [
        ("ORDER_ID", 1, "LINES_PK"),
        ("LINE", 2, "LINES_PK"),
    ]
    assert len(cur.execute("show primary keys").fetchall()) == 3
    described = {row[0]: (row[3], row[5], row[6]) for row in cur.execute("describe table items")}
    assert described == {
        "ID": ("N", "Y", "N"),
        "SKU": ("Y", "N", "Y"),
        "ORDER_ID": ("Y", "N", "N"),
    }


def test_unique_keys(cur):
    def unique_keys(sql: str = "show unique keys in table items") -> list[tuple]:
        return [(row[3], row[4], row[5], row[6]) for row in cur.execute(sql)]

    cur.execute(
        "create table items (id int primary key, sku varchar constraint items_sku unique, "
        "shelf int, slot int, constraint items_place unique (shelf, slot))"
    )
    assert unique_keys() == [
        ("ITEMS", "SKU", 1, "ITEMS_SKU"),
        ("ITEMS", "SHELF", 1, "ITEMS_PLACE"),
        ("ITEMS", "SLOT", 2, "ITEMS_PLACE"),
    ]
    cur.execute("insert into items values (1, 'a', 1, 1), (2, 'a', 1, 1)")  # not enforced

    cur.execute("alter table orders add constraint orders_note unique (note)")
    cur.execute("alter table orders add column code varchar unique")
    assert [row[:2] for row in unique_keys("show unique keys")] == [
        ("ITEMS", "SKU"),
        ("ITEMS", "SHELF"),
        ("ITEMS", "SLOT"),
        ("ORDERS", "NOTE"),
        ("ORDERS", "CODE"),
    ]
    assert len(cur.execute("show primary keys").fetchall()) == 1  # the kinds stay apart

    cur.execute("alter table items rename column slot to position")
    cur.execute("alter table items drop constraint items_sku")
    cur.execute("alter table items rename to stock")
    assert unique_keys("show unique keys in table stock") == [
        ("STOCK", "SHELF", 1, "ITEMS_PLACE"),
        ("STOCK", "POSITION", 2, "ITEMS_PLACE"),
    ]
    cur.execute("create or replace table stock (id int)")
    assert unique_keys("show unique keys in table stock") == []


def test_imported_keys(cur):
    def imported(sql: str) -> list[dict]:
        return cur.connection.cursor(DictCursor).execute(sql).fetchall()

    cur.execute("alter table orders add constraint orders_pk primary key (id)")
    cur.execute("create table customers (region varchar, code int, unique (region, code))")
    cur.execute(
        "create table lines (id int, order_id int constraint lines_order references orders, "
        "region varchar, code int, "
        "constraint lines_customer foreign key (region, code) references customers (region, code))"
    )
    cur.execute("insert into lines values (1, 404, 'nowhere', 0)")  # not enforced

    keys = imported("show imported keys in table lines")
    assert [
        (k["pk_table_name"], k["pk_column_name"], k["fk_column_name"], k["key_sequence"])
        for k in keys
    ] == [
        ("ORDERS", "ID", "ORDER_ID", 1),  # REFERENCES without columns: the primary key
        ("CUSTOMERS", "REGION", "REGION", 1),
        ("CUSTOMERS", "CODE", "CODE", 2),
    ]
    first = keys[0]
    assert (first["fk_name"], first["pk_name"]) == ("LINES_ORDER", "ORDERS_PK")
    assert (first["pk_database_name"], first["pk_schema_name"]) == ("TESTDB", "PUBLIC")
    assert (first["fk_database_name"], first["fk_schema_name"], first["fk_table_name"]) == (
        "TESTDB",
        "PUBLIC",
        "LINES",
    )
    assert (first["update_rule"], first["delete_rule"]) == ("NO ACTION", "NO ACTION")
    assert keys[1]["fk_name"] == keys[2]["fk_name"] == "LINES_CUSTOMER"
    assert keys[1]["pk_name"].startswith("SYS_CONSTRAINT_")  # the unnamed UNIQUE

    assert imported("show imported keys in table orders") == []
    cur.execute("alter table orders add column customer_code int")
    cur.execute("alter table orders add foreign key (customer_code) references customers (code)")
    assert len(imported("show imported keys")) == 4
    assert len(imported("show imported keys in schema testdb.public")) == 4

    # Renames on either side are followed; a dropped table takes its keys along.
    cur.execute("alter table customers rename to clients")
    cur.execute("alter table clients rename column code to client_code")
    cur.execute("alter table lines drop constraint lines_order")
    keys = imported("show imported keys in table lines")
    assert [(k["pk_table_name"], k["pk_column_name"], k["fk_column_name"]) for k in keys] == [
        ("CLIENTS", "REGION", "REGION"),
        ("CLIENTS", "CLIENT_CODE", "CODE"),
    ]
    cur.execute("drop table clients")
    assert imported("show imported keys") == []


def test_alter_table_primary_key(cur):
    def key() -> list[tuple[str, str]]:
        cur.execute("show primary keys in table orders")
        return [(row[4], row[6]) for row in cur]

    cur.execute("alter table orders add constraint orders_pk primary key (id)")
    assert key() == [("ID", "ORDERS_PK")]
    cur.execute("alter table orders rename column id to order_id")
    cur.execute("alter table orders rename to purchases_2")
    cur.execute("alter table purchases_2 rename to orders")
    assert key() == [("ORDER_ID", "ORDERS_PK")]
    cur.execute("alter table orders drop constraint other_name")  # not the key: no effect
    assert key() == [("ORDER_ID", "ORDERS_PK")]
    cur.execute("alter table orders drop primary key")
    assert key() == []
    cur.execute("alter table orders add primary key (order_id)")
    cur.execute("alter table orders add constraint u unique (note)")
    assert [column for column, _ in key()] == ["ORDER_ID"]
    cur.execute("alter table orders drop constraint orders_pk")  # the old name: no effect
    assert len(key()) == 1
    cur.execute("alter table orders add constraint named_pk primary key (order_id)")
    cur.execute("alter table orders drop constraint named_pk")
    assert key() == []
    cur.execute("drop table orders")
    cur.execute("create table orders (id int)")
    assert key() == []  # a new table of the same name starts without the old key


def test_swap_with(cur):
    cur.execute("insert into orders (id) values (1)")
    cur.execute("comment on table orders is 'live'")
    cur.execute("create table orders_next (id int primary key, extra varchar)")
    cur.execute("insert into orders_next values (2, 'x'), (3, 'y')")
    cur.execute("alter table orders swap with orders_next")
    assert cur.execute("select id from orders order by id").fetchall() == [(2,), (3,)]
    assert cur.execute("select id from orders_next").fetchall() == [(1,)]
    assert [row[0] for row in cur.execute("describe table orders")] == ["ID", "EXTRA"]
    assert [row[3] for row in cur.execute("show primary keys")] == ["ORDERS"]
    comments = {row[1]: row[5] for row in cur.execute("show tables")}
    assert comments == {"ORDERS": "", "ORDERS_NEXT": "live"}
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("alter table orders swap with missing")
    assert failure.value.errno == 2003


def test_rename_into_another_schema(cur):
    cur.execute("create schema archive")
    cur.execute("use schema public")
    cur.execute("insert into orders (id) values (1), (2)")
    cur.execute("comment on table orders is 'All orders'")
    cur.execute("comment on column orders.id is 'The key'")
    cur.execute("alter table orders add primary key (id)")
    cur.execute("alter table orders rename to archive.orders_2023")

    assert names(cur, "show tables in schema testdb.archive") == ["ORDERS_2023"]
    assert names(cur, "show tables in schema testdb.public") == []
    cur.execute("select id, note from archive.orders_2023 order by id")
    assert cur.fetchall() == [(1, "n/a"), (2, "n/a")]  # rows and the column default moved
    row = cur.connection.cursor(DictCursor).execute("describe table archive.orders_2023").fetchone()
    assert (row["name"], row["null?"], row["primary key"], row["comment"]) == (
        "ID",
        "N",
        "Y",
        "The key",
    )
    cur.execute("show tables in schema testdb.archive")
    assert cur.fetchone()[5] == "All orders"

    with pytest.raises(ProgrammingError) as failure:
        cur.execute("alter table archive.orders_2023 rename to nowhere.orders")
    assert failure.value.errno == 2003
    cur.execute("create database other")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("alter table testdb.archive.orders_2023 rename to other.public.orders")
    assert failure.value.errno == 2


def test_a_view_keeps_reading_its_tables_after_moving(cur):
    cur.execute("create schema reporting")
    cur.execute("use schema public")
    cur.execute("insert into orders (id) values (7)")
    cur.execute("alter view open_orders rename to reporting.open_orders")
    cur.execute("create table reporting.orders (id int)")  # must not capture the view
    assert cur.execute("select * from reporting.open_orders").fetchall() == [(7,)]
