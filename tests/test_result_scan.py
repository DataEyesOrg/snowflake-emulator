"""RESULT_SCAN and LAST_QUERY_ID: reading the result of an earlier statement again."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from snowflake.connector import ProgrammingError

LAST = "table(result_scan(last_query_id()))"
VALUES = (
    "select 1 as n, 2.50::number(10, 2) as d, 'text' as s, true as b, "
    "'2024-02-29'::date as day, '2024-02-29 12:30:00'::timestamp_ntz as at, null as nothing"
)


@pytest.fixture
def cur(conn):
    return conn.cursor()


@pytest.fixture(params=["ARROW", "JSON"])
def result_format(request, cur) -> str:
    cur.execute(f"alter session set python_connector_query_result_format = '{request.param}'")
    return request.param


def test_show_output_can_be_queried(cur):
    cur.execute("create table orders (id int)")
    cur.execute("create table customers (id int)")
    cur.execute("create view open_orders as select id from orders")
    cur.execute("show objects")
    # SHOW names its columns in lower case, so they need quoting, as on Snowflake.
    cur.execute(f'select "name", "kind" from {LAST} where "kind" = \'TABLE\' order by "name"')
    assert cur.fetchall() == [("CUSTOMERS", "TABLE"), ("ORDERS", "TABLE")]
    cur.execute("show tables")
    cur.execute(f'select count(*), min("created_on") <= current_timestamp() from {LAST}')
    assert cur.fetchall() == [(2, True)]


def test_values_and_types_survive(cur, result_format):
    expected = [
        (1, Decimal("2.50"), "text", True, dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 12, 30))
        + (None,)
    ]
    assert cur.execute(VALUES).fetchall() == expected
    described = [(c.name, c.type_code, c.precision, c.scale) for c in cur.description]
    cur.execute(f"select * from {LAST}")
    assert cur.fetchall() == expected
    assert [(c.name, c.type_code, c.precision, c.scale) for c in cur.description] == described


def test_a_large_result_is_read_whole(cur, result_format):
    cur.execute("select seq4() as n, 'row ' || n as label from table(generator(rowcount => 5000))")
    query_id = cur.sfqid
    cur.execute("select 1")  # no longer the last query: name it by id
    cur.execute(f"select count(*), sum(n), max(label) from table(result_scan('{query_id}')) as r")
    assert cur.fetchall() == [(5000, 12497500, "row 999")]


def test_result_scan_joins_and_feeds_other_statements(cur):
    cur.execute("create table orders (id int, customer varchar)")
    cur.execute("insert into orders values (1, 'ann'), (2, 'bob'), (3, 'ann')")
    cur.execute("select customer, count(*) as orders from orders group by customer")
    counts = cur.sfqid
    cur.execute(
        f"select o.id, c.orders from orders o join table(result_scan('{counts}')) c "
        "on c.customer = o.customer order by o.id"
    )
    assert cur.fetchall() == [(1, 2), (2, 1), (3, 2)]
    cur.execute(f"create table counts as select * from table(result_scan('{counts}'))")
    cur.execute(f"insert into counts select * from result_scan('{counts}')")  # TABLE() is optional
    assert cur.execute("select count(*), sum(orders) from counts").fetchall() == [(4, 6)]
    # The temporary copies behind RESULT_SCAN never show up in the catalog.
    assert [row[1] for row in cur.execute("show tables")] == ["COUNTS", "ORDERS"]


def test_dml_results_can_be_scanned(cur):
    cur.execute("create table t (id int)")
    cur.execute("insert into t values (1), (2), (3)")
    cur.execute(f'select "number of rows inserted" + 1 from {LAST}')
    assert cur.fetchall() == [(4,)]


def test_last_query_id(cur):
    first = cur.execute("select 'first'").sfqid
    second = cur.execute("select 'second'").sfqid
    assert cur.execute("select last_query_id()").fetchall() == [(second,)]
    # The SELECT LAST_QUERY_ID() above is now the latest, so -3 reaches back to "first".
    assert cur.execute("select last_query_id(-3), last_query_id(1)").fetchall() == [(first, first)]
    assert cur.execute("select last_query_id(2)").fetchall() == [(second,)]
    assert cur.execute("select last_query_id(-50), last_query_id(50)").fetchall() == [(None, None)]
    cur.execute("select * from table(result_scan(last_query_id(1)))")
    assert cur.fetchall() == [("first",)]


def test_last_query_id_belongs_to_the_session(conn, server):
    import snowflake.connector

    conn.cursor().execute("select 1")
    other = snowflake.connector.connect(**server.connection_parameters(database="testdb"))
    try:
        assert other.cursor().execute("select last_query_id()").fetchall() == [(None,)]
    finally:
        other.close()


def test_unknown_results_are_refused(cur):
    for sql in (
        "select * from table(result_scan('01b2c3d4-0000-0000-0000-000000000000'))",
        "select * from table(result_scan(last_query_id(-50)))",
    ):
        with pytest.raises(ProgrammingError) as failure:
            cur.execute(sql)
        assert failure.value.errno == 709
    cur.execute("select 1 as n")
    with pytest.raises(ProgrammingError) as failure:
        cur.execute(f"create view v as select * from {LAST}")
    assert failure.value.errno == 2
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select * from table(result_scan(upper('x')))")
    assert failure.value.errno == 2


def test_result_scan_inside_a_transaction(cur):
    cur.execute("create table t (id int)")
    seven = cur.execute("select 7 as id").sfqid
    cur.execute("begin")  # now the last query, so the result is named by its id
    cur.execute(f"insert into t select id from table(result_scan('{seven}'))")
    assert cur.execute(f"select count(*) from t join {LAST} on true").fetchall() == [(1,)]
    cur.execute("rollback")
    assert cur.execute("select count(*) from t").fetchall() == [(0,)]
    cur.execute("select 8 as id")
    cur.execute(f"insert into t select id from {LAST}")
    assert cur.execute("select id from t").fetchall() == [(8,)]


def test_result_scan_between_the_statements_of_one_request(cur):
    cur.execute("create table t (id int)")
    cur.execute(f'show tables; select "name" from {LAST}', num_statements=2)
    assert cur.nextset() is not None
    assert cur.fetchall() == [("T",)]


def test_results_are_fetched_again_by_query_id(cur):
    cur.execute("select seq4() as n from table(generator(rowcount => 3)) order by n")
    query_id = cur.sfqid
    again = cur.connection.cursor()
    again.get_results_from_sfqid(query_id)
    assert again.fetchall() == [(0,), (1,), (2,)]
