"""Several statements in one request, and results fetched again by query id."""

from __future__ import annotations

import pytest
from snowflake.connector import ProgrammingError

from helpers import call, login


def result_sets(cur) -> list[list[tuple]]:
    found = [cur.fetchall()]
    while cur.nextset() is not None:
        found.append(cur.fetchall())
    return found


def test_statements_run_in_order_and_each_has_a_result(conn):
    cur = conn.cursor()
    script = "create table t (id int); insert into t values (1), (2); select * from t order by id"
    cur.execute(script, num_statements=3)
    assert len(cur.multi_statement_savedIds) == 3
    assert result_sets(cur) == [
        [("Table T successfully created.",)],
        [(2,)],
        [(1,), (2,)],
    ]


def test_the_statement_count_must_match(conn):
    cur = conn.cursor()
    for script, count in (("select 1; select 2", 3), ("select 1; select 2", 1), ("select 1", 2)):
        with pytest.raises(ProgrammingError) as failure:
            cur.execute(script, num_statements=count)
        assert failure.value.errno == 8
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select 1; select 2")  # one statement per request by default
    assert failure.value.errno == 8


def test_any_number_of_statements(conn):
    cur = conn.cursor()
    assert result_sets(cur.execute("select 1; select 'x'", num_statements=0)) == [[(1,)], [("x",)]]
    assert result_sets(cur.execute("select 5", num_statements=0)) == [[(5,)]]

    cur.execute("alter session set multi_statement_count = 0")
    assert result_sets(cur.execute("select 1; select 2; select 3")) == [[(1,)], [(2,)], [(3,)]]


def test_a_failing_statement_keeps_what_ran_before_it(conn):
    cur = conn.cursor()
    cur.execute("create table t (id int)")
    script = "insert into t values (1); select * from missing; insert into t values (2)"
    with pytest.raises(ProgrammingError) as failure:
        cur.execute(script, num_statements=3)
    assert failure.value.errno == 2003
    assert cur.execute("select id from t").fetchall() == [(1,)]


def test_large_results_inside_a_script(server):
    import snowflake.connector

    parameters = server.connection_parameters(
        database="d", session_parameters={"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "arrow"}
    )
    with snowflake.connector.connect(**parameters) as conn:
        script = "select seq4() n from table(generator(rowcount => 1000)); select 1"
        first, second = result_sets(conn.cursor().execute(script, num_statements=2))
        assert sorted(first) == [(n,) for n in range(1000)]
        assert second == [(1,)]


def test_binds_are_dealt_out_statement_by_statement(server):
    token = login(server)["token"]
    query = {
        "sqlText": "select ?, ?; select ?",
        "parameters": {"MULTI_STATEMENT_COUNT": 2},
        "bindings": {
            "1": {"type": "FIXED", "value": "1"},
            "2": {"type": "TEXT", "value": "two"},
            "3": {"type": "FIXED", "value": "3"},
        },
    }
    parent = call(server, "/queries/v1/query-request", query, token=token)["data"]
    assert parent["rowset"] == [["Multiple statements executed successfully."]]
    assert parent["resultTypes"] == "4096,4096"
    rowsets = [
        call(server, f"/queries/{query_id}/result", None, token=token)["data"]["rowset"]
        for query_id in parent["resultIds"].split(",")
    ]
    assert rowsets == [[["1", "two"]], [["3"]]]


def test_results_and_status_by_query_id(server):
    token = login(server)["token"]
    query = {"sqlText": "select 42 as answer"}
    query_id = call(server, "/queries/v1/query-request", query, token=token)["data"]["queryId"]

    again = call(server, f"/queries/{query_id}/result", None, token=token)
    assert again["data"]["rowset"] == [["42"]]
    # Connector 5.x doubles the slash before the id.
    for path in (f"/monitoring/queries/{query_id}", f"/monitoring/queries//{query_id}"):
        status = call(server, path, None, token=token)
        assert [q["status"] for q in status["data"]["queries"]] == ["SUCCESS"]

    unknown = "00000000-0000-0000-0000-000000000000"
    missing = call(server, f"/queries/{unknown}/result", None, token=token)
    assert (missing["success"], missing["code"]) == (False, "000709")
    assert call(server, f"/monitoring/queries/{unknown}", None, token=token)["data"] == {
        "queries": []
    }
    refused = call(server, f"/queries/{query_id}/result", None, token="bogus")
    assert refused["code"] == "390111"
