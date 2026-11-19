"""Endpoints the Python connector does not reach in a short test, over plain HTTP."""

from __future__ import annotations

from typing import Any

import pytest

from helpers import call, login


def test_query_without_a_session_is_refused(server):
    reply = call(server, "/queries/v1/query-request", {"sqlText": "select 1"}, token="bogus")
    assert reply["success"] is False
    assert reply["code"] == "390111"


def test_token_renewal_replaces_the_session_token(server):
    session = login(server)
    old_token = session["token"]

    renewal = {"oldSessionToken": old_token, "requestType": "RENEW"}
    renewed = call(server, "/session/token-request", renewal, token=session["masterToken"])
    new_token = renewed["data"]["sessionToken"]
    assert new_token != old_token

    query = {"sqlText": "select 1"}
    assert call(server, "/queries/v1/query-request", query, token=new_token)["success"]
    assert not call(server, "/queries/v1/query-request", query, token=old_token)["success"]


def test_token_renewal_needs_the_master_token(server):
    session = login(server)
    reply = call(server, "/session/token-request", {}, token=session["token"])
    assert reply["success"] is False


def test_heartbeat(server):
    session = login(server)
    assert call(server, "/session/heartbeat", {}, token=session["token"])["success"]


@pytest.mark.parametrize(
    ("bind", "code"),
    [
        ({"type": "FIXED", "value": "abc"}, "100038"),
        ({"type": "REAL", "value": "abc"}, "100038"),
        ({"type": "BOOLEAN", "value": "maybe"}, "100038"),
        ("not an object", "390142"),
    ],
)
def test_malformed_binds_are_errors_not_http_500(server, bind, code):
    session = login(server)
    query = {"sqlText": "select ?", "bindings": {"1": bind}}
    reply = call(server, "/queries/v1/query-request", query, token=session["token"])
    assert reply["success"] is False
    assert reply["code"] == code


def test_malformed_body_is_an_error_not_http_500(server):
    session = login(server)
    reply = call(server, "/queries/v1/query-request", [1, 2], token=session["token"])
    assert reply["success"] is False
    assert reply["code"] == "390142"


def test_unexpected_failures_reach_the_driver_as_errors(server):
    # "data" of the wrong type trips an AttributeError inside the handler; any
    # such bug must come back in the envelope, since drivers retry on HTTP 500.
    reply = call(server, "/session/v1/login-request", {"data": [1, 2]})
    assert reply["success"] is False
    assert reply["code"] == "000603"


def test_describe_only_reports_columns_without_running(server):
    session = login(server)
    token = session["token"]

    def query(sql: str, **extra: Any) -> dict[str, Any]:
        return call(server, "/queries/v1/query-request", {"sqlText": sql, **extra}, token=token)

    assert query("create database d")["success"]
    assert query("create table t (id int, name varchar)")["success"]

    described = query("insert into t values (?, ?)", describeOnly=True)["data"]
    assert described["numberOfBinds"] == 2
    assert described["arrayBindSupported"] is True
    assert [c["name"] for c in described["rowtype"]] == ["number of rows inserted"]
    assert query("select count(*) from t")["data"]["rowset"] == [["0"]]

    described = query("select id, name from t where id = ?", describeOnly=True)["data"]
    assert [(c["name"], c["type"]) for c in described["rowtype"]] == [
        ("ID", "fixed"),
        ("NAME", "text"),
    ]
    assert described["rowset"] == []
    assert described["arrayBindSupported"] is False


def test_array_binds_run_the_statement_per_row(server):
    session = login(server)
    token = session["token"]
    call(server, "/queries/v1/query-request", {"sqlText": "create database d"}, token=token)
    create = {"sqlText": "create table t (id int, name varchar)"}
    call(server, "/queries/v1/query-request", create, token=token)

    insert = {
        "sqlText": "insert into t values (?, ?)",
        "bindings": {
            "1": {"type": "FIXED", "value": ["1", "2", "3"]},
            "2": {"type": "TEXT", "value": ["a", None, "c"]},
        },
    }
    reply = call(server, "/queries/v1/query-request", insert, token=token)
    assert reply["data"]["rowset"] == [["3"]]

    insert["bindings"]["2"]["value"] = ["a"]  # columns of different lengths
    reply = call(server, "/queries/v1/query-request", insert, token=token)
    assert (reply["success"], reply["code"]) == (False, "390142")
