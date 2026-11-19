"""Endpoints the Python connector does not reach in a short test, over plain HTTP."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

import pytest

from sfemu.testing import RunningServer


def call(server: RunningServer, path: str, body: Any, token: str | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        f"http://{server.host}:{server.port}{path}",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f'Snowflake Token="{token}"',
        },
    )
    # The emulator is local; never route these calls through a configured proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request) as response:
        return json.load(response)


def login(server: RunningServer) -> dict[str, Any]:
    body = {"data": {"LOGIN_NAME": "test", "PASSWORD": "test", "ACCOUNT_NAME": "test"}}
    return call(server, "/session/v1/login-request", body)["data"]


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
