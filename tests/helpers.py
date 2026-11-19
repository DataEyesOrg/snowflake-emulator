"""Plain HTTP calls to the emulator, for what the Python connector does not reach."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from sfemu.testing import RunningServer


def call(server: RunningServer, path: str, body: Any, token: str | None = None) -> dict[str, Any]:
    # A body of None makes it a GET, as drivers use for results and query status.
    request = urllib.request.Request(
        f"http://{server.host}:{server.port}{path}",
        data=None if body is None else json.dumps(body).encode(),
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
