"""The HTTP surface Snowflake drivers talk to.

These are the endpoints of Snowflake's driver protocol, as observed from the
open-source connectors. Every response uses the same envelope:
``{"success": bool, "code": str | None, "message": str | None, "data": ...}``.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import time
from typing import Any

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from sfemu import __version__, errors
from sfemu.engine import Engine
from sfemu.errors import SnowflakeError
from sfemu.session import Session, SessionManager
from sfemu.sql.executor import Executor, QueryResult

log = logging.getLogger("sfemu")

# The Snowflake release we claim to be; drivers only display it.
SERVER_VERSION = "9.0.0"
_TOKEN = re.compile(r'Snowflake Token="([^"]*)"')
_HOUR = 3600


def create_app(*, auto_create: bool = True) -> Starlette:
    engine = Engine()
    sessions = SessionManager(engine, auto_create=auto_create)
    executor = Executor(engine)

    def authenticate(request: Request) -> Session:
        session = sessions.get(_token(request))
        if session is None:
            raise errors.session_gone()
        return session

    async def login(request: Request) -> JSONResponse:
        data = (await _body(request)).get("data") or {}
        query = request.query_params
        session = await run_in_threadpool(
            sessions.login,
            user=str(data.get("LOGIN_NAME") or "").upper(),
            account=str(data.get("ACCOUNT_NAME") or "").upper(),
            database=_object_name(query.get("databaseName")),
            schema=_object_name(query.get("schemaName")),
            warehouse=_object_name(query.get("warehouse")),
            role=_object_name(query.get("roleName")),
        )
        log.info("login user=%s session=%s", session.user, session.id)
        return _ok(
            {
                "masterToken": session.master_token,
                "token": session.token,
                "validityInSeconds": _HOUR,
                "masterValidityInSeconds": 4 * _HOUR,
                "displayUserName": session.user,
                "serverVersion": SERVER_VERSION,
                "firstLogin": False,
                "healthCheckInterval": 45,
                "sessionId": session.id,
                "parameters": session.parameter_list(),
                "sessionInfo": _session_info(session),
            }
        )

    async def query(request: Request) -> JSONResponse:
        session = authenticate(request)
        body = await _body(request)
        sql = body.get("sqlText") or ""
        log.debug("query session=%s sql=%s", session.id, sql)
        result = await run_in_threadpool(executor.execute, session, sql, body.get("bindings"))
        return _ok(_query_data(session, result))

    async def session_endpoint(request: Request) -> JSONResponse:
        session = authenticate(request)
        if request.query_params.get("delete") == "true":
            await run_in_threadpool(sessions.close, session)
            log.info("logout session=%s", session.id)
        return _ok(None)

    async def renew_token(request: Request) -> JSONResponse:
        # Unlike every other call, renewal is authorized by the master token.
        session = sessions.renew(_token(request))
        if session is None:
            raise errors.session_gone()
        return _ok(
            {
                "sessionToken": session.token,
                "validityInSecondsST": _HOUR,
                "masterToken": session.master_token,
                "validityInSecondsMT": 4 * _HOUR,
                "sessionId": session.id,
            }
        )

    async def acknowledge(request: Request) -> JSONResponse:
        return _ok(None)

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "version": __version__})

    async def on_snowflake_error(request: Request, exc: SnowflakeError) -> JSONResponse:
        # Drivers expect HTTP 200 with success=false; the error is in the envelope.
        return JSONResponse(
            {
                "success": False,
                "code": exc.code,
                "message": exc.message,
                "data": {"errorCode": exc.code, "sqlState": exc.sqlstate},
            }
        )

    app = Starlette(
        routes=[
            Route("/session/v1/login-request", login, methods=["POST"]),
            Route("/queries/v1/query-request", query, methods=["POST"]),
            Route("/session", session_endpoint, methods=["POST"]),
            Route("/session/token-request", renew_token, methods=["POST"]),
            Route("/session/heartbeat", acknowledge, methods=["GET", "POST"]),
            Route("/telemetry/send", acknowledge, methods=["POST"]),
            Route("/health", health, methods=["GET"]),
        ],
        exception_handlers={SnowflakeError: on_snowflake_error},
    )
    app.state.engine = engine
    return app


def _token(request: Request) -> str | None:
    match = _TOKEN.search(request.headers.get("authorization", ""))
    return match.group(1) if match else None


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    if request.headers.get("content-encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _object_name(raw: str | None) -> str | None:
    """Apply Snowflake's identifier rule to a name sent as a connection parameter."""
    if not raw:
        return None
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        return raw[1:-1].replace('""', '"')
    return raw.upper()


def _ok(data: Any) -> JSONResponse:
    return JSONResponse({"success": True, "code": None, "message": None, "data": data})


def _session_info(session: Session) -> dict[str, str | None]:
    return {
        "databaseName": session.database,
        "schemaName": session.schema,
        "warehouseName": session.warehouse,
        "roleName": session.role,
    }


def _query_data(session: Session, result: QueryResult) -> dict[str, Any]:
    return {
        "parameters": session.parameter_list(),
        "rowtype": [column.to_rowtype() for column in result.columns],
        "rowset": result.rows,
        "total": len(result.rows),
        "returned": len(result.rows),
        "queryId": result.query_id,
        "sqlState": "00000",
        "databaseProvider": None,
        "finalDatabaseName": session.database,
        "finalSchemaName": session.schema,
        "finalWarehouseName": session.warehouse,
        "finalRoleName": session.role,
        "numberOfBinds": 0,
        "arrayBindSupported": False,
        "statementTypeId": result.statement_type_id,
        "version": 1,
        "sendResultTime": int(time.time() * 1000),
        "queryResultFormat": "json",
    }
