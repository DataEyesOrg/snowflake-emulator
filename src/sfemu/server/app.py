"""The HTTP surface Snowflake drivers talk to.

These are the endpoints of Snowflake's driver protocol, as observed from the
open-source connectors. Every response uses the same envelope:
``{"success": bool, "code": str | None, "message": str | None, "data": ...}``.
"""

from __future__ import annotations

import base64
import gzip
import json
import logging
import re
import time
from typing import Any

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
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
    results = executor.results

    def authenticate(request: Request) -> Session:
        session = sessions.get(_token(request))
        if session is None:
            raise errors.session_gone()
        return session

    async def login(request: Request) -> JSONResponse:
        data = (await _body(request)).get("data") or {}
        query = request.query_params
        parameters = data.get("SESSION_PARAMETERS")
        session = await run_in_threadpool(
            sessions.login,
            user=str(data.get("LOGIN_NAME") or "").upper(),
            account=str(data.get("ACCOUNT_NAME") or "").upper(),
            database=_object_name(query.get("databaseName")),
            schema=_object_name(query.get("schemaName")),
            warehouse=_object_name(query.get("warehouse")),
            role=_object_name(query.get("roleName")),
            parameters=parameters if isinstance(parameters, dict) else None,
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
        result = await run_in_threadpool(
            executor.execute,
            session,
            sql,
            body.get("bindings"),
            describe_only=bool(body.get("describeOnly")),
            statement_count=_statement_count(body),
        )
        return _ok(_query_data(request, session, result))

    async def query_result(request: Request) -> JSONResponse:
        session = authenticate(request)
        result = results.get(request.path_params["query_id"])
        if result is None:
            raise errors.query_not_found(request.path_params["query_id"])
        return _ok(_query_data(request, session, result))

    async def query_status(request: Request) -> JSONResponse:
        # Statements run to completion inside the request, so a known query has
        # always succeeded; a failed one was never stored.
        authenticate(request)
        # Connector 5.x asks for /monitoring/queries//<id>, with a doubled slash.
        query_id = request.path_params["query_id"].strip("/")
        known = results.get(query_id) is not None
        queries = [{"id": query_id, "status": "SUCCESS", "state": "SUCCEEDED"}] if known else []
        return _ok({"queries": queries})

    async def result_chunk(request: Request) -> Response:
        # Chunk URLs carry an unguessable query id, so like Snowflake's own
        # presigned chunk downloads no further credential is checked.
        chunk = results.chunk(request.path_params["query_id"], int(request.path_params["index"]))
        if chunk is None:
            return JSONResponse({"success": False, "code": "404001", "message": "Not found"}, 404)
        if chunk.arrow is not None:
            # Snowflake serves arrow chunks gzip-compressed; drivers sniff the
            # magic bytes rather than the Content-Encoding header.
            return Response(
                gzip.compress(chunk.arrow),
                media_type="application/octet-stream",
                headers={"Content-Encoding": "gzip"},
            )
        # JSON chunks are the result rows as concatenated JSON arrays, with no
        # outer bracket: drivers wrap them in one ("[" + body + "]") or stream
        # the arrays one by one.
        rows = [[cell for cell in row] for row in chunk.rows or []]
        body = gzip.compress(json.dumps(rows)[1:-1].encode())
        return Response(body, media_type="application/json", headers={"Content-Encoding": "gzip"})

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

    def guarded(endpoint: Any) -> Any:
        # A bug in the emulator must reach the driver as a Snowflake error. An
        # HTTP 500 would make it retry the statement until its timeout instead.
        async def wrapper(request: Request) -> Response:
            try:
                return await endpoint(request)
            except SnowflakeError:
                raise
            except Exception as exc:
                log.exception("unhandled error in %s", request.url.path)
                failure = errors.internal(f"{type(exc).__name__}: {exc}")
                return await on_snowflake_error(request, failure)

        return wrapper

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
            Route("/session/v1/login-request", guarded(login), methods=["POST"]),
            Route("/queries/v1/query-request", guarded(query), methods=["POST"]),
            Route("/queries/{query_id}/result", guarded(query_result), methods=["GET", "POST"]),
            Route("/monitoring/queries/{query_id:path}", guarded(query_status), methods=["GET"]),
            Route("/session", guarded(session_endpoint), methods=["POST"]),
            Route("/session/token-request", guarded(renew_token), methods=["POST"]),
            Route("/session/heartbeat", acknowledge, methods=["GET", "POST"]),
            Route("/telemetry/send", acknowledge, methods=["POST"]),
            Route("/results/{query_id}/{index:int}", result_chunk, methods=["GET"]),
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
    try:
        body = json.loads(raw)
    except ValueError as exc:
        raise errors.bad_request("the body is not JSON") from exc
    if not isinstance(body, dict):
        raise errors.bad_request("the body must be a JSON object")
    return body


def _statement_count(body: dict[str, Any]) -> int | None:
    """The MULTI_STATEMENT_COUNT a request carries, if any."""
    parameters = body.get("parameters")
    if not isinstance(parameters, dict) or "MULTI_STATEMENT_COUNT" not in parameters:
        return None
    try:
        return max(0, int(parameters["MULTI_STATEMENT_COUNT"]))
    except (TypeError, ValueError) as exc:
        raise errors.bad_request("MULTI_STATEMENT_COUNT must be a number") from exc


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


def _query_data(request: Request, session: Session, result: QueryResult) -> dict[str, Any]:
    data: dict[str, Any] = {
        "parameters": session.parameter_list(),
        "rowtype": [column.to_rowtype() for column in result.columns],
        "total": result.total,
        "queryId": result.query_id,
        "sqlState": "00000",
        "databaseProvider": None,
        "finalDatabaseName": session.database,
        "finalSchemaName": session.schema,
        "finalWarehouseName": session.warehouse,
        "finalRoleName": session.role,
        "numberOfBinds": result.bind_count,
        "arrayBindSupported": result.array_bind_supported,
        "statementTypeId": result.statement_type_id,
        "version": 1,
        "sendResultTime": int(time.time() * 1000),
        "queryAbortsAfterSecs": 0,
    }
    if result.arrow is not None:
        data["queryResultFormat"] = "arrow"
        data["rowsetBase64"] = base64.b64encode(result.arrow).decode()
    else:
        data["queryResultFormat"] = "json"
        data["rowset"] = result.rows
        data["returned"] = len(result.rows)
    if result.children:
        # A multi-statement request: drivers fetch each of these by its id.
        data["resultIds"] = ",".join(child.query_id for child in result.children)
        data["resultTypes"] = ",".join(str(child.statement_type_id) for child in result.children)
    if result.chunks:
        base = str(request.base_url).rstrip("/")
        data["chunks"] = [
            {
                "url": f"{base}/results/{result.query_id}/{index}",
                "rowCount": chunk.row_count,
                "uncompressedRows": chunk.row_count,
                "uncompressedSize": len(chunk.arrow) if chunk.arrow is not None else 0,
                "compressedSize": len(chunk.arrow) if chunk.arrow is not None else 0,
            }
            for index, chunk in enumerate(result.chunks)
        ]
        # Drivers replay these headers verbatim on chunk downloads.
        data["chunkHeaders"] = {"Authorization": f'Snowflake Token="{session.token}"'}
    return data
