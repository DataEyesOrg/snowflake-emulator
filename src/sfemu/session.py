"""Sessions: what a logged-in driver connection carries between requests."""

from __future__ import annotations

import contextlib
import itertools
import secrets
import threading
from dataclasses import dataclass, field
from typing import Any

import duckdb

from sfemu import errors
from sfemu.engine import DEFAULT_SCHEMA, Engine, quote

# Parameters drivers read from the login and query responses. UTC is a
# deliberate deviation from Snowflake's America/Los_Angeles default.
DEFAULT_PARAMETERS: dict[str, Any] = {
    "TIMEZONE": "UTC",
    "AUTOCOMMIT": True,
    "CLIENT_SESSION_KEEP_ALIVE": False,
    "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY": 3600,
    "CLIENT_RESULT_CHUNK_SIZE": 160,
    "CLIENT_PREFETCH_THREADS": 4,
    # Drivers upload array binds this large to a stage instead of sending them
    # inline; 0 turns that off, as there are no stages yet. (Snowflake: 65280.)
    "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD": 0,
    "CLIENT_TELEMETRY_ENABLED": False,
    "QUERY_RESULT_FORMAT": "JSON",
    "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "JSON",
    "BINARY_OUTPUT_FORMAT": "HEX",
    "DATE_OUTPUT_FORMAT": "YYYY-MM-DD",
    "TIME_OUTPUT_FORMAT": "HH24:MI:SS",
    "TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
    "TIMESTAMP_NTZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3",
    "TIMESTAMP_LTZ_OUTPUT_FORMAT": "",
    "TIMESTAMP_TZ_OUTPUT_FORMAT": "",
}


@dataclass
class Session:
    id: int
    token: str
    master_token: str
    user: str
    account: str
    cursor: duckdb.DuckDBPyConnection
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    parameters: dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_PARAMETERS))
    lock: threading.Lock = field(default_factory=threading.Lock)
    # Whether BEGIN opened a transaction that is still running. Tracked here
    # because asking DuckDB (a second BEGIN) would abort the transaction.
    in_transaction: bool = False
    # DuckDB's temporary tables live outside any schema, Snowflake's inside one.
    # They are therefore ordinary tables that the session drops when it closes.
    temporary_tables: set[tuple[str, str, str]] = field(default_factory=set)

    def use(self, database: str | None, schema: str | None) -> None:
        """Point both the session and its DuckDB cursor at a database and schema."""
        self.database, self.schema = database, schema
        if database is None:
            self.cursor.execute("USE memory")
        elif schema is None:
            self.cursor.execute(f"USE {quote(database)}")
        else:
            self.cursor.execute(f"USE {quote(database)}.{quote(schema)}")

    def parameter_list(self) -> list[dict[str, Any]]:
        return [{"name": name, "value": value} for name, value in self.parameters.items()]


class SessionManager:
    def __init__(self, engine: Engine, auto_create: bool = True) -> None:
        self._engine = engine
        self._auto_create = auto_create
        self._by_token: dict[str, Session] = {}
        self._ids = itertools.count(1_000_000_001)
        self._lock = threading.Lock()

    def login(
        self,
        *,
        user: str,
        account: str,
        database: str | None,
        schema: str | None,
        warehouse: str | None,
        role: str | None,
        parameters: dict[str, Any] | None = None,
    ) -> Session:
        session = Session(
            id=next(self._ids),
            token=secrets.token_urlsafe(32),
            master_token=secrets.token_urlsafe(32),
            user=user,
            account=account,
            cursor=self._engine.cursor(),
            warehouse=warehouse,
            role=role or "PUBLIC",
        )
        # Snowflake applies the login parameters and echoes them back; that
        # echo is how a driver learns the server agreed to e.g. Arrow results.
        session.parameters.update({str(k).upper(): v for k, v in (parameters or {}).items()})
        timezone = session.parameters.get("TIMEZONE")
        if isinstance(timezone, str):
            try:
                session.cursor.execute("SET TimeZone = ?", [timezone])
            except duckdb.Error as exc:
                raise errors.from_duckdb(exc) from exc
        self._enter_namespace(session, database, schema)
        with self._lock:
            self._by_token[session.token] = session
        return session

    def _enter_namespace(self, session: Session, database: str | None, schema: str | None) -> None:
        # Like Snowflake, a login naming a missing database still succeeds; the
        # session just has no current database. auto_create (the default) makes
        # the namespace instead, which is what test setups usually want.
        if database is None:
            return
        if not self._engine.has_database(database):
            if not self._auto_create:
                return
            self._engine.create_database(database)
        schema = schema or DEFAULT_SCHEMA
        if not self._engine.has_schema(database, schema):
            if not self._auto_create:
                session.use(database, None)
                return
            session.cursor.execute(f"CREATE SCHEMA {quote(database)}.{quote(schema)}")
        session.use(database, schema)

    def get(self, token: str | None) -> Session | None:
        if not token:
            return None
        with self._lock:
            return self._by_token.get(token)

    def renew(self, master_token: str | None) -> Session | None:
        """Issue a new session token to the session holding this master token."""
        with self._lock:
            session = next(
                (s for s in self._by_token.values() if s.master_token == master_token), None
            )
            if session is not None:
                del self._by_token[session.token]
                session.token = secrets.token_urlsafe(32)
                self._by_token[session.token] = session
            return session

    def close(self, session: Session) -> None:
        with self._lock:
            self._by_token.pop(session.token, None)
        with session.lock:
            for database, schema, name in session.temporary_tables:
                with contextlib.suppress(duckdb.Error):
                    session.cursor.execute(
                        f"DROP TABLE IF EXISTS {quote(database)}.{quote(schema)}.{quote(name)}"
                    )
            session.cursor.close()
