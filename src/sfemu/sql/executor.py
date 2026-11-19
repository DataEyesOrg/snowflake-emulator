"""Runs one Snowflake statement for a session.

Statements are dispatched on their sqlglot type. Anything not listed here is
rejected with Snowflake's "Unsupported feature" error rather than handed to
DuckDB, so a statement never passes locally by accident.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import uuid
import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
from sqlglot import exp

from sfemu import errors
from sfemu.arrow import ipc_stream, snowflake_table
from sfemu.engine import DEFAULT_SCHEMA, Engine, quote
from sfemu.errors import SnowflakeError
from sfemu.session import DEFAULT_PARAMETERS, Session
from sfemu.sql import translate
from sfemu.types import Column, column_from_duckdb, encode_value, fixed_column, text_column

# Statement type ids as Snowflake reports them. Drivers classify by family
# (the top hex digit), so the family is what has to be right.
SELECT = 0x1000
INSERT = 0x3100
UPDATE = 0x3200
DELETE = 0x3300
ALTER_SESSION = 0x4100
USE = 0x4300
TRANSACTION = 0x5000
DDL = 0x6000

# Result formats a driver can ask for; SELECT answers in the requested one,
# everything else stays JSON (drivers read DML row counts from the rowset).
ARROW = "arrow"
JSON = "json"

_SUCCESS = "Statement executed successfully."
# DML statements with their statement type id and row-count columns.
_DML: list[tuple[type[exp.Expression], int, list[str]]] = [
    (exp.Insert, INSERT, ["number of rows inserted"]),
    (exp.Update, UPDATE, ["number of rows updated", "number of multi-joined rows updated"]),
    (exp.Delete, DELETE, ["number of rows deleted"]),
]
_RELATION_KINDS = {"TABLE": "Table", "VIEW": "View"}


@dataclass
class ResultChunk:
    """One downloadable part of a large result, served behind its chunk URL."""

    row_count: int
    arrow: bytes | None = None  # an Arrow IPC stream, when the result is Arrow
    rows: list[list[str | None]] | None = None  # JSON-encoded rows otherwise


@dataclass
class QueryResult:
    columns: list[Column]
    rows: list[list[str | None]]
    statement_type_id: int
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    total: int | None = None
    arrow: bytes | None = None  # the inline first chunk, as an Arrow IPC stream
    chunks: list[ResultChunk] = field(default_factory=list)
    bind_count: int = 0
    # Whether a driver may send this statement once with a column of values per
    # bind; drivers ask with a describe-only request before executemany().
    array_bind_supported: bool = False

    def __post_init__(self) -> None:
        if self.total is None:
            self.total = len(self.rows)


def _dml_shape(statement: exp.Expression) -> tuple[int, list[str]] | None:
    for kind, type_id, names in _DML:
        if isinstance(statement, kind):
            return type_id, names
    return None


def _status(message: str, statement_type_id: int) -> QueryResult:
    return QueryResult([text_column("status", nullable=False)], [[message]], statement_type_id)


class Executor:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def execute(
        self,
        session: Session,
        sql: str,
        bindings: dict[str, Any] | None = None,
        *,
        describe_only: bool = False,
    ) -> QueryResult:
        """Run one statement, or with describe_only just report its result columns."""
        statements = translate.parse(sql)
        if not statements:
            raise SnowflakeError("000900", "42601", "SQL compilation error:\nEmpty SQL statement.")
        if len(statements) > 1:
            raise SnowflakeError(
                "000008",
                "0A000",
                f"Actual statement count {len(statements)} "
                "did not match the desired statement count 1.",
            )
        statement = statements[0]
        binds = translate.bind_count(statement)
        rows = _bind_rows(bindings)
        with session.lock:
            try:
                if describe_only:
                    result = self._describe(session, statement, binds)
                elif rows is None:
                    result = self._dispatch(session, statement, _bind_values(bindings))
                else:
                    result = self._dispatch_rows(session, statement, rows)
            except duckdb.Error as exc:
                raise errors.from_duckdb(exc) from exc
        result.bind_count = binds
        result.array_bind_supported = isinstance(statement, exp.Insert)
        return result

    def _describe(self, session: Session, statement: exp.Expression, binds: int) -> QueryResult:
        """The columns a statement would return, without running it."""
        if isinstance(statement, exp.Query):
            # Zero rows of the query give DuckDB's column types; the binds have
            # no values yet, so they are described as NULLs.
            translate.name_projections(statement)
            probe = exp.select("*").from_(statement.subquery("_describe")).limit(0)
            self._run(session, probe, [None] * binds)
            description = session.cursor.description
            columns = [column_from_duckdb(d[0], d[1]) for d in description]
            return QueryResult(columns, [], SELECT, total=0)
        dml = _dml_shape(statement)
        if dml is not None:
            type_id, names = dml
            self._check(session, statement)
            columns = [fixed_column(name, nullable=False) for name in names]
            return QueryResult(columns, [], type_id, total=0)
        return QueryResult([text_column("status", nullable=False)], [], DDL, total=0)

    def _dispatch_rows(
        self, session: Session, statement: exp.Expression, rows: list[list]
    ) -> QueryResult:
        """An array bind: run a DML statement once per row of bind values."""
        dml = _dml_shape(statement)
        if dml is None:
            raise errors.unsupported(f"array bind for {_describe(statement)}")
        type_id, names = dml
        self._check(session, statement)
        sql = translate.to_duckdb(statement)
        cursor = session.cursor
        # All rows or none: use a transaction of our own unless one is open.
        try:
            cursor.execute("BEGIN")
            own_transaction = True
        except duckdb.TransactionException:
            own_transaction = False
        affected = 0
        try:
            for row in rows:
                affected += cursor.execute(sql, row).fetchone()[0]
        except Exception:
            if own_transaction:
                cursor.execute("ROLLBACK")
            raise
        if own_transaction:
            cursor.execute("COMMIT")
        counts = [str(affected)] + ["0"] * (len(names) - 1)
        columns = [fixed_column(name, nullable=False) for name in names]
        return QueryResult(columns, [counts], type_id)

    def _dispatch(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        if isinstance(statement, exp.Query):
            return self._query(session, statement, params)
        dml = _dml_shape(statement)
        if dml is not None:
            return self._dml(session, statement, params, *dml)
        if isinstance(statement, exp.Create):
            return self._create(session, statement, params)
        if isinstance(statement, exp.Drop):
            return self._drop(session, statement)
        if isinstance(statement, exp.TruncateTable):
            self._run(session, statement, params)
            return _status(_SUCCESS, DDL)
        if isinstance(statement, exp.Use):
            return self._use(session, statement)
        if isinstance(statement, exp.Alter) and statement.args.get("kind") == "SESSION":
            return self._alter_session(session, statement)
        if isinstance(statement, exp.Transaction | exp.Commit | exp.Rollback):
            return self._transaction(session, statement)
        raise errors.unsupported(_describe(statement))

    # Queries and DML -------------------------------------------------------

    def _run(
        self,
        session: Session,
        statement: exp.Expression,
        params: list,
        creating: exp.Table | None = None,
    ) -> None:
        self._check(session, statement, creating)
        session.cursor.execute(translate.to_duckdb(statement), params)

    def _check(
        self, session: Session, statement: exp.Expression, creating: exp.Table | None = None
    ) -> None:
        """Everything that must hold before a statement may reach DuckDB."""
        self._require_namespace(session, statement)
        translate.check_supported(statement)
        _resolve_context_functions(session, statement)
        for table in translate.relations(statement):
            if table is creating:
                continue
            database, schema, name = _qualify(session, table)
            unqualified = not table.db
            if not self._engine.relation_visible(
                session.cursor, database, schema, name, temporary=unqualified
            ):
                raise errors.object_not_found(f"Object '{name}' does not exist or not authorized.")

    def _query(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        translate.name_projections(statement)
        self._run(session, statement, params)
        cursor = session.cursor
        columns = [column_from_duckdb(d[0], d[1]) for d in cursor.description]
        chunk_rows = _chunk_rows(session)
        if _result_format(session) == ARROW:
            # Arrow output on a DB-API result copies once more than the
            # relational API would; that copy is the price of parameters.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                table = snowflake_table(cursor.to_arrow_table(), columns)
            if table.num_rows == 0:
                # Snowflake sends an empty rowsetBase64 for an empty result and
                # lets rowtype describe the columns. A zero-row record batch
                # instead segfaults the 3.x / 4.x Python connectors in fetchall().
                return QueryResult(columns, [], SELECT, total=0, arrow=b"")
            parts: list[pa.RecordBatch] = table.to_batches(max_chunksize=chunk_rows)
            arrow_chunks = [
                ResultChunk(row_count=batch.num_rows, arrow=ipc_stream([batch]))
                for batch in parts[1:]
            ]
            return QueryResult(
                columns,
                [],
                SELECT,
                total=table.num_rows,
                arrow=ipc_stream([parts[0]]),
                chunks=arrow_chunks,
            )
        rows = [
            [encode_value(value, column) for value, column in zip(row, columns, strict=True)]
            for row in cursor.fetchall()
        ]
        first, rest = rows[:chunk_rows], rows[chunk_rows:]
        chunks = [
            ResultChunk(row_count=len(rest[i : i + chunk_rows]), rows=rest[i : i + chunk_rows])
            for i in range(0, len(rest), chunk_rows)
        ]
        return QueryResult(columns, first, SELECT, total=len(rows), chunks=chunks)

    def _dml(
        self,
        session: Session,
        statement: exp.Expression,
        params: list,
        statement_type_id: int,
        names: list[str],
    ) -> QueryResult:
        self._run(session, statement, params)
        affected = session.cursor.fetchone()[0]
        # Extra counters (multi-joined rows) are not tracked by DuckDB; report zero.
        counts = [str(affected)] + ["0"] * (len(names) - 1)
        columns = [fixed_column(name, nullable=False) for name in names]
        return QueryResult(columns, [counts], statement_type_id)

    # DDL -------------------------------------------------------------------

    def _create(self, session: Session, statement: exp.Create, params: list) -> QueryResult:
        kind = statement.kind
        target = statement.this.this if isinstance(statement.this, exp.Schema) else statement.this
        if_not_exists = bool(statement.args.get("exists"))
        replace = bool(statement.args.get("replace"))

        if kind == "DATABASE":
            name = target.name
            if self._engine.has_database(name):
                if if_not_exists:
                    return _status(f"{name} already exists, statement succeeded.", DDL)
                if not replace:
                    raise _already_exists(name)
                self._drop_database(session, name)
            self._engine.create_database(name)
            session.use(name, DEFAULT_SCHEMA)
            return _status(f"Database {name} successfully created.", DDL)

        if kind == "SCHEMA":
            database, name = _schema_target(session, target, "CREATE SCHEMA")
            if self._engine.has_schema(database, name):
                if if_not_exists:
                    return _status(f"{name} already exists, statement succeeded.", DDL)
                if not replace:
                    raise _already_exists(name)
                # Replacing a schema drops it together with its contents.
                session.cursor.execute(f"DROP SCHEMA {quote(database)}.{quote(name)} CASCADE")
            statement.set("replace", False)  # DuckDB has no CREATE OR REPLACE SCHEMA
            session.cursor.execute(translate.to_duckdb(statement))
            session.use(database, name)  # like CREATE DATABASE, this moves the session
            return _status(f"Schema {name} successfully created.", DDL)

        if kind in _RELATION_KINDS:
            self._require_namespace(session, statement)
            database, schema, name = _qualify(session, target)
            exists = self._engine.has_relation(kind, database, schema, name)
            if exists and if_not_exists:
                return _status(f"{name} already exists, statement succeeded.", DDL)
            if exists and not replace:
                raise _already_exists(name)
            self._run(session, statement, params, creating=target)
            return _status(f"{_RELATION_KINDS[kind]} {name} successfully created.", DDL)

        raise errors.unsupported(f"CREATE {kind}")

    def _drop(self, session: Session, statement: exp.Drop) -> QueryResult:
        kind = statement.kind
        targets = statement.args.get("tables") or []
        if_exists = bool(statement.args.get("exists"))
        if len(targets) != 1 or not isinstance(targets[0], exp.Table):
            raise errors.unsupported(_describe(statement))
        target = targets[0]
        name = target.name

        if kind == "DATABASE":
            exists = self._engine.has_database(name)
            if exists:
                self._drop_database(session, name)
        elif kind == "SCHEMA":
            database, name = _schema_target(session, target, "DROP SCHEMA")
            exists = self._engine.has_schema(database, name)
            if exists:
                # Snowflake drops a schema together with its contents.
                statement.set("cascade", True)
                session.cursor.execute(translate.to_duckdb(statement))
                if (session.database, session.schema) == (database, name):
                    session.use(database, None)
        elif kind in _RELATION_KINDS:
            self._require_namespace(session, statement)
            exists = self._engine.has_relation(kind, *_qualify(session, target))
            if exists:
                session.cursor.execute(translate.to_duckdb(statement))
        else:
            raise errors.unsupported(f"DROP {kind}")

        if exists:
            return _status(f"{name} successfully dropped.", DDL)
        if if_exists:
            message = f"Drop statement executed successfully ({name} already dropped)."
            return _status(message, DDL)
        label = _RELATION_KINDS.get(kind, kind.capitalize())
        raise errors.object_not_found(f"{label} '{name}' does not exist or not authorized.")

    def _drop_database(self, session: Session, name: str) -> None:
        if session.database == name:
            session.use(None, None)
        self._engine.drop_database(name)

    # Session state ---------------------------------------------------------

    def _use(self, session: Session, statement: exp.Use) -> QueryResult:
        target = statement.this
        name = target.name
        # A bare USE names a database, unless the name is qualified (db.schema).
        kind = "SCHEMA" if target.db else "DATABASE"
        if statement.args.get("kind") is not None:
            kind = statement.args["kind"].name.upper()
        if kind == "DATABASE":
            if not self._engine.has_database(name):
                raise _cannot_use()
            has_public = self._engine.has_schema(name, DEFAULT_SCHEMA)
            session.use(name, DEFAULT_SCHEMA if has_public else None)
        elif kind == "SCHEMA":
            database = target.db or session.database
            if database is None or not self._engine.has_schema(database, name):
                raise _cannot_use()
            session.use(database, name)
        elif kind == "WAREHOUSE":
            session.warehouse = name
        elif kind == "ROLE":
            session.role = name
        else:
            raise errors.unsupported(f"USE {kind}")
        return _status(_SUCCESS, USE)

    def _alter_session(self, session: Session, statement: exp.Alter) -> QueryResult:
        for action in statement.args.get("actions") or []:
            unset = bool(action.args.get("unset"))
            for item in action.expressions:
                if unset:
                    # UNSET puts the parameter back to its default.
                    name = item.this.name.upper()
                    if name in DEFAULT_PARAMETERS:
                        session.parameters[name] = DEFAULT_PARAMETERS[name]
                    else:
                        session.parameters.pop(name, None)
                    if name == "TIMEZONE":
                        session.cursor.execute("SET TimeZone = ?", [session.parameters[name]])
                    continue
                assignment = item.this
                name = assignment.this.name.upper()
                value = _literal_value(assignment.expression)
                if name == "TIMEZONE":
                    session.cursor.execute("SET TimeZone = ?", [value])
                session.parameters[name] = value
        return _status(_SUCCESS, ALTER_SESSION)

    def _transaction(self, session: Session, statement: exp.Expression) -> QueryResult:
        # COMMIT or ROLLBACK with nothing open, or BEGIN inside a transaction:
        # Snowflake accepts all three, DuckDB refuses them.
        with contextlib.suppress(duckdb.TransactionException):
            session.cursor.execute(translate.to_duckdb(statement))
        return _status(_SUCCESS, TRANSACTION)

    def _require_namespace(self, session: Session, statement: exp.Expression) -> None:
        if session.database is not None and session.schema is not None:
            return
        needs_database, needs_schema = translate.unqualified_relations(statement)
        action = _describe(statement)
        if needs_database and session.database is None:
            raise _no_current("database", action)
        if needs_schema and session.schema is None:
            raise _no_current("schema", action)


def _resolve_context_functions(session: Session, statement: exp.Expression) -> None:
    """Replace CURRENT_DATABASE() and friends with this session's values."""
    values: dict[type[exp.Expression], Any] = {
        exp.CurrentDatabase: session.database,
        exp.CurrentSchema: session.schema,
        exp.CurrentWarehouse: session.warehouse,
        exp.CurrentRole: session.role,
        exp.CurrentUser: session.user,
        exp.CurrentAccount: session.account,
        exp.CurrentSession: str(session.id),
    }
    for node in list(statement.find_all(*values)):
        value = values[type(node)]
        node.replace(exp.Null() if value is None else exp.Literal.string(value))


def _schema_target(session: Session, target: exp.Table, action: str) -> tuple[str, str]:
    """Resolve the (database, schema) a CREATE or DROP SCHEMA names, and pin it in the AST."""
    # sqlglot parses a schema name as a table reference with no table part.
    database = target.catalog or session.database
    if database is None:
        raise _no_current("database", action)
    target.set("catalog", exp.to_identifier(database, quoted=True))
    return database, target.db


def _qualify(session: Session, table: exp.Table) -> tuple[str, str, str]:
    return table.catalog or session.database, table.db or session.schema, table.name


def _describe(statement: exp.Expression) -> str:
    kind = statement.args.get("kind")
    head = statement.key.upper()
    if isinstance(statement, exp.Command):
        return str(statement.this).upper()
    return f"{head} {kind}" if isinstance(kind, str) else head


def _literal_value(node: exp.Expression) -> Any:
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal) and not node.is_string:
        return int(node.this) if node.is_int else float(node.this)
    return node.name


def _bind_rows(bindings: Any) -> list[list] | None:
    """The rows of an array bind, or None when the binds are plain scalars.

    An array bind carries a list of values per bind, one entry per row.
    """
    if not isinstance(bindings, dict) or not any(
        isinstance(b, dict) and isinstance(b.get("value"), list) for b in bindings.values()
    ):
        return None
    columns = []
    for position in _bind_positions(bindings):
        binding = bindings[position]
        if not isinstance(binding, dict) or not isinstance(binding.get("value"), list):
            raise errors.bad_request("array binds must be arrays for every bind")
        kind = str(binding.get("type", "ANY")).upper()
        columns.append([_bind_value(kind, value) for value in binding["value"]])
    if len({len(column) for column in columns}) != 1:
        raise errors.bad_request("array binds must all have the same length")
    return [list(row) for row in zip(*columns, strict=True)]


def _bind_positions(bindings: dict[str, Any]) -> list[str]:
    try:
        return sorted(bindings, key=int)
    except ValueError as exc:
        raise errors.bad_request("binding positions must be numbers") from exc


def _bind_values(bindings: dict[str, Any] | None) -> list:
    """Turn the driver's positional (qmark) bindings into DuckDB parameters."""
    if not bindings:
        return []
    if not isinstance(bindings, dict):
        raise errors.bad_request("bindings must be an object")
    values = []
    for position in _bind_positions(bindings):
        binding = bindings[position]
        if not isinstance(binding, dict):
            raise errors.bad_request(f"binding {position} must be an object")
        kind = str(binding.get("type", "ANY")).upper()
        values.append(_bind_value(kind, binding.get("value")))
    return values


_EPOCH = dt.datetime(1970, 1, 1)
_NOT_RECOGNIZED = {
    "BOOLEAN": "Boolean",
    "DATE": "Date",
    "TIME": "Time",
    "BINARY": "Binary",
    "TIMESTAMP_NTZ": "Timestamp",
    "TIMESTAMP_LTZ": "Timestamp",
    "TIMESTAMP_TZ": "Timestamp",
}


def _bind_value(kind: str, value: Any) -> Any:
    """One bind value, sent as a string in Snowflake's wire encoding, as a Python value."""
    if value is None:
        return None
    if isinstance(value, list):
        raise errors.bad_request("a bind value cannot be a nested array")
    if kind in ("TEXT", "ANY"):
        return value
    if kind not in ("FIXED", "REAL", *_NOT_RECOGNIZED):
        raise errors.unsupported(f"bind type {kind}")
    try:
        return _bind_scalar(kind, str(value))
    except (ValueError, ArithmeticError) as exc:
        what = _NOT_RECOGNIZED.get(kind, "Numeric")
        raise SnowflakeError(
            "100038", "22018", f"{what} value '{value}' is not recognized"
        ) from exc


def _bind_scalar(kind: str, value: str) -> Any:
    if kind == "FIXED":
        return int(value) if value.lstrip("-").isdigit() else Decimal(value)
    if kind == "REAL":
        return float(value)
    if kind == "BOOLEAN":
        if value.lower() not in ("true", "false"):
            raise ValueError(value)
        return value.lower() == "true"
    if kind == "BINARY":
        return bytes.fromhex(value)
    if kind == "DATE":  # milliseconds since the epoch
        return (_EPOCH + dt.timedelta(milliseconds=int(value))).date()
    if kind == "TIME":  # nanoseconds since midnight
        return (_EPOCH + dt.timedelta(microseconds=int(value) // 1000)).time()
    # Timestamps: nanoseconds since the epoch, in UTC. TIMESTAMP_TZ may add
    # the zone as " <offset in minutes + 1440>".
    nanos, _, zone = value.partition(" ")
    moment = _EPOCH + dt.timedelta(microseconds=int(nanos) // 1000)
    if kind == "TIMESTAMP_NTZ":
        return moment
    moment = moment.replace(tzinfo=dt.UTC)
    if zone:
        moment = moment.astimezone(dt.timezone(dt.timedelta(minutes=int(zone) - 1440)))
    return moment


def _already_exists(name: str) -> SnowflakeError:
    return SnowflakeError(
        "002002", "42710", f"SQL compilation error:\nObject '{name}' already exists."
    )


def _cannot_use() -> SnowflakeError:
    return SnowflakeError(
        "002043",
        "02000",
        "SQL compilation error:\nObject does not exist, or operation cannot be performed.",
    )


def _no_current(what: str, action: str) -> SnowflakeError:
    code = "090105" if what == "database" else "090106"
    return SnowflakeError(
        code,
        "22000",
        f"Cannot perform {action}. This session does not have a current {what}. "
        f"Call 'USE {what.upper()}', or use a qualified name.",
    )


def _result_format(session: Session) -> str:
    """The result format the driver asked for at login, if we speak it."""
    for name in ("QUERY_RESULT_FORMAT", "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"):
        value = session.parameters.get(name)
        if isinstance(value, str) and value.lower() == ARROW:
            return ARROW
    return JSON


def _chunk_rows(session: Session) -> int:
    """Rows per result chunk; Snowflake's default is 160."""
    try:
        size = int(session.parameters.get("CLIENT_RESULT_CHUNK_SIZE", 160))
    except (TypeError, ValueError):
        return 160
    return max(1, min(65_536, size))
