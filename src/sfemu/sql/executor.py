"""Runs one Snowflake statement for a session.

Statements are dispatched on their sqlglot type. Anything not listed here is
rejected with Snowflake's "Unsupported feature" error rather than handed to
DuckDB, so a statement never passes locally by accident.
"""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import duckdb
from sqlglot import exp

from sfemu import errors
from sfemu.engine import DEFAULT_SCHEMA, Engine
from sfemu.errors import SnowflakeError
from sfemu.session import Session
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

_SUCCESS = "Statement executed successfully."
_RELATION_KINDS = {"TABLE": "Table", "VIEW": "View"}


@dataclass
class QueryResult:
    columns: list[Column]
    rows: list[list[str | None]]
    statement_type_id: int
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _status(message: str, statement_type_id: int) -> QueryResult:
    return QueryResult([text_column("status", nullable=False)], [[message]], statement_type_id)


class Executor:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def execute(
        self, session: Session, sql: str, bindings: dict[str, Any] | None = None
    ) -> QueryResult:
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
        params = _bind_values(bindings)
        with session.lock:
            try:
                return self._dispatch(session, statements[0], params)
            except duckdb.Error as exc:
                raise errors.from_duckdb(exc) from exc

    def _dispatch(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        if isinstance(statement, exp.Query):
            return self._query(session, statement, params)
        if isinstance(statement, exp.Insert):
            return self._dml(session, statement, params, INSERT, ["number of rows inserted"])
        if isinstance(statement, exp.Update):
            names = ["number of rows updated", "number of multi-joined rows updated"]
            return self._dml(session, statement, params, UPDATE, names)
        if isinstance(statement, exp.Delete):
            return self._dml(session, statement, params, DELETE, ["number of rows deleted"])
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

    def _run(self, session: Session, statement: exp.Expression, params: list) -> None:
        self._require_namespace(session, statement)
        _resolve_context_functions(session, statement)
        session.cursor.execute(translate.to_duckdb(statement), params)

    def _query(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        translate.name_projections(statement)
        self._run(session, statement, params)
        cursor = session.cursor
        columns = [column_from_duckdb(d[0], d[1]) for d in cursor.description]
        rows = [
            [encode_value(value, column) for value, column in zip(row, columns, strict=True)]
            for row in cursor.fetchall()
        ]
        return QueryResult(columns, rows, SELECT)

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
                raise _already_exists(name)
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
            self._run(session, statement, params)
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
        kind = statement.args.get("kind")
        kind = kind.name.upper() if kind is not None else "SCHEMA"
        target = statement.this
        name = target.name
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
                    session.parameters.pop(item.this.name.upper(), None)
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


def _bind_values(bindings: dict[str, Any] | None) -> list:
    """Turn the driver's positional (qmark) bindings into DuckDB parameters."""
    if not bindings:
        return []
    values = []
    for position in sorted(bindings, key=int):
        binding = bindings[position]
        kind, value = str(binding.get("type", "ANY")).upper(), binding.get("value")
        if value is None:
            values.append(None)
        elif isinstance(value, list):
            raise errors.unsupported("array bind")
        elif kind == "FIXED":
            values.append(int(value) if value.lstrip("-").isdigit() else Decimal(value))
        elif kind == "REAL":
            values.append(float(value))
        elif kind == "BOOLEAN":
            values.append(value.lower() == "true")
        elif kind in ("TEXT", "ANY"):
            values.append(value)
        else:
            raise errors.unsupported(f"bind type {kind}")
    return values


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
