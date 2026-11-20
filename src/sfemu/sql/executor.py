"""Runs one Snowflake statement for a session.

Statements are dispatched on their sqlglot type. Anything not listed here is
rejected with Snowflake's "Unsupported feature" error rather than handed to
DuckDB, so a statement never passes locally by accident.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import threading
import uuid
import warnings
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from decimal import Decimal
from functools import partial
from typing import Any

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

from sfemu import errors
from sfemu.arrow import ipc_stream, snowflake_table
from sfemu.engine import (
    DEFAULT_SCHEMA,
    FOREIGN_KEY,
    PRIMARY_KEY,
    UNIQUE,
    Constraint,
    Engine,
    TableKey,
    quote,
)
from sfemu.errors import SnowflakeError
from sfemu.session import DEFAULT_PARAMETERS, Session
from sfemu.sql import metadata, semi, translate
from sfemu.types import Column, column_from_duckdb, encode_value, fixed_column, text_column

# Statement type ids as Snowflake reports them. Drivers classify by family
# (the top hex digit), so the family is what has to be right.
SELECT = 0x1000
INSERT = 0x3100
UPDATE = 0x3200
DELETE = 0x3300
MERGE = 0x3500
MULTI_STATEMENT = 0xA000
ALTER_SESSION = 0x4100
USE = 0x4300
TRANSACTION = 0x5000
DDL = 0x6000

# Result formats a driver can ask for; SELECT answers in the requested one,
# everything else stays JSON (drivers read DML row counts from the rowset).
ARROW = "arrow"
JSON = "json"

_SUCCESS = "Statement executed successfully."
# Statements that end the open transaction, as DDL does in Snowflake. ALTER
# SESSION is an Alter too, and harmless here: it is not transactional anyway.
_DDL_STATEMENTS = (exp.Create, exp.Drop, exp.TruncateTable, exp.Alter, exp.Comment)
# DML statements with their statement type id and row-count columns.
_DML: list[tuple[type[exp.Expression], int, list[str]]] = [
    (exp.Insert, INSERT, ["number of rows inserted"]),
    (exp.Update, UPDATE, ["number of rows updated", "number of multi-joined rows updated"]),
    (exp.Delete, DELETE, ["number of rows deleted"]),
]
_RELATION_KINDS = {"TABLE": "Table", "VIEW": "View"}
# Results are dropped after this many statements; drivers fetch them right
# after the query, so this only bounds memory.
_KEPT_RESULTS = 256


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
    # The per-statement results of a multi-statement request, which itself
    # answers with a one-line status; drivers fetch these by query id.
    children: list[QueryResult] = field(default_factory=list)
    # Whether a driver may send this statement once with a column of values per
    # bind; drivers ask with a describe-only request before executemany().
    array_bind_supported: bool = False
    # What RESULT_SCAN reads back: the values as DuckDB returned them, as an
    # Arrow table or as rows, and the DuckDB type of every column. A result
    # with neither is read back from its encoded rows.
    source: pa.Table | None = None
    values: list[Any] | None = None
    types: list[str] | None = None

    def __post_init__(self) -> None:
        if self.total is None:
            self.total = len(self.rows)


class ResultStore:
    """Results of recent statements, by query id.

    Drivers come back for them: for the chunks of a large result, for each
    statement of a multi-statement request, or when asked for the result of
    an earlier query. RESULT_SCAN reads them too. The oldest are dropped;
    Snowflake keeps them a day.
    """

    def __init__(self, capacity: int = _KEPT_RESULTS) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._results: OrderedDict[str, QueryResult] = OrderedDict()

    def add(self, result: QueryResult) -> None:
        with self._lock:
            for kept in (result, *result.children):
                self._results[kept.query_id] = kept
            while len(self._results) > self._capacity:
                self._results.popitem(last=False)

    def get(self, query_id: str) -> QueryResult | None:
        with self._lock:
            return self._results.get(query_id)

    def chunk(self, query_id: str, index: int) -> ResultChunk | None:
        result = self.get(query_id)
        if result is None or not 0 <= index < len(result.chunks):
            return None
        return result.chunks[index]


def _dml_shape(statement: exp.Expression) -> tuple[int, list[str]] | None:
    for kind, type_id, names in _DML:
        if isinstance(statement, kind):
            return type_id, names
    return None


def _rows_result(columns: list[Column], rows: list[list[Any]]) -> QueryResult:
    """A result set built in Python; SHOW and DESCRIBE always answer in JSON."""
    encoded = [
        [encode_value(value, column) for value, column in zip(row, columns, strict=True)]
        for row in rows
    ]
    return QueryResult(columns, encoded, SELECT, values=rows)


@contextlib.contextmanager
def _atomic(session: Session) -> Iterator[None]:
    """All or nothing: a transaction of our own, unless the session has one open."""
    if session.in_transaction:
        yield
        return
    session.cursor.execute("BEGIN")
    try:
        yield
    except BaseException:
        session.cursor.execute("ROLLBACK")
        raise
    session.cursor.execute("COMMIT")


def _is_grant(statement: exp.Expression) -> bool:
    if isinstance(statement, exp.Grant | exp.Revoke):
        return True
    return isinstance(statement, exp.Command) and str(statement.this).upper() in ("GRANT", "REVOKE")


def _pop_overwrite(session: Session, statement: exp.Expression) -> str | None:
    """Turn INSERT OVERWRITE into a plain INSERT; return the DELETE to run before it.

    The source query is read before the target is emptied in Snowflake. DuckDB
    reads it after the DELETE, in the same transaction, so an INSERT OVERWRITE
    that selects from its own target is refused rather than answered wrongly.
    """
    if not isinstance(statement, exp.Insert) or not statement.args.get("overwrite"):
        return None
    statement.set("overwrite", False)
    target = statement.this.this if isinstance(statement.this, exp.Schema) else statement.this
    key = _qualify(session, target)
    source = statement.expression
    if source is not None and any(
        _qualify(session, table) == key for table in translate.relations(source)
    ):
        raise errors.unsupported("INSERT OVERWRITE from the target table")
    return "DELETE FROM {}.{}.{}".format(*(quote(part) for part in key))


def _is_drop_primary_key(action: exp.Expression) -> bool:
    # sqlglot keeps this ALTER TABLE action as raw text.
    if not isinstance(action, exp.Command):
        return False
    words = f"{action.this} {action.expression or ''}".upper().split()
    return words == ["DROP", "PRIMARY", "KEY"]


def _constraint_name() -> str:
    return f"SYS_CONSTRAINT_{uuid.uuid4()}"


_KEY_CONSTRAINTS = (exp.PrimaryKeyColumnConstraint, exp.UniqueColumnConstraint)
_TABLE_CONSTRAINTS = (exp.Constraint, exp.PrimaryKey, exp.ForeignKey, *_KEY_CONSTRAINTS)
Qualify = Callable[[exp.Table], TableKey]


def _reference(reference: exp.Expression | None, qualify: Qualify) -> dict[str, Any]:
    """The table, and the columns if named, that a REFERENCES clause points at."""
    target = reference.this if reference is not None else None
    columns: list[str] = []
    if isinstance(target, exp.Schema):
        columns = [column.name for column in target.expressions]
        target = target.this
    if not isinstance(target, exp.Table):
        return {}
    return {"references": qualify(target), "referenced_columns": columns}


def _table_constraint(item: exp.Expression, qualify: Qualify) -> Constraint | None:
    """A table-level PRIMARY KEY, UNIQUE or FOREIGN KEY, named by CONSTRAINT or not."""
    named = isinstance(item, exp.Constraint)
    name = (item.name if named else "") or _constraint_name()
    for body in item.expressions if named else [item]:
        if isinstance(body, exp.PrimaryKey):
            return Constraint(PRIMARY_KEY, name, [column.name for column in body.expressions])
        if isinstance(body, exp.UniqueColumnConstraint) and isinstance(body.this, exp.Schema):
            return Constraint(UNIQUE, name, [column.name for column in body.this.expressions])
        if isinstance(body, exp.ForeignKey):
            columns = [column.name for column in body.expressions]
            target = _reference(body.args.get("reference"), qualify)
            return Constraint(FOREIGN_KEY, name, columns, **target)
    return None


def _pop_column_constraints(column: exp.ColumnDef, qualify: Qualify) -> list[Constraint]:
    """Strip PRIMARY KEY, UNIQUE and REFERENCES from a column and return them.

    Snowflake records these constraints and does not enforce them, DuckDB
    would. A primary key column is NOT NULL, which both do enforce.
    """
    found = []
    foreign_key_name = ""
    for constraint in list(column.args.get("constraints") or []):
        kind = constraint.kind
        name = constraint.name or _constraint_name()
        if isinstance(kind, exp.PrimaryKeyColumnConstraint):
            found.append(Constraint(PRIMARY_KEY, name, [column.name]))
            constraint.set("kind", exp.NotNullColumnConstraint())
            constraint.set("this", None)
            continue
        if isinstance(kind, exp.UniqueColumnConstraint):
            found.append(Constraint(UNIQUE, name, [column.name]))
        elif isinstance(kind, exp.ForeignKey):
            # CONSTRAINT name FOREIGN KEY REFERENCES ...: the name comes first.
            foreign_key_name = constraint.name
        elif isinstance(kind, exp.Reference):
            name = foreign_key_name or name
            found.append(Constraint(FOREIGN_KEY, name, [column.name], **_reference(kind, qualify)))
        else:
            continue
        constraint.pop()
    return found


def _pop_constraints(statement: exp.Create, qualify: Qualify) -> list[Constraint]:
    """Strip the key constraints of a CREATE TABLE and return them."""
    schema = statement.this
    if not isinstance(schema, exp.Schema):
        return []  # CREATE TABLE ... AS SELECT without a column list
    found: list[Constraint] = []
    columns: dict[str, exp.ColumnDef] = {}
    for item in list(schema.expressions):
        if isinstance(item, exp.ColumnDef):
            columns[item.name] = item
            found += _pop_column_constraints(item, qualify)
        elif isinstance(item, _TABLE_CONSTRAINTS):
            constraint = _table_constraint(item, qualify)
            if constraint is not None:
                found.append(constraint)
            item.pop()
    for constraint in found:
        for name in constraint.columns if constraint.kind == PRIMARY_KEY else []:
            column = columns.get(name)
            if column is not None and not column.find(exp.NotNullColumnConstraint):
                not_null = exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
                column.append("constraints", not_null)
    return found


def _pop_temporary(statement: exp.Create) -> bool:
    """Remove TEMPORARY from a CREATE TABLE and say whether it was there."""
    properties = statement.args.get("properties")
    found = False
    for prop in list(properties.expressions) if properties else []:
        if isinstance(prop, exp.TemporaryProperty):
            prop.pop()
            found = True
    return found


def _comment_sql(kind: str, target: str, text: str | None) -> str:
    literal = "NULL" if not text else "'" + text.replace("'", "''") + "'"
    return f"COMMENT ON {kind} {target} IS {literal}"


def _set_comment(kind: str, action: exp.AlterSet) -> str:
    """The comment of ALTER ... SET COMMENT = '...'; nothing else can be SET yet."""
    comment = action.find(exp.SchemaCommentProperty)
    if comment is None or len(list(action.find_all(exp.Property))) != 1:
        raise errors.unsupported(f"ALTER {kind} SET")
    return comment.this.name


def _status(message: str, statement_type_id: int) -> QueryResult:
    return QueryResult([text_column("status", nullable=False)], [[message]], statement_type_id)


class Executor:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self.results = ResultStore()

    def execute(
        self,
        session: Session,
        sql: str,
        bindings: dict[str, Any] | None = None,
        *,
        describe_only: bool = False,
        statement_count: int | None = None,
    ) -> QueryResult:
        """Run the SQL text of one request.

        ``statement_count`` is the request's MULTI_STATEMENT_COUNT: how many
        statements the text must hold, or 0 for any number. Without it the
        session's parameter decides, and that defaults to exactly one. With
        ``describe_only`` nothing runs and only the result columns come back.
        """
        statements = translate.parse(sql)
        if not statements:
            raise SnowflakeError("000900", "42601", "SQL compilation error:\nEmpty SQL statement.")
        if statement_count is None:
            statement_count = _statement_count(session)
        if statement_count not in (0, len(statements)):
            raise SnowflakeError(
                "000008",
                "0A000",
                f"Actual statement count {len(statements)} "
                f"did not match the desired statement count {statement_count}.",
            )
        with session.lock:
            self._drop_scratch(session)
            try:
                if len(statements) == 1:
                    return self._execute_one(session, statements[0], bindings, describe_only)
                return self._execute_many(session, statements, bindings, describe_only)
            except duckdb.Error as exc:
                raise errors.from_duckdb(exc) from exc

    def _execute_one(
        self,
        session: Session,
        statement: exp.Expression,
        bindings: dict[str, Any] | None,
        describe_only: bool,
    ) -> QueryResult:
        binds = translate.bind_count(statement)
        rows = _bind_rows(bindings)
        if describe_only:
            result = self._describe_only(session, statement, binds)
        elif rows is None:
            result = self._dispatch(session, statement, _bind_values(bindings))
        else:
            result = self._dispatch_rows(session, statement, rows)
        result.bind_count = binds
        result.array_bind_supported = isinstance(statement, exp.Insert)
        if not describe_only:
            self._remember(session, result)
        return result

    def _remember(self, session: Session, result: QueryResult) -> None:
        """Keep a finished statement's result and note it in the session's history."""
        self.results.add(result)
        session.query_ids.append(result.query_id)

    def _execute_many(
        self,
        session: Session,
        statements: list[exp.Expression],
        bindings: dict[str, Any] | None,
        describe_only: bool,
    ) -> QueryResult:
        """A multi-statement request: run the statements in order, keep every result.

        As on Snowflake, a failing statement fails the request, and whatever
        ran before it stays done.
        """
        if describe_only:
            raise errors.unsupported("describe of a multi-statement request")
        if _bind_rows(bindings) is not None:
            raise errors.unsupported("array bind in a multi-statement request")
        params = _bind_values(bindings)  # consumed left to right, statement by statement
        children = []
        for statement in statements:
            binds = translate.bind_count(statement)
            child = self._dispatch(session, statement, params[:binds])
            child.bind_count = binds
            params = params[binds:]
            children.append(child)
            self._remember(session, child)  # the next statement may RESULT_SCAN it
        parent = _status("Multiple statements executed successfully.", MULTI_STATEMENT)
        parent.columns = [text_column("multiple statement execution", nullable=False)]
        parent.children = children
        self.results.add(parent)
        return parent

    def _describe_only(
        self, session: Session, statement: exp.Expression, binds: int
    ) -> QueryResult:
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
        truncate = _pop_overwrite(session, statement)
        sql = translate.to_duckdb(statement, self._lookup(session))
        cursor = session.cursor
        affected = 0
        with _atomic(session):
            if truncate:
                cursor.execute(truncate)
            for row in rows:
                affected += cursor.execute(sql, row).fetchone()[0]
        counts = [str(affected)] + ["0"] * (len(names) - 1)
        columns = [fixed_column(name, nullable=False) for name in names]
        return QueryResult(columns, [counts], type_id)

    def _dispatch(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        if isinstance(statement, exp.Query):
            return self._query(session, statement, params)
        dml = _dml_shape(statement)
        if dml is not None:
            return self._dml(session, statement, params, *dml)
        if isinstance(statement, _DDL_STATEMENTS):
            self._commit_before_ddl(session)
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
        if isinstance(statement, exp.Alter) and statement.args.get("kind") in _RELATION_KINDS:
            return self._alter(session, statement)
        if isinstance(statement, exp.Merge):
            return self._merge(session, statement, params)
        if isinstance(statement, exp.Show):
            return self._show(session, statement)
        if isinstance(statement, exp.Describe):
            return self._describe_relation(session, statement)
        if isinstance(statement, exp.Comment):
            return self._comment(session, statement)
        if _is_grant(statement):
            # There is no access control yet: every role may do everything.
            return _status(_SUCCESS, DDL)
        if isinstance(statement, exp.Command):
            # Statements sqlglot keeps as raw text.
            head, rest = str(statement.this).upper(), str(statement.expression or "")
            if head == "SHOW" and rest.split()[:1] and rest.split()[0].upper() == "GRANTS":
                return _rows_result(metadata.GRANTS, [])
            if head == "ALTER":
                self._commit_before_ddl(session)
                return self._alter_column_comments(session, f"ALTER {rest}")
            if head == "SHOW" and rest.split()[:1] and rest.split()[0].upper() == "PARAMETERS":
                return _rows_result(*self._show_parameters(session, f"SHOW {rest}"))
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
    ) -> list[str | None] | None:
        """Check and run a statement; return the kinds of its result columns, if known."""
        self._check(session, statement, creating)
        sql, kinds = translate.compile(statement, self._lookup(session))
        session.cursor.execute(sql, params)
        return kinds

    def _lookup(self, session: Session) -> semi.Lookup:
        """How the SQL rewrites learn the kinds of a table's columns: from the catalog."""

        def columns(table: exp.Table) -> dict[str, str | None] | None:
            database, schema, name = _qualify(session, table)
            if database is None or schema is None:
                return None
            found = session.cursor.execute(
                "SELECT column_name, data_type FROM duckdb_columns() "
                "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
                [database, schema, name],
            ).fetchall()
            return {column: _column_kind(data_type) for column, data_type in found} or None

        return columns

    def _check(
        self, session: Session, statement: exp.Expression, creating: exp.Table | None = None
    ) -> None:
        """Everything that must hold before a statement may reach DuckDB."""
        self._require_namespace(session, statement)
        _resolve_last_query_id(session, statement)
        translate.check_supported(statement)
        _resolve_context_functions(session, statement)
        for table in translate.relations(statement):
            if table is creating:
                continue
            if self._kind(session, table) is None:
                raise errors.object_not_found(
                    f"Object '{table.name}' does not exist or not authorized."
                )
        # Both replace what they find with DuckDB names the checks would reject.
        metadata.expand_information_schema(statement, session.database, self._engine.started)
        self._expand_result_scans(session, statement)

    def _expand_result_scans(self, session: Session, statement: exp.Expression) -> None:
        """Point every RESULT_SCAN('<query id>') at a temporary table holding that result."""
        for scan in translate.result_scans(statement):
            arguments = scan.this.expressions
            if len(arguments) == 1 and isinstance(arguments[0], exp.Null):
                raise errors.query_not_found("NULL")  # LAST_QUERY_ID() with no such query
            if len(arguments) != 1 or not arguments[0].is_string:
                raise errors.unsupported("RESULT_SCAN of anything but a query id")
            result = self.results.get(arguments[0].name)
            if result is None:
                raise errors.query_not_found(arguments[0].name)
            table = exp.table_(self._scratch_table(session, result), "main", "temp", quoted=True)
            alias = scan.args.get("alias")
            if alias is not None:
                table.set("alias", alias.copy())
            scan.replace(table)

    def _scratch_table(self, session: Session, result: QueryResult) -> str:
        """Copy a kept result into a DuckDB temporary table of this session; return its name."""
        cursor = session.cursor
        name = f"sfemu_result_{uuid.uuid4().hex}"
        target = f"temp.main.{quote(name)}"
        session.scratch.append(name)
        names = [quote(column.name) for column in result.columns]
        types = result.types or [_duckdb_type(column) for column in result.columns]
        # DuckDB types an untyped NULL column as "NULL", which no table can have.
        types = ["VARCHAR" if kind == '"NULL"' else kind for kind in types]
        rows = result.values if result.values is not None else result.rows
        table = result.source if result.source is not None else _arrow_table(rows, len(names))
        if table is not None:
            # Arrow carries the values; the casts restore the exact DuckDB types.
            staged = f"{name}_in"
            # DuckDB will not register anything while the cursor's previous
            # statement (a BEGIN, a DDL) still has a result nobody read.
            with contextlib.suppress(duckdb.Error):
                cursor.fetchall()
            casts = [
                f"CAST(c{i} AS {kind}) AS {column}"
                for i, (column, kind) in enumerate(zip(names, types, strict=True))
            ]
            cursor.register(staged, table.rename_columns([f"c{i}" for i in range(len(names))]))
            try:
                cursor.execute(
                    f"CREATE TEMP TABLE {target} AS "
                    f"SELECT {', '.join(casts)} FROM temp.main.{quote(staged)}"
                ).fetchall()  # DuckDB refuses further work while a result is unread
                return name
            except duckdb.Error:
                if result.source is not None:
                    raise
                # Values Arrow guessed a type for that DuckDB cannot cast: go row by row.
            finally:
                cursor.unregister(staged)
        declared = ", ".join(f"{column} {kind}" for column, kind in zip(names, types, strict=True))
        cursor.execute(f"CREATE TEMP TABLE {target} ({declared})")
        if rows:
            marks = ", ".join("?" * len(names))
            cursor.executemany(f"INSERT INTO {target} VALUES ({marks})", rows)
        return name

    def _drop_scratch(self, session: Session) -> None:
        """Drop the RESULT_SCAN tables of earlier statements.

        Not inside a transaction: a rollback would bring a dropped table back
        with nobody left to drop it again.
        """
        if session.in_transaction:
            return
        kept = []
        for name in session.scratch:
            try:
                session.cursor.execute(f"DROP TABLE IF EXISTS temp.main.{quote(name)}")
            except duckdb.Error:
                kept.append(name)
        session.scratch = kept

    def _kind(self, session: Session, table: exp.Table) -> str | None:
        """'TABLE' or 'VIEW' if the session can see this relation, else None."""
        return self._engine.relation_kind(session.cursor, *_qualify(session, table))

    def _query(self, session: Session, statement: exp.Expression, params: list) -> QueryResult:
        translate.name_projections(statement)
        kinds = self._run(session, statement, params)
        cursor = session.cursor
        columns = [column_from_duckdb(d[0], d[1]) for d in cursor.description]
        if kinds is not None and len(kinds) == len(columns):
            # DuckDB holds every semi-structured value as JSON; whether a
            # column is an ARRAY or an OBJECT is known from the statement.
            columns = [
                replace(column, type=kind.lower())
                if column.type == "variant" and kind in (semi.ARRAY, semi.OBJECT)
                else column
                for column, kind in zip(columns, kinds, strict=True)
            ]
        types = [str(d[1]) for d in cursor.description]
        chunk_rows = _chunk_rows(session)
        if _result_format(session) == ARROW:
            # Arrow output on a DB-API result copies once more than the
            # relational API would; that copy is the price of parameters.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                raw = cursor.to_arrow_table()
                table = snowflake_table(raw, columns)
            if table.num_rows == 0:
                # Snowflake sends an empty rowsetBase64 for an empty result and
                # lets rowtype describe the columns. A zero-row record batch
                # instead segfaults the 3.x / 4.x Python connectors in fetchall().
                return QueryResult(columns, [], SELECT, total=0, arrow=b"", source=raw, types=types)
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
                source=raw,
                types=types,
            )
        fetched = cursor.fetchall()
        rows = [
            [encode_value(value, column) for value, column in zip(row, columns, strict=True)]
            for row in fetched
        ]
        first, rest = rows[:chunk_rows], rows[chunk_rows:]
        chunks = [
            ResultChunk(row_count=len(rest[i : i + chunk_rows]), rows=rest[i : i + chunk_rows])
            for i in range(0, len(rest), chunk_rows)
        ]
        return QueryResult(
            columns, first, SELECT, total=len(rows), chunks=chunks, values=fetched, types=types
        )

    def _dml(
        self,
        session: Session,
        statement: exp.Expression,
        params: list,
        statement_type_id: int,
        names: list[str],
    ) -> QueryResult:
        self._check(session, statement)
        truncate = _pop_overwrite(session, statement)
        sql = translate.to_duckdb(statement, self._lookup(session))
        with _atomic(session):
            if truncate:
                session.cursor.execute(truncate)
            affected = session.cursor.execute(sql, params).fetchone()[0]
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
            found = self._kind(session, target)
            if found and if_not_exists:
                return _status(f"{name} already exists, statement succeeded.", DDL)
            if found and not replace:
                raise _already_exists(name)
            if found and found != kind:
                raise SnowflakeError(
                    "002002",
                    "42710",
                    f"SQL compilation error:\nObject found is of type '{found}', "
                    f"not specified type '{kind}'.",
                )
            temporary = _pop_temporary(statement)
            qualify = partial(_qualify, session)
            constraints = _pop_constraints(statement, qualify) if kind == "TABLE" else []
            if kind == "VIEW" and translate.result_scans(statement):
                raise errors.unsupported("RESULT_SCAN in a view")
            if kind == "VIEW":
                # Pin what the view reads to where it resolves now, as Snowflake
                # does; the definition then survives a move to another schema.
                for table in translate.relations(statement):
                    if table is not target:
                        resolved = _qualify(session, table)
                        table.set("catalog", exp.to_identifier(resolved[0], quoted=True))
                        table.set("db", exp.to_identifier(resolved[1], quoted=True))
            self._run(session, statement, params, creating=target)
            key = (database, schema, name)
            self._engine.constraints.pop(key, None)
            for constraint in constraints:
                self._engine.add_constraint(key, constraint)
            if temporary:
                session.temporary_tables.add(key)
            else:
                session.temporary_tables.discard(key)
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
            exists = self._kind(session, target) == kind
            if exists:
                # Snowflake's CASCADE is about foreign keys; DuckDB's would also
                # drop the views that select from the table.
                statement.set("cascade", False)
                session.cursor.execute(translate.to_duckdb(statement))
                session.temporary_tables.discard(_qualify(session, target))
                self._engine.constraints.pop(_qualify(session, target), None)
        else:
            raise errors.unsupported(f"DROP {kind}")

        if exists:
            return _status(f"{name} successfully dropped.", DDL)
        if if_exists:
            message = f"Drop statement executed successfully ({name} already dropped)."
            return _status(message, DDL)
        label = _RELATION_KINDS.get(kind, kind.capitalize())
        raise errors.object_not_found(f"{label} '{name}' does not exist or not authorized.")

    def _alter(self, session: Session, statement: exp.Alter) -> QueryResult:
        kind = statement.args["kind"]
        target = statement.this
        actions = statement.args.get("actions") or []
        self._require_namespace(session, statement)
        qualify = partial(_qualify, session)
        engine = self._engine
        key = _qualify(session, target)
        if self._kind(session, target) != kind:
            if statement.args.get("exists"):
                return _status(_SUCCESS, DDL)
            label = _RELATION_KINDS[kind]
            raise errors.object_not_found(f"{label} '{key[2]}' does not exist or not authorized.")
        relation = ".".join(quote(part) for part in key)
        cursor = session.cursor
        steps: list[Callable[[], Any]] = []
        for action in actions:
            if isinstance(action, exp.AlterRename):
                new_key = _qualify(session, action.this)
                if self._kind(session, action.this) is not None:
                    raise _already_exists(new_key[2])
                steps.append(partial(self._relocate, session, kind, key, new_key))
            elif isinstance(action, exp.SwapTable):
                other = _qualify(session, action.this)
                if kind != "TABLE" or self._kind(session, action.this) != "TABLE":
                    raise errors.object_not_found(
                        f"Table '{other[2]}' does not exist or not authorized."
                    )
                # Three moves through a name nobody else can be using.
                parked = (key[0], key[1], f"sfemu_swap_{uuid.uuid4().hex}")
                steps.append(partial(self._relocate, session, kind, key, parked))
                steps.append(partial(self._relocate, session, kind, other, key))
                steps.append(partial(self._relocate, session, kind, parked, other))
            elif isinstance(action, exp.AlterSet):
                sql = _comment_sql(kind, relation, _set_comment(kind, action))
                steps.append(partial(cursor.execute, sql))
            elif isinstance(action, exp.AlterColumn) and action.args.get("comment") is not None:
                column = f"{relation}.{quote(action.name)}"
                sql = _comment_sql("COLUMN", column, action.args["comment"].name)
                steps.append(partial(cursor.execute, sql))
            elif isinstance(action, exp.AddConstraint):
                # Recorded and never enforced; the table a foreign key names
                # is not looked up, so it need not exist yet.
                for item in action.expressions:
                    constraint = _table_constraint(item, qualify)
                    if constraint is not None:
                        steps.append(partial(engine.add_constraint, key, constraint))
            elif isinstance(action, exp.Drop) and action.args.get("kind") == "CONSTRAINT":
                # sqlglot keeps the constraint's name where a DROP keeps its tables.
                for dropped in action.args.get("tables") or []:
                    steps.append(partial(engine.drop_constraints, key, name=dropped.name))
            elif _is_drop_primary_key(action):
                steps.append(partial(engine.drop_constraints, key))
            elif isinstance(action, exp.ColumnDef | exp.Drop | exp.RenameColumn | exp.AlterColumn):
                # DuckDB runs one action per ALTER statement.
                if isinstance(action, exp.ColumnDef):
                    for constraint in _pop_column_constraints(action, qualify):
                        steps.append(partial(engine.add_constraint, key, constraint))
                single = exp.Alter(this=target.copy(), kind=kind, actions=[action.copy()])
                translate.check_supported(single)
                steps.append(partial(cursor.execute, translate.to_duckdb(single)))
                if isinstance(action, exp.RenameColumn):
                    renamed = (action.this.name, action.args["to"].name)
                    steps.append(partial(engine.rename_column, key, *renamed))
            else:
                raise errors.unsupported(f"ALTER {kind} ... {action.key.upper()}")
        with _atomic(session):
            for step in steps:
                step()
        return _status(_SUCCESS, DDL)

    def _relocate(
        self,
        session: Session,
        kind: str,
        old: tuple[str, str, str],
        new: tuple[str, str, str],
    ) -> None:
        """Rename a table or view, moving it to another schema if the new name says so."""
        cursor = session.cursor
        old_name = ".".join(quote(part) for part in old)
        new_name = ".".join(quote(part) for part in new)
        if old[:2] == new[:2]:
            cursor.execute(f"ALTER {kind} {old_name} RENAME TO {quote(new[2])}")
        elif old[0] != new[0]:
            # DuckDB cannot write to two databases in one transaction.
            raise errors.unsupported(f"ALTER {kind} RENAME TO another database")
        else:
            # DuckDB cannot move a relation between schemas: recreate it from
            # its own definition, copy the rows and comments, drop the original.
            if not self._engine.has_schema(new[0], new[1]):
                raise errors.object_not_found(
                    f"Schema '{new[1]}' does not exist or not authorized."
                )
            source, column = ("duckdb_tables()", "table_name")
            if kind == "VIEW":
                source, column = ("duckdb_views()", "view_name")
            where = f"database_name = ? AND schema_name = ? AND {column} = ?"
            definition, comment = cursor.execute(
                f"SELECT sql, comment FROM {source} WHERE {where}", list(old)
            ).fetchone()
            create = sqlglot.parse_one(definition, read=translate.DUCKDB)
            target = create.this.this if isinstance(create.this, exp.Schema) else create.this
            target.replace(exp.table_(new[2], db=new[1], catalog=new[0], quoted=True))
            cursor.execute(create.sql(dialect=translate.DUCKDB, identify=True))
            if kind == "TABLE":
                cursor.execute(f"INSERT INTO {new_name} SELECT * FROM {old_name}")
            if comment:
                cursor.execute(_comment_sql(kind, new_name, comment))
            comments = cursor.execute(
                "SELECT column_name, comment FROM duckdb_columns() "
                "WHERE database_name = ? AND schema_name = ? AND table_name = ? "
                "AND comment IS NOT NULL",
                list(old),
            ).fetchall()
            for column_name, text in comments:
                cursor.execute(_comment_sql("COLUMN", f"{new_name}.{quote(column_name)}", text))
            cursor.execute(f"DROP {kind} {old_name}")
        if old in session.temporary_tables:
            session.temporary_tables.discard(old)
            session.temporary_tables.add(new)
        self._engine.move_constraints(old, new)

    def _alter_column_comments(self, session: Session, sql: str) -> QueryResult:
        """ALTER TABLE t ALTER [COLUMN] a COMMENT '..', b COMMENT '..' — dbt's persist_docs."""
        tokens = translate.tokens(sql)
        kinds = [kind for kind, _ in tokens]
        unsupported = errors.unsupported("ALTER")
        drop_key = [text.upper() for _, text in tokens[-3:]] == ["DROP", "PRIMARY", "KEY"]
        if drop_key:
            tokens, kinds = [*tokens[:-3], ("ALTER", "")], [*kinds[:-3], "ALTER"]
        if kinds[:2] != ["ALTER", "TABLE"] or "ALTER" not in kinds[2:]:
            raise unsupported
        split = kinds.index("ALTER", 2)
        name_tokens = tokens[2:split]
        if_exists = [text.upper() for _, text in name_tokens[:2]] == ["IF", "EXISTS"]
        if if_exists:
            name_tokens = name_tokens[2:]
        name = "".join(quote(text) if kind == "IDENTIFIER" else text for kind, text in name_tokens)
        table = translate.parse(f"SELECT * FROM {name}")[0].find(exp.Table)
        if table is None or not isinstance(table.this, exp.Identifier):
            raise unsupported
        comments: list[tuple[str, str]] = []
        items = tokens[split + 1 :]
        while items:
            if items[0][0] == "COLUMN":
                items = items[1:]
            if len(items) < 3 or items[1][0] != "COMMENT" or "STRING" not in items[2][0]:
                raise unsupported
            (kind, column), _, (_, text) = items[:3]
            comments.append((column if kind == "IDENTIFIER" else column.upper(), text))
            items = items[3:]
            if items and items[0][0] != "COMMA":
                raise unsupported
            items = items[1:]
        if table.catalog is None and session.database is None:
            raise _no_current("database", "ALTER TABLE")
        database, schema, table_name = _qualify(session, table)
        if self._kind(session, table) is None:
            if if_exists:
                return _status(_SUCCESS, DDL)
            raise errors.object_not_found(f"Table '{table_name}' does not exist or not authorized.")
        if drop_key:
            self._engine.drop_constraints((database, schema, table_name))
            return _status(_SUCCESS, DDL)
        relation = f"{quote(database)}.{quote(schema)}.{quote(table_name)}"
        with _atomic(session):
            for column, text in comments:
                session.cursor.execute(_comment_sql("COLUMN", f"{relation}.{quote(column)}", text))
        return _status(_SUCCESS, DDL)

    def _comment(self, session: Session, statement: exp.Comment) -> QueryResult:
        kind = str(statement.args.get("kind") or "").upper()
        text = statement.expression.name if statement.expression is not None else None
        target = statement.this
        if kind in _RELATION_KINDS and isinstance(target, exp.Table):
            self._require_namespace(session, statement)
            found = self._kind(session, target)
            if found is None:
                if statement.args.get("exists"):
                    return _status(_SUCCESS, DDL)
                raise errors.object_not_found(
                    f"Object '{target.name}' does not exist or not authorized."
                )
            database, schema, name = _qualify(session, target)
            relation = f"{quote(database)}.{quote(schema)}.{quote(name)}"
            # Snowflake accepts COMMENT ON TABLE for a view; DuckDB wants the kind right.
            session.cursor.execute(_comment_sql(found, relation, text))
        elif kind == "COLUMN" and isinstance(target, exp.Column):
            parts = [part.name for part in target.parts]
            if len(parts) < 2:
                raise errors.syntax_error("COMMENT ON COLUMN needs a table name.")
            *table_parts, column = parts
            table = exp.to_table(".".join(quote(part) for part in table_parts))
            database, schema, name = _qualify(session, table)
            if self._kind(session, table) is None:
                if statement.args.get("exists"):
                    return _status(_SUCCESS, DDL)
                raise errors.object_not_found(f"Object '{name}' does not exist or not authorized.")
            relation = f"{quote(database)}.{quote(schema)}.{quote(name)}.{quote(column)}"
            session.cursor.execute(_comment_sql("COLUMN", relation, text))
        # Comments on schemas, databases and other objects are accepted and dropped:
        # DuckDB has nowhere to keep them.
        return _status(_SUCCESS, DDL)

    def _merge(self, session: Session, statement: exp.Merge, params: list) -> QueryResult:
        self._check(session, statement)
        whens = statement.args["whens"].expressions
        thens = [when.args.get("then") for when in whens]
        present = {
            "inserted": any(isinstance(then, exp.Insert) for then in thens),
            "updated": any(isinstance(then, exp.Update) for then in thens),
            "deleted": any(not isinstance(then, exp.Insert | exp.Update) for then in thens),
        }
        # DuckDB reports one total; Snowflake a count per kind of action taken.
        sql = translate.to_duckdb(statement, self._lookup(session)) + " RETURNING merge_action"
        actions = [row[0] for row in session.cursor.execute(sql, params).fetchall()]
        tally = {
            "inserted": actions.count("INSERT"),
            "updated": actions.count("UPDATE"),
            "deleted": actions.count("DELETE"),
        }
        names = [name for name, used in present.items() if used]
        columns = [fixed_column(f"number of rows {name}", nullable=False) for name in names]
        return QueryResult(columns, [[str(tally[name]) for name in names]], MERGE)

    # Catalog views ---------------------------------------------------------

    def _show(self, session: Session, statement: exp.Show) -> QueryResult:
        columns, rows = metadata.show(
            session.cursor,
            statement,
            database=session.database,
            schema=session.schema,
            started=self._engine.started,
            constraints=self._engine.constraints,
        )
        return _rows_result(columns, rows)

    def _show_parameters(self, session: Session, sql: str) -> metadata.Rows:
        """SHOW PARAMETERS [LIKE '...'] [IN SESSION]; other scopes are not kept."""
        tokens = translate.tokens(sql)[2:]
        like = None
        if tokens and tokens[0][0] == "LIKE":
            if len(tokens) < 2 or "STRING" not in tokens[1][0]:
                raise errors.syntax_error("SHOW PARAMETERS LIKE needs a pattern.")
            like, tokens = tokens[1][1], tokens[2:]
        scope = [text.upper() for _, text in tokens]
        if scope not in ([], ["IN", "SESSION"], ["FOR", "SESSION"]):
            raise errors.unsupported(f"SHOW PARAMETERS {' '.join(scope)}")
        return metadata.show_parameters(session.parameters, DEFAULT_PARAMETERS, like)

    def _describe_relation(self, session: Session, statement: exp.Describe) -> QueryResult:
        kind = str(statement.args.get("kind") or "TABLE").upper()
        target = statement.this
        if kind not in _RELATION_KINDS or not isinstance(target, exp.Table):
            raise errors.unsupported(f"DESCRIBE {kind}")
        self._require_namespace(session, statement)
        database, schema, name = _qualify(session, target)
        found = self._kind(session, target)
        # DESCRIBE TABLE also describes a view; DESCRIBE VIEW only views.
        if found is None or (kind == "VIEW" and found != "VIEW"):
            label = _RELATION_KINDS[kind]
            raise errors.object_not_found(f"{label} '{name}' does not exist or not authorized.")
        constraints = self._engine.constraints.get((database, schema, name), [])
        described = metadata.describe(session.cursor, database, schema, name, constraints)
        return _rows_result(*described)

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
        begin = isinstance(statement, exp.Transaction)
        if begin != session.in_transaction:
            session.cursor.execute("BEGIN" if begin else translate.to_duckdb(statement))
            session.in_transaction = begin
        return _status(_SUCCESS, TRANSACTION)

    def _commit_before_ddl(self, session: Session) -> None:
        """DDL in Snowflake commits the open transaction and runs in one of its own."""
        if session.in_transaction:
            session.cursor.execute("COMMIT")
            session.in_transaction = False

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


def _resolve_last_query_id(session: Session, statement: exp.Expression) -> None:
    """Replace LAST_QUERY_ID([n]) with the id of a statement this session ran.

    Counted from the end for a negative n (the default is -1, the latest) and
    from the session's first statement for a positive one; NULL if there is none.
    """
    for node in list(statement.find_all(exp.Anonymous)):
        if node.name.upper() != "LAST_QUERY_ID":
            continue
        position = -1
        if node.expressions:
            try:
                position = int(node.expressions[0].sql())
            except ValueError:
                raise errors.unsupported("LAST_QUERY_ID of anything but a number") from None
        ids = session.query_ids
        index = position - 1 if position > 0 else position
        known = position != 0 and -len(ids) <= index < len(ids)
        node.replace(exp.Literal.string(ids[index]) if known else exp.Null())


_DUCKDB_TYPES = {
    "real": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "time": "TIME",
    "timestamp_ntz": "TIMESTAMP",
    "timestamp_ltz": "TIMESTAMPTZ",
    "binary": "BLOB",
}


def _duckdb_type(column: Column) -> str:
    """The DuckDB type for a result column that was built here, not by DuckDB."""
    if column.type == "fixed":
        return f"DECIMAL({column.precision or 38}, {column.scale or 0})"
    return _DUCKDB_TYPES.get(column.type, "VARCHAR")


def _arrow_table(rows: list[Any], width: int) -> pa.Table | None:
    """Rows of Python values as an Arrow table, if Arrow can tell their types."""
    try:
        return pa.table({f"c{i}": pa.array([row[i] for row in rows]) for i in range(width)})
    except (pa.ArrowException, TypeError, ValueError, OverflowError):
        return None


def _column_kind(data_type: str) -> str:
    """The kind, as sfemu.sql.semi understands it, of a DuckDB column type."""
    if data_type == "JSON":
        return semi.VARIANT
    if data_type.startswith("DECIMAL") or data_type.endswith("INT"):
        return semi.NUMBER
    if data_type in ("DOUBLE", "FLOAT"):
        return semi.NUMBER
    if data_type == "VARCHAR":
        return semi.TEXT
    if data_type == "BOOLEAN":
        return semi.BOOLEAN
    if data_type.startswith(("DATE", "TIME")):
        return semi.TEMPORAL
    return "OTHER"


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


def _statement_count(session: Session) -> int:
    """How many statements one request may hold; Snowflake's default is 1, and 0 is any."""
    try:
        return max(0, int(session.parameters.get("MULTI_STATEMENT_COUNT", 1)))
    except (TypeError, ValueError):
        return 1


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
