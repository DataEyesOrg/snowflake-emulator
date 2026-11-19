"""Snowflake's views of the catalog: SHOW, DESCRIBE and INFORMATION_SCHEMA.

All three read DuckDB's catalog functions and present the result the way
Snowflake does. What DuckDB does not record (owners, creation times, retention)
is filled with fixed values: the owner is always SYSADMIN and every object
dates from the moment the emulator started.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp

from sfemu import errors
from sfemu.engine import INTERNAL_DATABASES
from sfemu.types import Column, fixed_column, text_column

OWNER = "SYSADMIN"
INFORMATION_SCHEMA = "INFORMATION_SCHEMA"

Rows = tuple[list[Column], list[list[Any]]]

_USER_DATABASE = "database_name NOT IN ({})".format(", ".join(f"'{d}'" for d in INTERNAL_DATABASES))
_USER_SCHEMA = f"NOT internal AND schema_name <> 'main' AND {_USER_DATABASE}"

# A DuckDB column type as the type name INFORMATION_SCHEMA.COLUMNS reports.
_DATA_TYPE = """CASE
    WHEN data_type LIKE 'DECIMAL%' OR data_type LIKE '%INT' THEN 'NUMBER'
    WHEN data_type IN ('DOUBLE', 'FLOAT') THEN 'FLOAT'
    WHEN data_type = 'BOOLEAN' THEN 'BOOLEAN'
    WHEN data_type = 'DATE' THEN 'DATE'
    WHEN data_type LIKE 'TIME%ZONE' AND data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP_LTZ'
    WHEN data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP_NTZ'
    WHEN data_type LIKE 'TIME%' THEN 'TIME'
    WHEN data_type = 'BLOB' THEN 'BINARY'
    WHEN data_type IN ('JSON', 'VARIANT') OR data_type LIKE 'UNION%' THEN 'VARIANT'
    WHEN data_type LIKE '%]' THEN 'ARRAY'
    WHEN data_type LIKE 'STRUCT%' OR data_type LIKE 'MAP%' THEN 'OBJECT'
    ELSE 'TEXT'
END"""
# Snowflake integers are NUMBER(38, 0) whatever DuckDB stored them as.
_PRECISION = "CASE WHEN data_type LIKE 'DECIMAL%' THEN numeric_precision ELSE 38 END"
_SCALE = "CASE WHEN data_type LIKE 'DECIMAL%' THEN numeric_scale ELSE 0 END"

_COLUMNS_SQL = f"""
SELECT database_name, schema_name, table_name, column_name, column_index, column_default,
       is_nullable, comment, {_DATA_TYPE} AS type_name,
       {_PRECISION} AS type_precision, {_SCALE} AS type_scale
FROM duckdb_columns()
WHERE NOT internal AND {_USER_DATABASE}
"""

_OBJECTS_SQL = f"""
SELECT database_name, schema_name, table_name AS name, 'TABLE' AS kind, comment,
       estimated_size AS row_count, NULL AS definition
FROM duckdb_tables()
WHERE {_USER_SCHEMA} AND NOT temporary
UNION ALL
SELECT database_name, schema_name, view_name, 'VIEW', comment, NULL, sql
FROM duckdb_views()
WHERE {_USER_SCHEMA} AND NOT temporary
"""

# Every database has an INFORMATION_SCHEMA, which DuckDB's catalog cannot hold.
_SCHEMAS_SQL = f"""
SELECT database_name, schema_name, comment FROM duckdb_schemas() WHERE {_USER_SCHEMA}
UNION ALL
SELECT database_name, '{INFORMATION_SCHEMA}', NULL FROM duckdb_databases() WHERE {_USER_DATABASE}
"""

_DATABASES_SQL = f"SELECT database_name, comment FROM duckdb_databases() WHERE {_USER_DATABASE}"


# SHOW ------------------------------------------------------------------------


def show(
    cursor: duckdb.DuckDBPyConnection,
    statement: exp.Show,
    *,
    database: str | None,
    schema: str | None,
    started: dt.datetime,
) -> Rows:
    """Answer a SHOW command for a session whose current namespace is given."""
    what = str(statement.name).upper()
    terse = bool(statement.args.get("terse"))
    if what == "DATABASES":
        columns, rows = _show_databases(cursor, terse, database, started)
    elif what == "SCHEMAS":
        scope = _scope(cursor, statement, database, None, default="DATABASE")
        columns, rows = _show_schemas(cursor, terse, scope, (database, schema), started)
    elif what in ("OBJECTS", "TABLES", "VIEWS"):
        scope = _scope(cursor, statement, database, schema, default="SCHEMA")
        columns, rows = _show_objects(cursor, what, terse, scope, started)
    else:
        raise errors.unsupported(f"SHOW {what}")
    return columns, _narrow(statement, columns, rows)


def _scope(
    cursor: duckdb.DuckDBPyConnection,
    statement: exp.Show,
    database: str | None,
    schema: str | None,
    *,
    default: str,
) -> tuple[str | None, str | None]:
    """The (database, schema) a SHOW is limited to; None means "all of them"."""
    kind = statement.args.get("scope_kind")
    target = statement.args.get("scope")
    if kind is None:
        # No IN clause: the current schema or database, or the whole account
        # for a session that has none.
        if default == "SCHEMA" and database and schema:
            return database, schema
        return database, None
    kind = str(kind).upper()
    if kind == "ACCOUNT":
        return None, None
    if kind == "DATABASE":
        name = target.name if target is not None else database
        if name is None or not _exists(cursor, _DATABASES_SQL, "database_name = ?", [name]):
            raise _no_such_scope()
        return name, None
    if kind == "SCHEMA":
        scope_database = (target.db if target is not None else None) or database
        scope_schema = target.name if target is not None else schema
        found = (
            scope_database is not None
            and scope_schema is not None
            and _exists(
                cursor,
                _SCHEMAS_SQL,
                "database_name = ? AND schema_name = ?",
                [scope_database, scope_schema],
            )
        )
        if not found:
            raise _no_such_scope()
        return scope_database, scope_schema
    raise errors.unsupported(f"SHOW ... IN {kind}")


def _show_databases(
    cursor: duckdb.DuckDBPyConnection, terse: bool, current: str | None, started: dt.datetime
) -> Rows:
    names = [row[0] for row in _select(cursor, _DATABASES_SQL, order="database_name")]
    comments = dict(_select(cursor, _DATABASES_SQL))
    if terse:
        return _TERSE, [[started, name, "STANDARD", None, None] for name in names]
    columns = _columns(
        "created_on:ts name is_default is_current origin owner comment options "
        "retention_time kind owner_role_type"
    )
    rows = [
        [
            started,
            n,
            "N",
            _yn(n == current),
            "",
            OWNER,
            comments[n] or "",
            "",
            "1",
            "STANDARD",
            "ROLE",
        ]
        for n in names
    ]
    return columns, rows


def _show_schemas(
    cursor: duckdb.DuckDBPyConnection,
    terse: bool,
    scope: tuple[str | None, str | None],
    current: tuple[str | None, str | None],
    started: dt.datetime,
) -> Rows:
    where, params = _where(scope[0], None)
    found = _select(cursor, _SCHEMAS_SQL, where, params, order="database_name, schema_name")
    if terse:
        return _TERSE, [[started, name, None, database, None] for database, name, _ in found]
    columns = _columns(
        "created_on:ts name is_default is_current database_name owner comment options "
        "retention_time owner_role_type"
    )
    rows = [
        [
            started,
            name,
            "N",
            _yn((database, name) == current),
            database,
            "" if name == INFORMATION_SCHEMA else OWNER,
            comment or "",
            "",
            "1",
            "" if name == INFORMATION_SCHEMA else "ROLE",
        ]
        for database, name, comment in found
    ]
    return columns, rows


def _show_objects(
    cursor: duckdb.DuckDBPyConnection,
    what: str,
    terse: bool,
    scope: tuple[str | None, str | None],
    started: dt.datetime,
) -> Rows:
    where, params = _where(*scope)
    if what != "OBJECTS":
        where = f"({where}) AND kind = ?"
        params = [*params, what[:-1]]
    found = _select(cursor, _OBJECTS_SQL, where, params, order="database_name, schema_name, name")
    if terse:
        rows = [
            [started, name, kind, database, schema] for database, schema, name, kind, *_ in found
        ]
        return _TERSE, rows
    if what == "VIEWS":
        columns = _columns(
            "created_on:ts name reserved database_name schema_name owner comment text "
            "is_secure is_materialized owner_role_type change_tracking"
        )
        rows = [
            [started, name, "", database, schema, OWNER, comment or "", text, "false", "false",
             "ROLE", "OFF"]
            for database, schema, name, _kind, comment, _rows, text in found
        ]  # fmt: skip
        return columns, rows
    head = (
        "created_on:ts name database_name schema_name kind comment cluster_by rows:n bytes:n owner"
    )
    if what == "TABLES":
        columns = _columns(
            f"{head} retention_time automatic_clustering change_tracking is_external "
            "enable_schema_evolution owner_role_type is_event is_hybrid is_iceberg is_dynamic "
            "is_immutable"
        )
        tail = ["1", "OFF", "OFF", "N", "N", "ROLE", "N", "N", "N", "N", "N"]
    else:
        columns = _columns(f"{head} retention_time owner_role_type is_hybrid is_dynamic is_iceberg")
        tail = ["1", "ROLE", "N", "N", "N"]
    rows = [
        [started, name, database, schema, kind, comment or "", "", row_count, 0, OWNER, *tail]
        for database, schema, name, kind, comment, row_count, _text in found
    ]
    return columns, rows


def _narrow(statement: exp.Show, columns: list[Column], rows: list[list[Any]]) -> list[list[Any]]:
    """Apply the LIKE, STARTS WITH and LIMIT ... FROM clauses of a SHOW."""
    name_at = [column.name for column in columns].index("name")
    like = statement.args.get("like")
    if like:
        # LIKE in SHOW is case-insensitive, unlike the operator.
        pattern = re.compile(_like_regex(like.name), re.IGNORECASE | re.DOTALL)
        rows = [row for row in rows if pattern.fullmatch(row[name_at])]
    starts_with = statement.args.get("starts_with")
    if starts_with:
        rows = [row for row in rows if row[name_at].startswith(starts_with.name)]
    after = statement.args.get("from_") or statement.args.get("from")
    if after:
        rows = [row for row in rows if row[name_at] > after.name]
    limit = statement.args.get("limit")
    if limit:
        rows = rows[: int(limit.expression.name)]
    return rows


def _like_regex(pattern: str) -> str:
    return "".join(
        ".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern
    )


# DESCRIBE ----------------------------------------------------------------------


def describe(cursor: duckdb.DuckDBPyConnection, database: str, schema: str, name: str) -> Rows:
    """DESCRIBE TABLE or VIEW; the relation is known to exist."""
    found = _select(
        cursor,
        _COLUMNS_SQL,
        "database_name = ? AND schema_name = ? AND table_name = ?",
        [database, schema, name],
        order="column_index",
    )
    columns = _columns(
        'name type kind null? default "primary key" "unique key" check expression comment '
        '"policy name" "privacy domain"'
    )
    rows = [
        [
            column_name,
            _full_type(type_name, precision, scale),
            "COLUMN",
            _yn(nullable),
            default,
            "N",
            "N",
            None,
            None,
            comment,
            None,
            None,
        ]
        for (_d, _s, _t, column_name, _i, default, nullable, comment, type_name, precision, scale)
        in found
    ]  # fmt: skip
    return columns, rows


def _full_type(name: str, precision: int, scale: int) -> str:
    """A type name with its parameters, as DESCRIBE and SHOW COLUMNS print it."""
    if name == "NUMBER":
        return f"NUMBER({precision},{scale})"
    if name == "TEXT":
        return "VARCHAR(16777216)"
    if name == "BINARY":
        return "BINARY(8388608)"
    if name in ("TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"):
        return f"{name}(9)"
    return name


# INFORMATION_SCHEMA ---------------------------------------------------------


def _information_schema_views(database: str, started: dt.datetime) -> dict[str, str]:
    """DuckDB queries standing in for the INFORMATION_SCHEMA views of one database."""
    db = "'" + database.replace("'", "''") + "'"
    at = f"TIMESTAMPTZ '{started.isoformat()}'"
    objects = f"({_OBJECTS_SQL}) WHERE database_name = {db}"
    return {
        "SCHEMATA": f"""
            SELECT database_name AS "CATALOG_NAME", schema_name AS "SCHEMA_NAME",
                   CASE WHEN schema_name = '{INFORMATION_SCHEMA}' THEN NULL ELSE '{OWNER}' END
                       AS "SCHEMA_OWNER",
                   'NO' AS "IS_TRANSIENT", 'NO' AS "IS_MANAGED_ACCESS", 1 AS "RETENTION_TIME",
                   {at} AS "CREATED", {at} AS "LAST_ALTERED", comment AS "COMMENT"
            FROM ({_SCHEMAS_SQL}) WHERE database_name = {db}""",
        "TABLES": f"""
            SELECT database_name AS "TABLE_CATALOG", schema_name AS "TABLE_SCHEMA",
                   name AS "TABLE_NAME", '{OWNER}' AS "TABLE_OWNER",
                   CASE kind WHEN 'TABLE' THEN 'BASE TABLE' ELSE kind END AS "TABLE_TYPE",
                   'NO' AS "IS_TRANSIENT", CAST(NULL AS VARCHAR) AS "CLUSTERING_KEY",
                   row_count AS "ROW_COUNT",
                   CASE kind WHEN 'TABLE' THEN 0 END AS "BYTES",
                   CASE kind WHEN 'TABLE' THEN 1 END AS "RETENTION_TIME",
                   {at} AS "CREATED", {at} AS "LAST_ALTERED", 'NO' AS "AUTO_CLUSTERING_ON",
                   comment AS "COMMENT", 'NO' AS "IS_TEMPORARY", 'NO' AS "IS_ICEBERG",
                   'NO' AS "IS_DYNAMIC", 'NO' AS "IS_HYBRID"
            FROM {objects}""",
        "VIEWS": f"""
            SELECT database_name AS "TABLE_CATALOG", schema_name AS "TABLE_SCHEMA",
                   name AS "TABLE_NAME", '{OWNER}' AS "TABLE_OWNER",
                   definition AS "VIEW_DEFINITION", 'NO' AS "IS_SECURE",
                   {at} AS "CREATED", {at} AS "LAST_ALTERED", comment AS "COMMENT"
            FROM {objects} AND kind = 'VIEW'""",
        "COLUMNS": f"""
            SELECT database_name AS "TABLE_CATALOG", schema_name AS "TABLE_SCHEMA",
                   table_name AS "TABLE_NAME", column_name AS "COLUMN_NAME",
                   column_index AS "ORDINAL_POSITION", column_default AS "COLUMN_DEFAULT",
                   CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END AS "IS_NULLABLE",
                   type_name AS "DATA_TYPE",
                   CASE type_name WHEN 'TEXT' THEN 16777216 WHEN 'BINARY' THEN 8388608 END
                       AS "CHARACTER_MAXIMUM_LENGTH",
                   CASE type_name WHEN 'TEXT' THEN 16777216 WHEN 'BINARY' THEN 8388608 END
                       AS "CHARACTER_OCTET_LENGTH",
                   CASE type_name WHEN 'NUMBER' THEN type_precision END AS "NUMERIC_PRECISION",
                   CASE type_name WHEN 'NUMBER' THEN 10 END AS "NUMERIC_PRECISION_RADIX",
                   CASE type_name WHEN 'NUMBER' THEN type_scale END AS "NUMERIC_SCALE",
                   CASE WHEN type_name LIKE 'TIME%' THEN 9 END AS "DATETIME_PRECISION",
                   comment AS "COMMENT"
            FROM ({_COLUMNS_SQL}) WHERE database_name = {db}""",
    }


def is_information_schema(table: exp.Table) -> bool:
    return table.db == INFORMATION_SCHEMA


def expand_information_schema(
    statement: exp.Expression, database: str | None, started: dt.datetime
) -> None:
    """Replace references to INFORMATION_SCHEMA views with queries over DuckDB's catalog.

    Must run after the statement has been checked: the replacement is DuckDB
    SQL, which the checks would reject.
    """
    for table in list(statement.find_all(exp.Table)):
        if not isinstance(table.this, exp.Identifier) or not is_information_schema(table):
            continue
        catalog = table.catalog or database
        if catalog is None:
            raise errors.SnowflakeError(
                "090105",
                "22000",
                "Cannot perform SELECT. This session does not have a current database. "
                "Call 'USE DATABASE', or use a qualified name.",
            )
        view = _information_schema_views(catalog, started).get(table.name)
        if view is None:
            raise errors.unsupported(f"{INFORMATION_SCHEMA}.{table.name}")
        alias = table.alias or table.name
        query = sqlglot.parse_one(view, read="duckdb")
        table.replace(query.subquery(exp.to_identifier(alias, quoted=True)))


# Helpers ---------------------------------------------------------------------

_KINDS = {"ts": "timestamp_ltz", "n": "fixed"}


def _columns(spec: str) -> list[Column]:
    """Result columns from a compact spec: names, optionally "name:ts" or "name:n"."""
    columns = []
    for item in re.findall(r'"[^"]+"(?::\w+)?|\S+', spec):
        name, _, kind = item.rpartition(":") if ":" in item else (item, "", "")
        name = name.strip('"')
        if kind == "ts":
            columns.append(Column(name, "timestamp_ltz", scale=9))
        elif kind == "n":
            columns.append(fixed_column(name))
        else:
            columns.append(text_column(name))
    return columns


_TERSE = _columns("created_on:ts name kind database_name schema_name")
# SHOW GRANTS: always empty, as grants are accepted but not recorded.
GRANTS = _columns(
    "created_on:ts privilege granted_on name granted_to grantee_name grant_option granted_by"
)


def _where(database: str | None, schema: str | None) -> tuple[str, list[str]]:
    if database is None:
        return "TRUE", []
    if schema is None:
        return "database_name = ?", [database]
    return "database_name = ? AND schema_name = ?", [database, schema]


def _select(
    cursor: duckdb.DuckDBPyConnection,
    source: str,
    where: str = "TRUE",
    params: list[Any] | None = None,
    *,
    order: str | None = None,
) -> list[tuple]:
    sql = f"SELECT * FROM ({source}) WHERE {where}"
    if order:
        sql += f" ORDER BY {order}"
    return cursor.execute(sql, params or []).fetchall()


def _exists(cursor: duckdb.DuckDBPyConnection, source: str, where: str, params: list[Any]) -> bool:
    return bool(_select(cursor, source, where, params))


def _yn(flag: Any) -> str:
    return "Y" if flag else "N"


def _no_such_scope() -> errors.SnowflakeError:
    return errors.SnowflakeError(
        "002043",
        "02000",
        "SQL compilation error:\nObject does not exist, or operation cannot be performed.",
    )
