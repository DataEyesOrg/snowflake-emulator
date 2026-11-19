"""Snowflake SQL in, DuckDB SQL out, by way of sqlglot."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sfemu import errors

SNOWFLAKE = "snowflake"
DUCKDB = "duckdb"


def parse(sql: str) -> list[exp.Expression]:
    """Parse Snowflake SQL into statements with Snowflake identifier casing applied."""
    try:
        statements = sqlglot.parse(sql, read=SNOWFLAKE)
    except (ParseError, TokenError) as exc:
        # TokenError is the tokenizer's failure, e.g. an unterminated string.
        raise errors.syntax_error(_plain(str(exc))) from exc
    statements = [s for s in statements if s is not None]
    for statement in statements:
        # Keys of an object literal ({'a': 1}) are strings, not identifiers,
        # so they must keep their case.
        for pair in statement.find_all(exp.PropertyEQ):
            if isinstance(pair.this, exp.Identifier):
                pair.this.set("quoted", True)
    # Unquoted identifiers fold to upper case in Snowflake; do it once, here.
    return [normalize_identifiers(s, dialect=SNOWFLAKE) for s in statements]


# Snowflake's INT, BIGINT, SMALLINT, TINYINT and BYTEINT are all synonyms of
# NUMBER(38, 0); in DuckDB they would be 32-, 64-, 16- and 8-bit integers.
_INTEGER_TYPES = {
    exp.DataType.Type.INT,
    exp.DataType.Type.BIGINT,
    exp.DataType.Type.SMALLINT,
    exp.DataType.Type.TINYINT,
    exp.DataType.Type.MEDIUMINT,
}


def to_duckdb(statement: exp.Expression) -> str:
    statement = statement.copy()  # keep Snowflake's types in the caller's tree
    for data_type in list(statement.find_all(exp.DataType)):
        if data_type.this in _INTEGER_TYPES:
            data_type.replace(exp.DataType.build("DECIMAL(38, 0)"))
    # Every identifier is quoted so DuckDB stores the case Snowflake would.
    return statement.sql(dialect=DUCKDB, identify=True)


def bind_count(statement: exp.Expression) -> int:
    return sum(1 for _ in statement.find_all(exp.Placeholder))


def relations(statement: exp.Expression) -> list[exp.Table]:
    """The tables and views a statement refers to by name, CTE references excluded."""
    cte_names = {cte.alias for cte in statement.find_all(exp.CTE)}
    return [
        table
        for table in statement.find_all(exp.Table)
        if isinstance(table.this, exp.Identifier) and (table.db or table.name not in cte_names)
    ]


def check_supported(statement: exp.Expression) -> None:
    """Reject what would run on DuckDB but is not Snowflake.

    The statement types are filtered by the executor; this covers what hides
    inside an accepted statement.
    """
    for node in statement.walk():
        if isinstance(node, exp.Anonymous):
            # A function sqlglot has no Snowflake model for goes to DuckDB by
            # name: at best a DuckDB function (list_value), at worst a
            # same-named one with other semantics (hash).
            raise errors.unsupported(f"function {node.name.upper()}")
        if isinstance(node, exp.Table) and not isinstance(node.this, exp.Identifier):
            raise errors.unsupported(f"table function {_callable_name(node.this)}")
        if isinstance(node, exp.Unnest | exp.ReadCSV):
            raise errors.unsupported(f"table function {_callable_name(node)}")
        if isinstance(node, exp.Table) and node.db.upper() == "INFORMATION_SCHEMA":
            # DuckDB has an information_schema of its own, shaped differently.
            raise errors.unsupported("INFORMATION_SCHEMA")


def _callable_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return node.name.upper()
    if isinstance(node, exp.Func):
        return node.sql_name()
    return node.key.upper()


def name_projections(statement: exp.Expression) -> None:
    """Give unaliased expressions the column name Snowflake would derive.

    Snowflake names such a column after its SQL text, upper-cased. We derive it
    from sqlglot's rendering, so spacing can differ from the text as typed.
    """
    select = statement
    while isinstance(select, exp.SetOperation):
        select = select.left
    if not isinstance(select, exp.Select):
        return
    for projection in list(select.expressions):
        if isinstance(projection, exp.Alias | exp.Column | exp.Star) or projection.is_star:
            continue
        name = projection.sql(dialect=SNOWFLAKE).upper()
        projection.replace(exp.alias_(projection.copy(), exp.to_identifier(name, quoted=True)))


def unqualified_relations(statement: exp.Expression) -> tuple[bool, bool]:
    """Whether any relation relies on the current (database, schema) to resolve."""
    needs_database = needs_schema = False
    for table in relations(statement):
        needs_database = needs_database or not table.catalog
        needs_schema = needs_schema or not table.db
    return needs_database, needs_schema


def _plain(message: str) -> str:
    # sqlglot underlines the offending token with ANSI escapes.
    return message.replace("\x1b[4m", "").replace("\x1b[0m", "")
