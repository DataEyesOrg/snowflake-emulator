"""Snowflake SQL in, DuckDB SQL out, by way of sqlglot."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sfemu import errors

SNOWFLAKE = "snowflake"
DUCKDB = "duckdb"


def parse(sql: str) -> list[exp.Expression]:
    """Parse Snowflake SQL into statements with Snowflake identifier casing applied."""
    try:
        statements = sqlglot.parse(sql, read=SNOWFLAKE)
    except ParseError as exc:
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


def to_duckdb(statement: exp.Expression) -> str:
    # Every identifier is quoted so DuckDB stores the case Snowflake would.
    return statement.sql(dialect=DUCKDB, identify=True)


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
    cte_names = {cte.alias for cte in statement.find_all(exp.CTE)}
    needs_database = needs_schema = False
    for table in statement.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            continue  # table functions, VALUES and the like
        if not table.db and table.name in cte_names:
            continue
        needs_database = needs_database or not table.catalog
        needs_schema = needs_schema or not table.db
    return needs_database, needs_schema


def _plain(message: str) -> str:
    # sqlglot underlines the offending token with ANSI escapes.
    return message.replace("\x1b[4m", "").replace("\x1b[0m", "")
