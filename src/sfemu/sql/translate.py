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
    statements = [normalize_identifiers(s, dialect=SNOWFLAKE) for s in statements]
    for statement in statements:
        _rewrite_conversions(statement)
    return statements


# TO_TIMESTAMP* of one argument: sqlglot models it only for literals, since what
# it does depends on the argument's type. For text and timestamps it is a cast;
# a number (an epoch) fails in DuckDB's cast instead of being misread.
_CONVERSIONS = {
    "TO_TIMESTAMP": "TIMESTAMPNTZ",
    "TO_TIMESTAMP_NTZ": "TIMESTAMPNTZ",
    "TO_TIMESTAMP_LTZ": "TIMESTAMPLTZ",
    "TO_TIMESTAMP_TZ": "TIMESTAMPTZ",
}


def _rewrite_conversions(statement: exp.Expression) -> None:
    for node in list(statement.find_all(exp.Anonymous)):
        target = _CONVERSIONS.get(node.name.upper())
        if target and len(node.expressions) == 1:
            node.replace(exp.cast(node.expressions[0].copy(), target, dialect=SNOWFLAKE))


def tokens(sql: str) -> list[tuple[str, str]]:
    """(token type, text) pairs of a statement sqlglot could only keep as raw text."""
    try:
        return [(t.token_type.name, t.text) for t in sqlglot.tokenize(sql, read=SNOWFLAKE)]
    except TokenError as exc:
        raise errors.syntax_error(_plain(str(exc))) from exc


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
    for merge in statement.find_all(exp.Merge):
        # DuckDB takes bare column names as the targets of UPDATE SET.
        for update in merge.find_all(exp.Update):
            for assignment in update.expressions:
                if isinstance(assignment.this, exp.Column):
                    assignment.this.replace(exp.column(assignment.this.this.copy()))
    for properties in statement.find_all(exp.Properties):
        # Dropped silently by the generator otherwise, with a warning per statement.
        for prop in list(properties.expressions):
            if isinstance(prop, exp.TransientProperty | exp.CopyGrantsProperty):
                prop.pop()
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
        if isinstance(table.this, exp.Identifier)
        and (table.db or table.name not in cte_names)
        and table.db != "INFORMATION_SCHEMA"  # answered by sfemu.sql.metadata
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
