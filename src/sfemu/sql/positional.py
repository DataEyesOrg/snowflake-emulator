"""Columns by position: ``$1``, ``COLUMN1`` of a VALUES clause, ``OBJECT_CONSTRUCT(*)``.

All three need the names of the columns of a FROM item, in order. DuckDB has
no ``$1`` for a column, and names the columns of VALUES ``col0``, ``col1``. So
the names are worked out here, before any other rewrite, and written into the
statement: ``$2`` becomes the second column of the item it is read from.
"""

from __future__ import annotations

from sqlglot import exp

from sfemu import errors
from sfemu.sql import semi

_SNOWFLAKE = "snowflake"


def rewrite(statement: exp.Expression, lookup: semi.Lookup | None) -> None:
    """Name the columns of VALUES and replace positional references, in place."""
    anonymous = 0
    for values in list(statement.find_all(exp.Values)):
        if not isinstance(values.parent, exp.From | exp.Join):
            continue  # the rows of an INSERT
        alias = values.args.get("alias")
        if alias is None or alias.this is None:
            anonymous += 1
            alias = exp.TableAlias(this=exp.to_identifier(f"_VALUES_{anonymous}", quoted=True))
            values.set("alias", alias)
        if not alias.args.get("columns"):
            row = values.expressions[0] if values.expressions else None
            width = len(row.expressions) if isinstance(row, exp.Tuple) else 1
            alias.set(
                "columns",
                [exp.to_identifier(f"COLUMN{n}", quoted=True) for n in range(1, width + 1)],
            )
    # The innermost first: an outer $1 may read a column that is an inner $1.
    for node in reversed(list(statement.walk())):
        if isinstance(node, exp.Parameter) and isinstance(node.this, exp.Literal):
            _position(node, lookup)
        elif isinstance(node, exp.StarMap):
            _star_map(node, lookup)


def _position(parameter: exp.Parameter, lookup: semi.Lookup | None) -> None:
    select = parameter.find_ancestor(exp.Select)
    if parameter.this.is_string:
        return
    if select is None:
        raise errors.unsupported(f"${parameter.this.name} outside a SELECT")
    holder = parameter.parent
    qualified = isinstance(holder, exp.Column) and holder.this is parameter
    table = holder.table if qualified else ""
    sources = [s for s in semi.sources(select) if not table or s.alias_or_name == table]
    reference = f"${parameter.this.name}"
    names = _names(sources[0], lookup) if sources else None
    if names is None:
        raise errors.unsupported(f"{reference} for a FROM item whose columns are not known")
    index = int(parameter.this.name) - 1
    if not 0 <= index < len(names):
        raise errors.syntax_error(f"invalid identifier '{reference}'")
    alias = sources[0].alias_or_name
    column = exp.column(
        exp.to_identifier(names[index], quoted=True),
        table=exp.to_identifier(alias, quoted=True) if alias else None,
    )
    (holder if qualified else parameter).replace(column)


def _star_map(node: exp.StarMap, lookup: semi.Lookup | None) -> None:
    """OBJECT_CONSTRUCT(*): the columns of the FROM items under their names."""
    select = node.find_ancestor(exp.Select)
    star = node.this
    table = star.table if isinstance(star, exp.Column) else ""
    star = star.this if isinstance(star, exp.Column) else star
    if select is None or not isinstance(star, exp.Star) or not semi.sources(select):
        return  # refused by the rule in sfemu.sql.semi
    pairs: list[exp.Expression] = []
    left_out = {column.name for column in star.args.get("except_") or []}
    for source in semi.sources(select):
        if table and source.alias_or_name != table:
            continue
        names = _names(source, lookup)
        if names is None:
            raise errors.unsupported(
                "OBJECT_CONSTRUCT(*) of a FROM item whose columns are not known"
            )
        alias = source.alias_or_name
        for name in names:
            if name in left_out:
                continue
            column = exp.column(
                exp.to_identifier(name, quoted=True),
                table=exp.to_identifier(alias, quoted=True) if alias else None,
            )
            pairs.append(exp.PropertyEQ(this=exp.Literal.string(name), expression=column))
    node.replace(exp.Struct(expressions=pairs))


def _names(source: exp.Expression, lookup: semi.Lookup | None) -> list[str] | None:
    """The column names of a FROM item, in order, or None if they cannot be told."""
    alias = source.args.get("alias")
    renamed = [column.name for column in (alias.args.get("columns") or [])] if alias else []
    if renamed:
        return renamed
    if isinstance(source.this, exp.Explode):
        return list(semi.FLATTEN_COLUMNS)
    if isinstance(source, exp.Subquery):
        return _outputs(source.this, lookup)
    if not isinstance(source, exp.Table) or not isinstance(source.this, exp.Identifier):
        return None
    definition = None if source.db else semi.cte(source, source.name)
    if definition is not None:
        return _outputs(definition, lookup)
    columns = lookup(source) if lookup is not None else None
    return list(columns) if columns else None


def _outputs(query: exp.Expression, lookup: semi.Lookup | None) -> list[str] | None:
    """The names of the result columns of a query."""
    while isinstance(query, exp.SetOperation | exp.Subquery):
        query = query.left if isinstance(query, exp.SetOperation) else query.this
    if not isinstance(query, exp.Select):
        return None
    names: list[str] = []
    for projection in list(query.expressions):
        if projection.is_star:
            table = projection.table if isinstance(projection, exp.Column) else ""
            for source in semi.sources(query):
                if table and source.alias_or_name != table:
                    continue
                found = _names(source, lookup)
                if found is None:
                    return None
                names += found
        elif projection.alias_or_name:
            names.append(projection.alias_or_name)
        else:
            # An expression is named after its text, as at the top of a statement.
            name = projection.sql(dialect=_SNOWFLAKE).upper()
            projection.replace(exp.alias_(projection.copy(), exp.to_identifier(name, quoted=True)))
            names.append(name)
    return names
