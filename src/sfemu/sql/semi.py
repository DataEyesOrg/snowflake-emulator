"""Semi-structured SQL: Snowflake's VARIANT syntax as DuckDB expressions over JSON.

The rewrite runs on a parsed Snowflake statement right before DuckDB SQL is
generated from it. It replaces what DuckDB has no equal for (``FLATTEN``,
``OBJECT_CONSTRUCT``, ``v['key']``, the ``ARRAY_*`` functions) with calls of
the macros in :mod:`sfemu.variant`, and it spells out the conversions
Snowflake applies silently when a VARIANT meets a typed value: ``v:n > 1``,
``v:name = 'ann'``, ``v:name::string``.

Those conversions need to know which expressions are VARIANTs. DuckDB cannot
be asked, as it sees the statement only after the rewrite, so the kind of an
expression is worked out here: from the functions and casts in it, and for a
column from the table, subquery, CTE or FLATTEN it comes from.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlglot import exp

from sfemu import errors, variant

# The kinds of value the rewrite tells apart.
VARIANT, ARRAY, OBJECT = "VARIANT", "ARRAY", "OBJECT"
NUMBER, TEXT, BOOLEAN = "NUMBER", "TEXT", "BOOLEAN"
DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ = "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ"
TEMPORAL = frozenset({DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ})
SEMI = frozenset({VARIANT, ARRAY, OBJECT})

# Column name to kind for a table or view, or None if there is no such relation.
Lookup = Callable[[exp.Table], "dict[str, str | None] | None"]

_KIND = "sf_kind"
# Subtrees written in DuckDB SQL by the emulator itself are left alone.
RAW = "sf_raw"
_FLATTEN = "sf_flatten"
_MISSING: Any = object()

_FLATTEN_COLUMNS = {
    "SEQ": NUMBER,
    "KEY": TEXT,
    "PATH": TEXT,
    "INDEX": NUMBER,
    "VALUE": VARIANT,
    "THIS": VARIANT,
}

_TYPE = exp.DataType.Type
_SEMI_TYPES = {
    _TYPE.VARIANT: VARIANT,
    _TYPE.OBJECT: OBJECT,
    _TYPE.MAP: OBJECT,
    _TYPE.ARRAY: ARRAY,
    _TYPE.JSON: VARIANT,
}


def type_kind(data_type: exp.DataType) -> str | None:
    """The kind of the values of a Snowflake or DuckDB data type."""
    this = data_type.this
    if this in _SEMI_TYPES:
        return _SEMI_TYPES[this]
    if this in exp.DataType.TEXT_TYPES:
        return TEXT
    if this in exp.DataType.NUMERIC_TYPES:
        return NUMBER
    if this == _TYPE.DATE:
        return DATE
    if this in (_TYPE.TIME, _TYPE.TIMETZ):
        return TIME
    if this in (_TYPE.TIMESTAMPLTZ, _TYPE.TIMESTAMPTZ):
        # TIMESTAMP_TZ is kept as an instant too: see sfemu.sql.dates.
        return TIMESTAMP_LTZ
    if this in exp.DataType.TEMPORAL_TYPES:
        return TIMESTAMP
    if this == _TYPE.BOOLEAN:
        return BOOLEAN
    return None


# Functions sqlglot has no class for, by name: (macro, extra arguments, result kind).
_IS = {
    "IS_OBJECT": ("sf_is", ["OBJECT"]),
    "IS_VARCHAR": ("sf_is", ["VARCHAR"]),
    "IS_CHAR": ("sf_is", ["VARCHAR"]),
    "IS_BOOLEAN": ("sf_is", ["BOOLEAN"]),
    "IS_INTEGER": ("sf_is", ["INTEGER"]),
    "IS_DECIMAL": ("sf_is_decimal", []),
    "IS_DOUBLE": ("sf_is_number", []),
    "IS_REAL": ("sf_is_number", []),
}
_AS = {
    "AS_VARCHAR": ("sf_as_text", [], TEXT),
    "AS_CHAR": ("sf_as_text", [], TEXT),
    "AS_INTEGER": ("sf_as_integer", [], NUMBER),
    "AS_DOUBLE": ("sf_as_double", [], NUMBER),
    "AS_REAL": ("sf_as_double", [], NUMBER),
    "AS_DECIMAL": ("sf_as_decimal", [], NUMBER),
    "AS_NUMBER": ("sf_as_decimal", [], NUMBER),
    "AS_BOOLEAN": ("sf_as_boolean", [], BOOLEAN),
    "AS_ARRAY": ("sf_as", ["ARRAY"], ARRAY),
    "AS_OBJECT": ("sf_as", ["OBJECT"], OBJECT),
}
_OBJECT_EDITS = {"OBJECT_DELETE": "sf_object_delete", "OBJECT_PICK": "sf_object_pick"}
# Snowflake functions the strict mode lets through because they are answered here.
FUNCTIONS = frozenset({*_IS, *_AS, *_OBJECT_EDITS, "TO_OBJECT"})

# One-argument functions of an array: class, macro, result kind.
_ARRAY_UNARY: list[tuple[type[exp.Expression], str, str]] = [
    (exp.ArraySize, "sf_array_size", NUMBER),
    (exp.ArrayCompact, "sf_array_compact", ARRAY),
    (exp.ArrayDistinct, "sf_array_distinct", ARRAY),
    (exp.ArrayReverse, "sf_array_reverse", ARRAY),
    (exp.Flatten, "sf_array_flatten", ARRAY),
    (exp.ArrayMin, "sf_array_min", VARIANT),
    (exp.ArrayMax, "sf_array_max", VARIANT),
    (exp.JSONKeys, "sf_object_keys", ARRAY),
    (exp.StripNullValue, "sf_strip_null_value", VARIANT),
    (exp.IsNullValue, "sf_is_null_value", BOOLEAN),
    (exp.Typeof, "sf_typeof", TEXT),
    (exp.ToArray, "sf_to_array", ARRAY),
]
# Functions of an array and a value, both VARIANTs: class, macro, value first?, result kind.
_ARRAY_BINARY: list[tuple[type[exp.Expression], str, bool, str]] = [
    (exp.ArrayContains, "sf_array_contains", True, BOOLEAN),
    (exp.ArrayPosition, "sf_array_position", True, NUMBER),
    (exp.ArrayAppend, "sf_array_append", False, ARRAY),
    (exp.ArrayPrepend, "sf_array_prepend", False, ARRAY),
    (exp.ArrayRemove, "sf_array_remove", False, ARRAY),
    (exp.ArrayExcept, "sf_array_except", False, ARRAY),
    (exp.ArrayOverlaps, "sf_arrays_overlap", False, BOOLEAN),
]

_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ)
_ARITHMETIC = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.IntDiv, exp.Mod)
# Functions that hand a VARIANT argument on as it is.
_PASS_THROUGH = (
    exp.Coalesce,
    exp.If,
    exp.Nvl2,
    exp.Case,
    exp.Nullif,
    exp.Min,
    exp.Max,
    exp.AnyValue,
    exp.Greatest,
    exp.Least,
    exp.Lag,
    exp.Lead,
    exp.FirstValue,
    exp.LastValue,
    exp.NthValue,
)
_NUMERIC_FUNCTIONS = (
    exp.Sum,
    exp.Avg,
    exp.Abs,
    exp.Round,
    exp.Floor,
    exp.Ceil,
    exp.Sqrt,
    exp.Pow,
    exp.Ln,
    exp.Log,
    exp.Exp,
    exp.Sign,
    exp.Stddev,
    exp.Variance,
    exp.Median,
)
_TEXT_FUNCTIONS = (
    exp.Upper,
    exp.Lower,
    exp.Trim,
    exp.Substring,
    exp.Concat,
    exp.DPipe,
    exp.GroupConcat,
    exp.JSONExtractScalar,
    exp.JSONFormat,
    exp.ArrayToString,
    exp.Typeof,
    exp.CheckJson,
)
_NUMBER_FUNCTIONS = (*_NUMERIC_FUNCTIONS, exp.Count, exp.Length, exp.ArraySize, exp.Neg)
_ARRAY_RESULTS = (
    exp.Array,
    exp.ArrayConstructCompact,
    exp.ArrayAgg,
    exp.ArrayUniqueAgg,
    exp.ArraySlice,
    exp.ArrayConcat,
    exp.ArrayInsert,
    exp.ArrayIntersect,
    exp.ArraySort,
    exp.Split,
    exp.StrtokToArray,
)
_OBJECT_RESULTS = (exp.Struct, exp.JSONObject, exp.ObjectInsert, exp.ObjectAgg, exp.StarMap)
_VARIANT_RESULTS = (exp.JSONExtract, exp.GetExtract, exp.Bracket, exp.ParseJSON, exp.ToVariant)
# Nodes whose VARIANT arguments are dealt with by a rule of their own, or not at all.
_AWARE = (
    *_PASS_THROUGH,
    *_ARRAY_RESULTS,
    *_OBJECT_RESULTS,
    *_VARIANT_RESULTS,
    *(cls for cls, _, _ in _ARRAY_UNARY),
    *(cls for cls, _, _, _ in _ARRAY_BINARY),
    exp.Cast,
    exp.TryCast,
    exp.Count,
    exp.Anonymous,
    exp.Explode,
    exp.JSONExtractScalar,
    exp.JSONFormat,
    exp.ArrayToString,
    exp.CheckJson,
    exp.GenerateSeries,
)


def rewrite(statement: exp.Expression, lookup: Lookup | None = None) -> list[str | None] | None:
    """Rewrite a statement in place; return the kinds of its result columns, if known.

    ``lookup`` gives the column kinds of a table or view. Without it only what
    the statement itself shows to be a VARIANT is treated as one.
    """
    rewriter = _Rewriter(lookup)
    rewriter.run(statement)
    return rewriter.projections(statement)


def _json() -> exp.DataType:
    return exp.DataType.build("JSON", dialect="duckdb")


def marked(node: exp.Expression, kind: str | None) -> exp.Expression:
    node.meta[_KIND] = kind
    return node


def call(name: str, *arguments: exp.Expression, kind: str | None) -> exp.Expression:
    call = exp.Anonymous(this=f"{variant.SCHEMA}.{name}", expressions=list(arguments))
    return marked(call, kind)


def text(value: exp.Expression) -> exp.Expression:
    """A VARIANT as the text Snowflake converts it to."""
    return call("sf_text", value, kind=TEXT)


def _as(value: exp.Expression, kind: str | None) -> exp.Expression:
    """A VARIANT converted for a comparison with, or use as, a value of another kind."""
    if kind == NUMBER:
        return marked(exp.cast(text(value), "DOUBLE"), NUMBER)
    if kind == BOOLEAN:
        return marked(exp.cast(text(value), "BOOLEAN"), BOOLEAN)
    return text(value)


def _literal_json(text: str, kind: str) -> exp.Expression:
    return marked(exp.Cast(this=exp.Literal.string(text), to=_json()), kind)


def swap(node: exp.Expression, build: Callable[[], exp.Expression]) -> exp.Expression:
    """Put what ``build`` returns in the place of ``node``.

    The place is noted first: ``build`` may move ``node`` or its children into
    the replacement, which re-parents them.
    """
    parent, key, index = node.parent, node.arg_key, node.index
    replacement = build()
    if parent is not None:
        parent.set(key, replacement, index=index)
    return replacement


class _Rewriter:
    def __init__(self, lookup: Lookup | None) -> None:
        self._lookup = lookup
        self._tables: dict[tuple[str, str, str], dict[str, str | None] | None] = {}
        self._done: set[int] = set()
        self._depth = 0

    def run(self, statement: exp.Expression) -> None:
        nodes = [node for node in statement.walk(bfs=False, prune=_is_raw) if not _is_raw(node)]
        # Children before their parents: a rule sees its arguments rewritten.
        for node in reversed(nodes):
            if id(node) not in self._done:
                self._rewrite(node)

    def projections(self, statement: exp.Expression) -> list[str | None] | None:
        query = statement
        while isinstance(query, exp.SetOperation):
            query = query.left
        if not isinstance(query, exp.Select) or any(p.is_star for p in query.expressions):
            return None
        return [self.kind(projection) for projection in query.expressions]

    # Kinds -----------------------------------------------------------------

    def kind(self, node: exp.Expression | None) -> str | None:
        if node is None:
            return None
        if _KIND not in node.meta:
            node.meta[_KIND] = None  # stops a cycle; replaced right below
            node.meta[_KIND] = self._infer(node)
        return node.meta[_KIND]

    def _infer(self, node: exp.Expression) -> str | None:
        found = dates.infer(self, node)
        if found is not NotImplemented:
            return found
        if isinstance(node, exp.Paren | exp.Alias | exp.WithinGroup | exp.Window | exp.Filter):
            return self.kind(node.this)
        if isinstance(node, exp.Subquery):
            query = node.this
            while isinstance(query, exp.SetOperation):
                query = query.left
            if isinstance(query, exp.Select) and len(query.expressions) == 1:
                return self.kind(query.expressions[0])
            return None
        if isinstance(node, exp.Literal):
            return TEXT if node.is_string else NUMBER
        if isinstance(node, exp.Boolean):
            return BOOLEAN
        if isinstance(node, exp.Cast | exp.TryCast):
            return type_kind(node.to)
        if isinstance(node, exp.Column):
            return self._column_kind(node)
        if isinstance(node, _VARIANT_RESULTS):
            return VARIANT
        if isinstance(node, _ARRAY_RESULTS):
            return ARRAY
        if isinstance(node, _OBJECT_RESULTS):
            return OBJECT
        if isinstance(node, exp.Anonymous):
            name = node.name.upper()
            if name in _AS:
                return _AS[name][2]
            if name in _OBJECT_EDITS or name == "TO_OBJECT":
                return OBJECT
            return BOOLEAN if name in _IS else None
        for cls, _, kind in _ARRAY_UNARY:
            if isinstance(node, cls):
                return kind
        for cls, _, _, kind in _ARRAY_BINARY:
            if isinstance(node, cls):
                return kind
        if isinstance(node, _PASS_THROUGH):
            return self._branch_kind(node)
        if isinstance(node, _TEXT_FUNCTIONS):
            return TEXT
        if isinstance(node, (*_NUMBER_FUNCTIONS, *_ARITHMETIC)):
            return NUMBER
        if isinstance(node, exp.Predicate | exp.Connector | exp.Not):
            return BOOLEAN
        return None

    def _branch_kind(self, node: exp.Expression) -> str | None:
        """The kind of a function that returns one of its arguments."""
        if isinstance(node, exp.Case):
            branches = [branch.args.get("true") for branch in node.args.get("ifs") or []]
            branches.append(node.args.get("default"))
        elif isinstance(node, exp.If | exp.Nvl2):
            branches = [node.args.get("true"), node.args.get("false")]
        elif isinstance(node, exp.Coalesce | exp.Greatest | exp.Least):
            branches = [node.this, *node.expressions]
        else:
            branches = [node.this]
        return next((kind for kind in map(self.kind, branches) if kind is not None), None)

    def _column_kind(self, column: exp.Column) -> str | None:
        holder: exp.Expression | None = column
        while holder is not None:
            holder = holder.find_ancestor(exp.Select, exp.Update, exp.Delete, exp.Merge)
            if holder is None:
                return None
            found = self._find(holder, column.table, column.name)
            if found is not _MISSING:
                return found
        return None

    def _find(self, holder: exp.Expression, table: str, name: str) -> Any:
        """The kind of a column among the FROM items of a statement; _MISSING if not there."""
        unsure = False
        for source in _sources(holder):
            if table and source.alias_or_name != table:
                continue
            found = self._source_column(source, name)
            if found is None:
                unsure = True  # a source whose columns are not known may have it
            elif found is not _MISSING:
                return found
        return None if unsure else _MISSING

    def _source_column(self, source: exp.Expression, name: str) -> Any:
        if source.meta.get(_FLATTEN) or isinstance(source.this, exp.Explode):
            return _FLATTEN_COLUMNS.get(name, _MISSING)
        if isinstance(source, exp.Subquery):
            return self._output(source.this, name)
        if not isinstance(source, exp.Table) or not isinstance(source.this, exp.Identifier):
            return None
        definition = None if source.db else _cte(source, source.name)
        if definition is not None:
            return self._output(definition, name)
        columns = self._table(source)
        if columns is None:
            return None
        return columns.get(name, _MISSING)

    def _output(self, query: exp.Expression, name: str) -> Any:
        """The kind of an output column of a subquery or CTE."""
        while isinstance(query, exp.SetOperation | exp.Subquery):
            query = query.left if isinstance(query, exp.SetOperation) else query.this
        if not isinstance(query, exp.Select) or self._depth > 20:
            return None
        self._depth += 1
        try:
            for projection in query.expressions:
                if not projection.is_star and projection.alias_or_name == name:
                    return self.kind(projection)
            for projection in query.expressions:
                if projection.is_star:  # the column comes through the star, or not at all
                    table = projection.table if isinstance(projection, exp.Column) else ""
                    return self._find(query, table, name)
            return _MISSING
        finally:
            self._depth -= 1

    def _table(self, table: exp.Table) -> dict[str, str | None] | None:
        if self._lookup is None:
            return None
        key = (table.catalog, table.db, table.name)
        if key not in self._tables:
            self._tables[key] = self._lookup(table)
        return self._tables[key]

    # Rewriting ---------------------------------------------------------------

    def _variant(self, value: exp.Expression) -> exp.Expression:
        """A value as a VARIANT. TO_JSON leaves what already is JSON alone."""
        if self.kind(value) in SEMI:
            return value
        return marked(exp.Anonymous(this="TO_JSON", expressions=[value]), VARIANT)

    def _rewrite(self, node: exp.Expression) -> None:
        if dates.rewrite(self, node):
            return
        for cls, macro, kind in _ARRAY_UNARY:
            if isinstance(node, cls):
                swap(node, lambda m=macro, k=kind: call(m, self._variant(node.this), kind=k))
                return
        for cls, macro, value_first, kind in _ARRAY_BINARY:
            if isinstance(node, cls):
                array, value = self._variant(node.this), self._variant(node.expression)
                arguments = (value, array) if value_first else (array, value)
                swap(node, lambda m=macro, a=arguments, k=kind: call(m, *a, kind=k))
                return
        rule = _RULES.get(type(node))
        if rule is not None:
            rule(self, node)
        elif isinstance(node, _COMPARISONS):
            self._balance(node, "this", "expression")
        elif isinstance(node, (*_ARITHMETIC, exp.Neg)):
            self._convert(node, NUMBER)
        elif isinstance(node, exp.Where | exp.Having | exp.Connector | exp.Not):
            self._convert(node, BOOLEAN)
        elif isinstance(node, exp.Func) and not isinstance(node, _AWARE):
            # Snowflake converts a VARIANT argument to what the function takes.
            self._convert(node, NUMBER if isinstance(node, _NUMERIC_FUNCTIONS) else TEXT)

    def _convert(self, node: exp.Expression, kind: str) -> None:
        """Convert the direct VARIANT arguments of a node to the kind it works on."""
        for value in list(node.args.values()):
            for child in value if isinstance(value, list) else [value]:
                if isinstance(child, exp.Order) and child.this is not None:
                    child = child.this  # LISTAGG(x) WITHIN GROUP (ORDER BY ...) keeps x here
                if isinstance(child, exp.Expression) and self.kind(child) in SEMI:
                    swap(child, lambda c=child: _as(c, kind))

    def _balance(self, node: exp.Expression, left: str, right: str) -> None:
        """A VARIANT compared with a typed value is converted to that value's type."""
        first, second = node.args.get(left), node.args.get(right)
        kinds = self.kind(first), self.kind(second)
        if isinstance(first, exp.Null) or isinstance(second, exp.Null):
            return
        if kinds[0] in SEMI and kinds[1] not in SEMI:
            swap(first, lambda: _as(first, kinds[1]))
        elif kinds[1] in SEMI and kinds[0] not in SEMI:
            swap(second, lambda: _as(second, kinds[0]))

    def _cast(self, node: exp.Cast) -> None:
        target = type_kind(node.to)
        value = node.this
        if target in SEMI:
            swap(node, lambda: marked(self._variant(value), target))
            return
        source = self.kind(value)
        if source in SEMI:
            swap(value, lambda: text(value))
        elif source is None and isinstance(value, exp.Column):
            # A column of a kind not known here: let DuckDB, which will know
            # its type, pick the branch. The other one is folded away.
            def dispatch() -> exp.Expression:
                unpacked = text(exp.Cast(this=value.copy(), to=_json()))
                converted = type(node)(this=unpacked, to=node.to.copy())
                is_json = exp.EQ(
                    this=exp.Anonymous(this="TYPEOF", expressions=[value.copy()]),
                    expression=exp.Literal.string("JSON"),
                )
                return marked(exp.Case().when(is_json, converted).else_(node), target)

            swap(node, dispatch)

    def _like(self, node: exp.Expression) -> None:
        self._convert(node, TEXT)

    def _in(self, node: exp.In) -> None:
        if self.kind(node.this) not in SEMI:
            return
        items = node.expressions
        kind = next((k for k in map(self.kind, items) if k is not None), TEXT)
        if kind not in SEMI:
            value = node.this
            swap(value, lambda: _as(value, kind))

    def _between(self, node: exp.Between) -> None:
        if self.kind(node.this) in SEMI:
            kind = self.kind(node.args.get("low"))
            value = node.this
            if kind not in SEMI:
                swap(value, lambda: _as(value, kind))

    def _ordered(self, node: exp.Ordered) -> None:
        """ORDER BY a VARIANT: numbers by size, and only then by their text."""
        value = node.this
        order = node.parent
        if self.kind(value) not in SEMI or not isinstance(order, exp.Order):
            return
        by_number = node.copy()
        number = exp.TryCast(this=text(value.copy()), to=exp.DataType.build("DOUBLE"))
        by_number.set("this", number)
        swap(value, lambda: text(value))
        keys = list(order.expressions)
        keys.insert(keys.index(node), by_number)
        order.set("expressions", keys)

    def _path(self, node: exp.Expression) -> None:
        # DuckDB's -> binds more loosely than IS NULL and the comparisons do.
        kind = self.kind(node)
        swap(node, lambda: marked(exp.Paren(this=node), kind))

    def _parse_json(self, node: exp.ParseJSON) -> None:
        macro = "sf_try_parse_json" if node.args.get("safe") else "sf_parse_json"
        swap(node, lambda: call(macro, node.this, kind=VARIANT))

    def _check_json(self, node: exp.CheckJson) -> None:
        swap(node, lambda: call("sf_check_json", node.this, kind=TEXT))

    def _to_variant(self, node: exp.ToVariant) -> None:
        swap(node, lambda: self._variant(node.this))

    def _to_json(self, node: exp.JSONFormat) -> None:
        swap(node, lambda: marked(exp.cast(self._variant(node.this), "VARCHAR"), TEXT))

    def _get(self, node: exp.Expression) -> None:
        if isinstance(node, exp.Bracket):
            if len(node.expressions) != 1:
                return
            key = node.expressions[0]
        else:
            key = node.expression
        swap(node, lambda: call("sf_get", self._variant(node.this), key, kind=VARIANT))

    def _object(self, node: exp.Expression) -> None:
        pairs = node.expressions
        wanted = exp.JSONKeyValue if isinstance(node, exp.JSONObject) else exp.PropertyEQ
        if not all(isinstance(pair, wanted) for pair in pairs):
            return
        macro = "sf_object_keep_null" if isinstance(node, exp.JSONObject) else "sf_object"
        if not pairs:
            swap(node, lambda: _literal_json("{}", OBJECT))
            return

        def build() -> exp.Expression:
            keys, values = [], []
            for pair in pairs:
                key = pair.this
                # An identifier here is the text of a key, kept in its own case.
                keys.append(
                    exp.Literal.string(key.name) if isinstance(key, exp.Identifier) else key
                )
                values.append(self._variant(pair.expression))
            return call(
                macro, exp.Array(expressions=keys), exp.Array(expressions=values), kind=OBJECT
            )

        swap(node, build)

    def _star_map(self, node: exp.StarMap) -> None:
        raise errors.unsupported("OBJECT_CONSTRUCT(*)")

    def _array(self, node: exp.Expression) -> None:
        items = node.expressions

        def build() -> exp.Expression:
            if not items:
                return _literal_json("[]", ARRAY)
            # JSON_ARRAY turns each argument into JSON: SQL NULL becomes JSON null.
            array = marked(exp.Anonymous(this="JSON_ARRAY", expressions=list(items)), ARRAY)
            if isinstance(node, exp.ArrayConstructCompact):
                return call("sf_array_compact", array, kind=ARRAY)
            return array

        swap(node, build)

    def _array_to_string(self, node: exp.ArrayToString) -> None:
        swap(
            node,
            lambda: call(
                "sf_array_to_string", self._variant(node.this), node.expression, kind=TEXT
            ),
        )

    def _array_slice(self, node: exp.ArraySlice) -> None:
        start, end = node.args.get("start"), node.args.get("end")
        swap(node, lambda: call("sf_array_slice", self._variant(node.this), start, end, kind=ARRAY))

    def _array_concat(self, node: exp.ArrayConcat) -> None:
        def build() -> exp.Expression:
            joined = self._variant(node.this)
            for other in node.expressions:
                joined = call("sf_array_cat", joined, self._variant(other), kind=ARRAY)
            return joined

        swap(node, build)

    def _array_insert(self, node: exp.ArrayInsert) -> None:
        position, value = node.args.get("position"), node.expression
        swap(
            node,
            lambda: call(
                "sf_array_insert",
                self._variant(node.this),
                position,
                self._variant(value),
                kind=ARRAY,
            ),
        )

    def _array_intersect(self, node: exp.ArrayIntersect) -> None:
        if len(node.expressions) == 2:
            first, second = (self._variant(item) for item in node.expressions)
            swap(node, lambda: call("sf_array_intersection", first, second, kind=ARRAY))

    def _array_sort(self, node: exp.ArraySort) -> None:
        descending = isinstance(node.args.get("asc"), exp.Boolean) and not node.args["asc"].this

        def build() -> exp.Expression:
            ordered = call("sf_array_sort", self._variant(node.this), kind=ARRAY)
            return call("sf_array_reverse", ordered, kind=ARRAY) if descending else ordered

        swap(node, build)

    def _range(self, node: exp.GenerateSeries) -> None:
        if not node.args.get("is_end_exclusive"):
            return  # not ARRAY_GENERATE_RANGE
        bounds = [node.args[key] for key in ("start", "end", "step") if node.args.get(key)]
        swap(
            node,
            lambda: marked(
                exp.Anonymous(
                    this="TO_JSON", expressions=[exp.Anonymous(this="RANGE", expressions=bounds)]
                ),
                ARRAY,
            ),
        )

    def _split(self, node: exp.Split) -> None:
        # The generator's STR_SPLIT gives a DuckDB list; SPLIT returns an ARRAY.
        swap(node, lambda: marked(exp.Anonymous(this="TO_JSON", expressions=[node]), ARRAY))

    def _strtok_to_array(self, node: exp.StrtokToArray) -> None:
        delimiters = node.expression or exp.Literal.string(" ")
        swap(node, lambda: call("sf_strtok_to_array", node.this, delimiters, kind=ARRAY))

    def _array_agg(self, node: exp.Expression) -> None:
        if isinstance(node, exp.ArrayUniqueAgg):
            value = node.this
            distinct = exp.Distinct(expressions=[value])
            node = swap(node, lambda: exp.ArrayAgg(this=distinct, nulls_excluded=True))
        # The values are aggregated as they are, so DISTINCT and ORDER BY see
        # them typed; TO_JSON then makes an array of a list of any type.
        # ORDER BY, OVER and FILTER wrap the aggregate; the array is made of the whole.
        top = node
        while isinstance(top.parent, exp.WithinGroup | exp.Window | exp.Filter) and (
            top.parent.this is top
        ):
            top = top.parent
            self._done.add(id(top))
        swap(top, lambda: call("sf_array_agg", top, kind=ARRAY))

    def _object_agg(self, node: exp.ObjectAgg) -> None:
        def build() -> exp.Expression:
            keys = exp.Anonymous(this="LIST", expressions=[node.this])
            values = exp.Anonymous(this="LIST", expressions=[self._variant(node.expression)])
            return call("sf_object", keys, values, kind=OBJECT)

        swap(node, build)

    def _object_insert(self, node: exp.ObjectInsert) -> None:
        key, value = node.args.get("key"), node.args.get("value")
        swap(
            node,
            lambda: call(
                "sf_object_insert", self._variant(node.this), key, self._variant(value), kind=OBJECT
            ),
        )

    def _is_array(self, node: exp.IsArray) -> None:
        self._is(node, node.this, "sf_is", ["ARRAY"])

    def _is(self, node: exp.Expression, value: exp.Expression, macro: str, extra: list) -> None:
        literals = [exp.Literal.string(text) for text in extra]
        swap(node, lambda: call(macro, self._variant(value), *literals, kind=BOOLEAN))

    def _anonymous(self, node: exp.Anonymous) -> None:
        name = node.name.upper()
        arguments = node.expressions
        if not arguments:
            return
        value = arguments[0]
        if name in _IS:
            self._is(node, value, *_IS[name])
        elif name in _AS:
            macro, extra, kind = _AS[name]
            literals = [exp.Literal.string(text) for text in extra]
            swap(node, lambda: call(macro, self._variant(value), *literals, kind=kind))
        elif name in _OBJECT_EDITS:
            keys = exp.Array(expressions=arguments[1:])
            macro = _OBJECT_EDITS[name]
            swap(node, lambda: call(macro, self._variant(value), keys, kind=OBJECT))
        elif name == "TO_OBJECT":
            swap(node, lambda: marked(self._variant(value), OBJECT))

    def _flatten(self, node: exp.Explode) -> None:
        holder = node.parent
        if not isinstance(holder, exp.Lateral | exp.TableFromRows | exp.Table) or (
            holder.this is not node
        ):
            return
        options: dict[str, exp.Expression] = {}
        for argument in [node.this, *node.expressions]:
            if isinstance(argument, exp.Kwarg):
                options[argument.this.name.upper()] = argument.expression
            elif argument is not None:
                options.setdefault("INPUT", argument)
        unknown = set(options) - {"INPUT", "PATH", "OUTER", "RECURSIVE", "MODE"}
        if unknown or "INPUT" not in options:
            raise errors.syntax_error("FLATTEN takes INPUT, PATH, OUTER, RECURSIVE and MODE.")
        mode = options.get("MODE")
        if mode is not None and mode.name.upper() != "BOTH":
            raise errors.unsupported(f"FLATTEN with MODE => '{mode.name.upper()}'")
        self._done.add(id(holder))

        def build() -> exp.Expression:
            flattened = call(
                "sf_flatten",
                self._variant(options["INPUT"]),
                options.get("PATH") or exp.Null(),
                options.get("OUTER") or exp.false(),
                options.get("RECURSIVE") or exp.false(),
                kind=None,
            )
            table = exp.Table(this=flattened)
            alias = holder.args.get("alias")
            if alias is not None and alias.this is not None:
                table.set("alias", exp.TableAlias(this=alias.this))
            table.meta[_FLATTEN] = True
            return table

        table = swap(holder, build)
        join = table.parent
        if (
            isinstance(join, exp.Join)
            and (join.side or join.kind in ("INNER", "OUTER"))
            and not join.args.get("on")
            and not join.args.get("using")
        ):
            join.set("on", exp.true())  # DuckDB wants a condition where Snowflake does not


_RULES: dict[type[exp.Expression], Callable[[_Rewriter, Any], None]] = {
    exp.Cast: _Rewriter._cast,
    exp.TryCast: _Rewriter._cast,
    exp.Like: _Rewriter._like,
    exp.ILike: _Rewriter._like,
    exp.DPipe: _Rewriter._like,
    exp.In: _Rewriter._in,
    exp.Between: _Rewriter._between,
    exp.Ordered: _Rewriter._ordered,
    exp.ParseJSON: _Rewriter._parse_json,
    exp.CheckJson: _Rewriter._check_json,
    exp.ToVariant: _Rewriter._to_variant,
    exp.JSONFormat: _Rewriter._to_json,
    exp.GetExtract: _Rewriter._get,
    exp.Bracket: _Rewriter._get,
    exp.Struct: _Rewriter._object,
    exp.JSONObject: _Rewriter._object,
    exp.StarMap: _Rewriter._star_map,
    exp.Array: _Rewriter._array,
    exp.ArrayConstructCompact: _Rewriter._array,
    exp.ArrayToString: _Rewriter._array_to_string,
    exp.ArraySlice: _Rewriter._array_slice,
    exp.ArrayConcat: _Rewriter._array_concat,
    exp.ArrayInsert: _Rewriter._array_insert,
    exp.ArrayIntersect: _Rewriter._array_intersect,
    exp.ArraySort: _Rewriter._array_sort,
    exp.GenerateSeries: _Rewriter._range,
    exp.Split: _Rewriter._split,
    exp.StrtokToArray: _Rewriter._strtok_to_array,
    exp.ArrayAgg: _Rewriter._array_agg,
    exp.ArrayUniqueAgg: _Rewriter._array_agg,
    exp.ObjectAgg: _Rewriter._object_agg,
    exp.ObjectInsert: _Rewriter._object_insert,
    exp.IsArray: _Rewriter._is_array,
    exp.Anonymous: _Rewriter._anonymous,
    exp.Explode: _Rewriter._flatten,
    exp.JSONExtract: _Rewriter._path,
    exp.JSONExtractScalar: _Rewriter._path,
}


def _is_raw(node: exp.Expression) -> bool:
    return bool(node.meta.get(RAW))


def _sources(holder: exp.Expression) -> list[exp.Expression]:
    """The FROM items a column of this statement may come from."""
    found = []
    if isinstance(holder, exp.Select):
        origin = holder.args.get("from_") or holder.args.get("from")
        found.append(origin.this if origin is not None else None)
    else:
        found.append(holder.this)
        found.append(holder.args.get("using"))
        origin = holder.args.get("from_") or holder.args.get("from")
        found.append(origin.this if origin is not None else None)
    found += [join.this for join in holder.args.get("joins") or []]
    return [source for source in found if isinstance(source, exp.Expression)]


def _cte(node: exp.Expression, name: str) -> exp.Expression | None:
    """The query of the CTE of this name that is in scope at a node."""
    holder: exp.Expression | None = node
    while holder is not None:
        clause = holder.args.get("with_") or holder.args.get("with")
        for cte in clause.expressions if clause is not None else []:
            if cte.alias == name:
                return cte.this
        holder = holder.parent
    return None


# The rules for dates and times. They build on what is defined above, so the
# module can only be imported once all of it exists.
from sfemu.sql import dates  # noqa: E402
