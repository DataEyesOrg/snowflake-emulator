"""Semi-structured values: how VARIANT, OBJECT and ARRAY live in DuckDB.

All three are stored as DuckDB's JSON type, in one canonical text form: no
insignificant whitespace, object keys sorted. Snowflake does not keep the key
order of an object either, and one form per value is what makes equality,
GROUP BY and DISTINCT on variants come out right. Numbers keep their digits,
without the zeros that end a fraction.

JSON null and SQL NULL stay apart, as on Snowflake: ``v:key`` is the JSON
value ``null`` for a key holding null and SQL NULL for a missing key.

Snowflake's functions over these values are DuckDB macros in ``memory.main``,
which is no Snowflake database, so user SQL cannot define over them; the
statement rewriter in :mod:`sfemu.sql.semi` calls them by their full name.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb

SCHEMA = "memory.main"


class _Number(str):
    """A JSON number kept as the digits it was written with."""

    __slots__ = ()


def _decimal(text: str) -> _Number:
    # 1.50 and 1.5 are one value and must be one text; an exponent is kept,
    # as it is what tells a DOUBLE from a DECIMAL.
    if "e" not in text and "E" not in text:
        text = text.rstrip("0").rstrip(".")
    return _Number(text)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _load(text: str) -> Any:
    return json.loads(
        text, parse_int=_Number, parse_float=_decimal, parse_constant=_reject_constant
    )


def _dump(value: Any, indent: str | None, depth: int = 0) -> str:
    if isinstance(value, _Number):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    is_object = isinstance(value, dict)
    if not value:
        return "{}" if is_object else "[]"
    if indent is None:
        inner, outer, joiner, colon = "", "", ",", ":"
    else:
        inner, outer = "\n" + indent * (depth + 1), "\n" + indent * depth
        joiner, colon = "," + inner, ": "
    if is_object:
        items = [
            json.dumps(key, ensure_ascii=False) + colon + _dump(value[key], indent, depth + 1)
            for key in sorted(value)
        ]
        return "{" + inner + joiner.join(items) + outer + "}"
    items = [_dump(item, indent, depth + 1) for item in value]
    return "[" + inner + joiner.join(items) + outer + "]"


def canonical(text: str) -> str:
    """JSON text in the one form variants are stored in; raises ValueError if it is not JSON."""
    return _dump(_load(text), None)


def pretty(text: str) -> str:
    """A stored variant the way Snowflake prints it: indented by two spaces."""
    try:
        return _dump(_load(text), "  ")
    except ValueError:
        return text  # not JSON after all: a DuckDB value of another type, shown as it is


def _parse_json(text: str) -> str:
    try:
        return canonical(text)
    except (ValueError, RecursionError) as exc:
        raise ValueError(f"Error parsing JSON: {exc}") from None


def install(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the functions and macros every session's SQL may refer to."""
    connection.create_function("sf_canonical_json", _parse_json, ["VARCHAR"], "VARCHAR")
    for name, parameters, body in _MACROS:
        connection.execute(f"CREATE OR REPLACE MACRO {SCHEMA}.{name}({parameters}) AS {body}")
    connection.execute(_FLATTEN)


_S = SCHEMA
_NUMBERS = "('INTEGER', 'DECIMAL', 'DOUBLE')"
# A JSON array as a DuckDB list of JSON values, and such a list as an array again.
_L = "CAST({} AS JSON[])"
# The keys of the object o that are in the list ks, or those that are not,
# and the values of o under a list of keys.
_PICKED = "list_filter(json_keys(o), lambda k: list_contains({}, k))"
_OTHERS = "list_filter(json_keys(o), lambda k: NOT list_contains({}, k))"
_VALUES_OF = f"list_transform({{}}, lambda k: json_extract(o, {_S}.sf_key_path(k)))"

# name, parameters, body. A body may use the macros defined before it.
_MACROS: list[tuple[str, str, str]] = [
    ("sf_parse_json", "s", "CAST(sf_canonical_json(CAST(s AS VARCHAR)) AS JSON)"),
    (
        "sf_try_parse_json",
        "s",
        f"CASE WHEN json_valid(CAST(s AS VARCHAR)) THEN {_S}.sf_parse_json(s) END",
    ),
    (
        "sf_check_json",
        "s",
        "CASE WHEN s IS NULL OR json_valid(CAST(s AS VARCHAR)) THEN NULL "
        "ELSE 'syntax error in JSON' END",
    ),
    # A variant as text: a string without its quotes, anything else as JSON; NULL for JSON null.
    ("sf_text", "v", "json_extract_string(v, '$')"),
    ("sf_key_path", "k", """'$."' || replace(CAST(k AS VARCHAR), '"', '\\"') || '"'"""),
    (
        "sf_get",
        "v, k",
        f"json_extract(v, CASE WHEN typeof(k) = 'VARCHAR' THEN {_S}.sf_key_path(k) "
        "ELSE '$[' || CAST(k AS VARCHAR) || ']' END)",
    ),
    (
        "sf_typeof",
        "v",
        """CASE json_type(v)
            WHEN 'OBJECT' THEN 'OBJECT' WHEN 'ARRAY' THEN 'ARRAY' WHEN 'VARCHAR' THEN 'VARCHAR'
            WHEN 'BOOLEAN' THEN 'BOOLEAN' WHEN 'NULL' THEN 'NULL_VALUE'
            WHEN 'UBIGINT' THEN 'INTEGER' WHEN 'BIGINT' THEN 'INTEGER'
            WHEN 'DOUBLE' THEN
                CASE WHEN contains(lower(CAST(v AS VARCHAR)), 'e') THEN 'DOUBLE'
                     WHEN contains(CAST(v AS VARCHAR), '.') THEN 'DECIMAL' ELSE 'INTEGER' END
        END""",
    ),
    ("sf_is", "v, kind", f"{_S}.sf_typeof(v) = kind"),
    ("sf_is_number", "v", f"{_S}.sf_typeof(v) IN {_NUMBERS}"),
    ("sf_is_decimal", "v", f"{_S}.sf_typeof(v) IN ('INTEGER', 'DECIMAL')"),
    ("sf_is_null_value", "v", "json_type(v) = 'NULL'"),
    ("sf_strip_null_value", "v", "CASE WHEN json_type(v) = 'NULL' THEN NULL ELSE v END"),
    ("sf_as", "v, kind", f"CASE WHEN {_S}.sf_typeof(v) = kind THEN v END"),
    ("sf_as_text", "v", f"CASE WHEN json_type(v) = 'VARCHAR' THEN {_S}.sf_text(v) END"),
    (
        "sf_as_integer",
        "v",
        f"CASE WHEN {_S}.sf_typeof(v) = 'INTEGER' THEN CAST({_S}.sf_text(v) AS DECIMAL(38, 0)) END",
    ),
    (
        "sf_as_double",
        "v",
        f"CASE WHEN {_S}.sf_typeof(v) IN {_NUMBERS} THEN CAST({_S}.sf_text(v) AS DOUBLE) END",
    ),
    (
        "sf_as_decimal",
        "v",
        f"CASE WHEN {_S}.sf_typeof(v) IN {_NUMBERS} "
        f"THEN CAST(CAST({_S}.sf_text(v) AS DOUBLE) AS DECIMAL(38, 0)) END",
    ),
    (
        "sf_as_boolean",
        "v",
        f"CASE WHEN json_type(v) = 'BOOLEAN' THEN CAST({_S}.sf_text(v) AS BOOLEAN) END",
    ),
    # Objects ---------------------------------------------------------------
    # From aligned lists of keys and JSON values; a NULL key or value drops the pair.
    (
        "sf_object",
        "ks, vs",
        "CAST(map_from_entries(list_sort(list_filter(list_zip(ks, vs), "
        "lambda e: e[1] IS NOT NULL AND e[2] IS NOT NULL))) AS JSON)",
    ),
    (
        "sf_object_keep_null",
        "ks, vs",
        "CAST(map_from_entries(list_sort(list_filter(list_zip(ks, "
        "list_transform(vs, lambda x: coalesce(x, CAST('null' AS JSON)))), "
        "lambda e: e[1] IS NOT NULL))) AS JSON)",
    ),
    (
        "sf_object_keys",
        "o",
        "CASE WHEN json_type(o) = 'OBJECT' THEN to_json(list_sort(json_keys(o))) END",
    ),
    (
        "sf_object_pick",
        "o, ks",
        f"CASE WHEN json_type(o) = 'OBJECT' THEN {_S}.sf_object({_PICKED.format('ks')}, "
        f"{_VALUES_OF.format(_PICKED.format('ks'))}) END",
    ),
    (
        "sf_object_delete",
        "o, ks",
        f"CASE WHEN json_type(o) = 'OBJECT' THEN {_S}.sf_object({_OTHERS.format('ks')}, "
        f"{_VALUES_OF.format(_OTHERS.format('ks'))}) END",
    ),
    # The pair replaces one of the same key; a NULL value just removes the key.
    (
        "sf_object_insert",
        "o, key, v",
        f"CASE WHEN json_type(o) = 'OBJECT' THEN {_S}.sf_object("
        f"list_append({_OTHERS.format('[key]')}, key), "
        f"list_append({_VALUES_OF.format(_OTHERS.format('[key]'))}, v)) END",
    ),
    # Arrays ----------------------------------------------------------------
    (
        "sf_to_array",
        "v",
        "CASE WHEN v IS NULL THEN NULL WHEN json_type(v) = 'ARRAY' THEN v ELSE json_array(v) END",
    ),
    (
        "sf_array_size",
        "a",
        "CASE WHEN json_type(a) = 'ARRAY' THEN CAST(json_array_length(a) AS BIGINT) END",
    ),
    ("sf_array_contains", "v, a", f"list_contains({_L.format('a')}, v)"),
    (
        "sf_array_position",
        "v, a",
        f"nullif(list_position({_L.format('a')}, v), 0) - 1",
    ),
    (
        "sf_array_to_string",
        "a, sep",
        f"array_to_string(list_transform({_L.format('a')}, "
        f"lambda x: coalesce({_S}.sf_text(x), '')), sep)",
    ),
    (
        "sf_array_slice",
        "a, first, last",
        f"to_json(list_slice({_L.format('a')}, CASE WHEN first < 0 THEN first ELSE first + 1 END, "
        "CASE WHEN last < 0 THEN last - 1 ELSE last END))",
    ),
    ("sf_array_cat", "a, b", f"to_json(list_concat({_L.format('a')}, {_L.format('b')}))"),
    ("sf_array_append", "a, v", f"to_json(list_append({_L.format('a')}, v))"),
    ("sf_array_prepend", "a, v", f"to_json(list_prepend(v, {_L.format('a')}))"),
    (
        "sf_array_insert",
        "a, pos, v",
        f"to_json(list_concat(list_append(list_slice({_L.format('a')}, 1, pos), v), "
        f"list_slice({_L.format('a')}, pos + 1, json_array_length(a))))",
    ),
    (
        "sf_array_compact",
        "a",
        f"to_json(list_filter({_L.format('a')}, lambda x: json_type(x) <> 'NULL'))",
    ),
    (
        "sf_array_distinct",
        "a",
        f"to_json(list_filter({_L.format('a')}, "
        f"lambda x, i: list_position({_L.format('a')}, x) = i))",
    ),
    ("sf_array_remove", "a, v", f"to_json(list_filter({_L.format('a')}, lambda x: x <> v))"),
    (
        "sf_array_intersection",
        "a, b",
        f"to_json(list_filter({_L.format('a')}, lambda x: list_contains({_L.format('b')}, x)))",
    ),
    (
        "sf_array_except",
        "a, b",
        f"to_json(list_filter({_L.format('a')}, lambda x: NOT list_contains({_L.format('b')}, x)))",
    ),
    ("sf_arrays_overlap", "a, b", f"list_has_any({_L.format('a')}, {_L.format('b')})"),
    (
        "sf_array_flatten",
        "a",
        f"to_json(flatten(list_transform({_L.format('a')}, lambda x: CAST(x AS JSON[]))))",
    ),
    ("sf_array_reverse", "a", f"to_json(list_reverse({_L.format('a')}))"),
    # Sorted as values, not as JSON text: numbers by size, then everything else.
    (
        "sf_sorted",
        "a",
        f"list_transform(list_sort(list_transform("
        f"list_filter({_L.format('a')}, lambda x: json_type(x) <> 'NULL'), "
        f"lambda x: {{'number': TRY_CAST({_S}.sf_text(x) AS DOUBLE), 'value': x}})), "
        "lambda e: e.value)",
    ),
    ("sf_array_sort", "a", f"to_json({_S}.sf_sorted(a))"),
    ("sf_array_min", "a", f"{_S}.sf_sorted(a)[1]"),
    ("sf_array_max", "a", f"{_S}.sf_sorted(a)[-1]"),
    # The result of an ARRAY_AGG: a JSON array, empty when no value was aggregated.
    ("sf_array_agg", "items", "coalesce(to_json(items), CAST('[]' AS JSON))"),
    (
        "sf_strtok_to_array",
        "s, delimiters",
        "to_json(list_filter(str_split_regex(s, '[' || regexp_escape(delimiters) || ']'), "
        "lambda x: x <> ''))",
    ),
]

# FLATTEN: one row per element of an array or member of an object, with the
# columns Snowflake names. json_tree numbers the root 0, so its children are
# the rows whose parent is 0; a recursive FLATTEN takes all the descendants.
_FLATTEN = f"""
CREATE OR REPLACE MACRO {_S}.sf_flatten(input, path, is_outer, is_recursive, mode) AS TABLE
WITH source AS (
    SELECT CASE WHEN coalesce(path, '') = '' THEN input
                ELSE json_extract(input, '$.' || path) END AS root,
           coalesce(path, '') AS prefix
)
SELECT 1 AS "SEQ",
       CASE WHEN item.fullkey NOT LIKE '%]' THEN item.key END AS "KEY",
       CASE WHEN item.id IS NULL THEN NULL
            WHEN source.prefix = '' THEN ltrim(substr(item.fullkey, 2), '.')
            ELSE source.prefix || substr(item.fullkey, 2) END AS "PATH",
       CASE WHEN item.fullkey LIKE '%]' THEN CAST(item.key AS BIGINT) END AS "INDEX",
       item.value AS "VALUE",
       CASE WHEN item.id IS NOT NULL THEN json_extract(source.root, item.path) END AS "THIS"
FROM source
LEFT JOIN LATERAL (
    SELECT * FROM json_tree(source.root) AS node
    WHERE json_type(source.root) IN ('ARRAY', 'OBJECT')
      AND (node.parent = 0 OR (is_recursive AND node.parent IS NOT NULL))
      AND (mode = 'BOTH' OR (json_type(source.root) = mode
                             AND (mode = 'ARRAY') = (node.fullkey LIKE '%]')))
) AS item ON TRUE
WHERE is_outer OR item.id IS NOT NULL
ORDER BY item.id
"""
