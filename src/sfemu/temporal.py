"""Dates and timestamps: what Snowflake does with them that DuckDB does not.

Two things live here. The macros turn text into a timestamp the way
Snowflake's ``TO_TIMESTAMP`` does, where a string of digits is an epoch and an
offset may follow the time after a space. The format model reads Snowflake's
``'YYYY-MM-DD HH24:MI:SS'`` patterns for ``TO_CHAR``.

The statement rewriter in :mod:`sfemu.sql.dates` calls both.
"""

from __future__ import annotations

import duckdb

from sfemu.variant import SCHEMA

_S = SCHEMA
_DIGITS = "regexp_full_match(trim(s), '-?[0-9]+')"
# The text when it is no number. DuckDB evaluates both branches of a CASE over
# a constant, so neither may fail on the input meant for the other.
_REST = f"CASE WHEN NOT {_DIGITS} THEN s END"
# Snowflake's default TIMESTAMP_NTZ_OUTPUT_FORMAT, as a strftime format.
NTZ_TEXT = "%Y-%m-%d %H:%M:%S.%g"

# name, parameters, body. A body may use the macros defined before it.
_MACROS: list[tuple[str, str, str]] = [
    # A count of seconds since the epoch, times the factor that makes it microseconds.
    (
        "sf_from_epoch",
        "v, factor",
        "make_timestamp(CAST(CAST(v AS DECIMAL(38, 9)) * factor AS BIGINT))",
    ),
    # A number of seconds since the epoch, or of milli-, micro- or nanoseconds
    # when it is too large to be seconds: Snowflake's rule for digit strings.
    (
        "sf_epoch",
        "v",
        "make_timestamp(CASE WHEN abs(v) < 31536000000 THEN v * 1000000 "
        "WHEN abs(v) < 31536000000000 THEN v * 1000 "
        "WHEN abs(v) < 31536000000000000 THEN v ELSE v // 1000 END)",
    ),
    (
        "sf_to_timestamp",
        "s",
        f"CASE WHEN {_DIGITS} THEN {_S}.sf_epoch(TRY_CAST(trim(s) AS BIGINT)) "
        f"ELSE CAST({_REST} AS TIMESTAMP) END",
    ),
    (
        "sf_try_to_timestamp",
        "s",
        f"CASE WHEN {_DIGITS} THEN {_S}.sf_epoch(TRY_CAST(trim(s) AS BIGINT)) "
        f"ELSE TRY_CAST({_REST} AS TIMESTAMP) END",
    ),
    # '12:00:00 +0200' as DuckDB reads an offset: '12:00:00+02:00'.
    (
        "sf_offset_text",
        "s",
        "regexp_replace(regexp_replace(s, '\\s+(Z|[+-][0-9])', '\\1'), "
        "'([+-][0-9]{2})([0-9]{2})$', '\\1:\\2')",
    ),
    (
        "sf_to_timestamp_ltz",
        "s",
        f"CASE WHEN {_DIGITS} THEN {_S}.sf_epoch(TRY_CAST(trim(s) AS BIGINT)) AT TIME ZONE 'UTC' "
        f"ELSE CAST({_S}.sf_offset_text({_REST}) AS TIMESTAMPTZ) END",
    ),
    (
        "sf_try_to_timestamp_ltz",
        "s",
        f"CASE WHEN {_DIGITS} THEN {_S}.sf_epoch(TRY_CAST(trim(s) AS BIGINT)) AT TIME ZONE 'UTC' "
        f"ELSE TRY_CAST({_S}.sf_offset_text({_REST}) AS TIMESTAMPTZ) END",
    ),
    # Snowflake's default TIMESTAMP_LTZ_OUTPUT_FORMAT.
    # with the offset as +HHMM; strftime's %z writes +HH or +HH:MM.
    (
        "sf_ltz_text",
        "t",
        f"strftime(t, '{NTZ_TEXT} ') || rpad(replace(strftime(t, '%z'), ':', ''), 5, '0')",
    ),
]


def install(connection: duckdb.DuckDBPyConnection) -> None:
    for name, parameters, body in _MACROS:
        connection.execute(f"CREATE OR REPLACE MACRO {SCHEMA}.{name}({parameters}) AS {body}")


# Format models ---------------------------------------------------------------

STRFTIME, FRACTION, ZONE = "strftime", "fraction", "zone"

# Snowflake's format elements and their strftime equals. Longer ones first:
# MMMM before MM. The names of months and days come out as Jan and Mon
# whatever the case of the element.
_ELEMENTS = [
    ("YYYY", "%Y"),
    ("MMMM", "%B"),
    ("HH24", "%H"),
    ("HH12", "%I"),
    ("MON", "%b"),
    ("YY", "%y"),
    ("MM", "%m"),
    ("DD", "%d"),
    ("DY", "%a"),
    ("AM", "%p"),
    ("PM", "%p"),
    ("MI", "%M"),
    ("SS", "%S"),
]
_ZONES = ["TZH:TZM", "TZHTZM", "TZH", "TZM"]
# Digits of the fraction of a second that strftime writes by itself: %g is
# milliseconds and %f microseconds. Nanoseconds are microseconds and zeros.
_FRACTIONS = {0: "", 3: "%g", 6: "%f", 7: "%f0", 8: "%f00", 9: "%f000"}


def format_model(text: str) -> tuple[list[tuple[str, str]], bool]:
    """A Snowflake date format as parts, and whether it has any element at all.

    A part is ``(STRFTIME, format)``, ``(FRACTION, digits)`` for the leading
    digits of the microseconds, or ``(ZONE, element)`` for a piece of the
    time zone offset. Whatever is not an element stands for itself.
    """
    parts: list[tuple[str, str]] = []
    found = False
    upper = text.upper()
    position = 0

    def plain(chunk: str) -> None:
        if parts and parts[-1][0] == STRFTIME:
            parts[-1] = (STRFTIME, parts[-1][1] + chunk)
        else:
            parts.append((STRFTIME, chunk))

    while position < len(text):
        rest = upper[position:]
        if text[position] == '"':
            end = text.find('"', position + 1)
            end = len(text) if end < 0 else end
            plain(text[position + 1 : end].replace("%", "%%"))
            position = end + 1
            continue
        zone = next((z for z in _ZONES if rest.startswith(z)), None)
        if zone is not None:
            parts.append((ZONE, zone))
            position += len(zone)
        elif rest.startswith("FF"):
            digit = rest[2:3]
            digits = int(digit) if digit.isdigit() else 9
            position += 3 if digit.isdigit() else 2
            if digits in _FRACTIONS:
                plain(_FRACTIONS[digits])
            else:
                parts.append((FRACTION, str(digits)))
        else:
            element = next((e for e in _ELEMENTS if rest.startswith(e[0])), None)
            if element is None:
                plain(text[position].replace("%", "%%"))
                position += 1
                continue
            plain(element[1])
            position += len(element[0])
        found = True
    return parts, found
