"""Dates, times and timestamps: where Snowflake's results differ from DuckDB's.

These are rules of the statement rewriter in :mod:`sfemu.sql.semi`, which
works out the kind of every expression. Most of what is here needs that kind:

- a DATE moved by days, months or years stays a DATE, where DuckDB answers
  with a TIMESTAMP: ``DATEADD``, ``DATE_TRUNC``, ``ADD_MONTHS``, ``+ INTERVAL``;
- a timestamp as text has Snowflake's default format, with milliseconds;
- ``TO_TIMESTAMP`` of a number is an epoch, of text is a parse;
- ``TO_CHAR`` with a format takes Snowflake's format elements.

``TIMESTAMP_TZ`` has no equal in DuckDB, whose TIMESTAMPTZ is an instant with
no offset of its own. It is kept as one, which is ``TIMESTAMP_LTZ``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from sqlglot import exp
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.time import format_time

from sfemu import errors, temporal
from sfemu.sql import semi

DATE, TIME, TIMESTAMP, LTZ = semi.DATE, semi.TIME, semi.TIMESTAMP, semi.TIMESTAMP_LTZ
STAMPS = frozenset({TIMESTAMP, LTZ})

# TO_TIMESTAMP and its kin with one argument, which sqlglot leaves as plain
# function calls because what they do depends on the argument's type:
# name to (kind of the result, is it the TRY_ form).
_TO_TIMESTAMP = {
    f"{prefix}{name}": (kind, bool(prefix))
    for prefix in ("", "TRY_")
    for name, kind in [
        ("TO_TIMESTAMP", TIMESTAMP),
        ("TO_TIMESTAMP_NTZ", TIMESTAMP),
        ("TO_TIMESTAMP_LTZ", LTZ),
        ("TO_TIMESTAMP_TZ", LTZ),
    ]
}
# TO_DATE of one argument, which sqlglot leaves alone when it is a string of
# digits, an epoch: name to whether it is the TRY_ form.
_TO_DATE = {"TO_DATE": False, "DATE": False, "TRY_TO_DATE": True}
# Snowflake functions the strict mode lets through because they are answered here.
FUNCTIONS = frozenset({*_TO_TIMESTAMP, *_TO_DATE})

_DATE_UNITS = {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY"}
# The names Snowflake takes for the units an interval can keep a DATE a DATE with.
_UNIT_NAMES = {
    **dict.fromkeys(["Y", "YY", "YYY", "YYYY", "YR", "YEAR"], "YEAR"),
    **dict.fromkeys(["Q", "QTR", "QUARTER"], "QUARTER"),
    **dict.fromkeys(["MM", "MON", "MONTH"], "MONTH"),
    **dict.fromkeys(["W", "WK", "WEEK", "WEEKOFYEAR", "WOY", "WY"], "WEEK"),
    **dict.fromkeys(["D", "DD", "DAY", "DAYOFMONTH"], "DAY"),
}
_INTERVAL_PART = re.compile(r"(-?\d+)\s*([A-Za-z]+)")
# A literal that is a time of day and not a date: 10:20, 10:20:30.5
_TIME_TEXT = re.compile(r"\s*\d{1,2}:\d")

_DATES = (
    exp.CurrentDate,
    exp.TsOrDsToDate,
    exp.StrToDate,
    exp.LastDay,
    exp.NextDay,
    exp.PreviousDay,
    exp.DateFromParts,
    exp.Date,
)
_TIMES = (exp.TsOrDsToTime, exp.TimeFromParts, exp.Time, exp.Localtime, exp.CurrentTime)
# The result is of the kind of the first argument.
_SAME_KIND = (exp.TimestampTrunc, exp.DateTrunc, exp.AddMonths, exp.TimeSlice)
_NUMBERS = (
    exp.DateDiff,
    exp.Extract,
    exp.DayOfWeek,
    exp.DayOfWeekIso,
    exp.DayOfMonth,
    exp.DayOfYear,
    exp.WeekOfYear,
    exp.YearOfWeek,
    exp.Year,
    exp.Quarter,
    exp.Month,
    exp.Day,
    exp.Hour,
    exp.Minute,
    exp.Second,
    exp.MonthsBetween,
)
_TEXTS = (exp.ToChar, exp.TimeToStr, exp.Dayname, exp.Monthname)

# The arguments that are dates or times, by function.
_MOMENTS: dict[type[exp.Expression], tuple[str, ...]] = {
    exp.DateDiff: ("this", "expression"),
    exp.Extract: ("expression",),
    **dict.fromkeys(
        [
            exp.DateAdd,
            exp.TimeAdd,
            exp.TimestampTrunc,
            exp.DateTrunc,
            exp.AddMonths,
            exp.LastDay,
            exp.NextDay,
            exp.PreviousDay,
            exp.TimeSlice,
            exp.Dayname,
            exp.Monthname,
            exp.DayOfWeek,
            exp.DayOfWeekIso,
            exp.DayOfMonth,
            exp.DayOfYear,
            exp.WeekOfYear,
            exp.YearOfWeek,
            exp.Year,
            exp.Quarter,
            exp.Month,
            exp.Day,
        ],
        ("this",),
    ),
}

_Rewriter = Any  # semi._Rewriter, which this module only calls back into


def infer(rewriter: _Rewriter, node: exp.Expression) -> Any:
    """The kind of a date or time expression, or NotImplemented for any other."""
    if isinstance(node, _DATES):
        return DATE
    if isinstance(node, _TIMES):
        return TIME
    if isinstance(node, exp.CurrentTimestamp):
        return TIMESTAMP if node.args.get("sysdate") else LTZ
    if isinstance(node, exp.StrToTime | exp.UnixToTime):
        target = node.args.get("target_type")
        return semi.type_kind(target) if target is not None else TIMESTAMP
    if isinstance(node, exp.TimestampFromParts | exp.ConvertTimezone):
        return TIMESTAMP
    if isinstance(node, exp.DateAdd | exp.TimeAdd):
        return _shifted(rewriter.kind(node.this), {_unit(node.args.get("unit"))})
    if isinstance(node, _SAME_KIND):
        return rewriter.kind(node.this)
    if isinstance(node, exp.Add | exp.Sub):
        left, right = rewriter.kind(node.this), rewriter.kind(node.expression)
        if isinstance(node.expression, exp.Interval) and left in semi.TEMPORAL:
            return _shifted(left, _interval_units(node.expression))
        if isinstance(node.this, exp.Interval) and right in semi.TEMPORAL:
            return _shifted(right, _interval_units(node.this))
        if left in semi.TEMPORAL:
            # The days between two dates are a number; a date and a number, a date.
            return semi.NUMBER if right in semi.TEMPORAL else left
        return right if right in semi.TEMPORAL and isinstance(node, exp.Add) else NotImplemented
    if isinstance(node, exp.Anonymous):
        name = node.name.upper()
        if name in _TO_DATE:
            return DATE
        return _TO_TIMESTAMP[name][0] if name in _TO_TIMESTAMP else NotImplemented
    if isinstance(node, _NUMBERS):
        return semi.NUMBER
    if isinstance(node, _TEXTS):
        return semi.TEXT
    return NotImplemented


def rewrite(rewriter: _Rewriter, node: exp.Expression) -> bool:
    """Apply the rule for a node, if there is one; True if the node is dealt with."""
    for key in _MOMENTS.get(type(node), ()):
        value = node.args.get(key)
        if value is None:
            continue
        kind = rewriter.kind(value)
        # Text where a date is wanted is read as a timestamp, a VARIANT as its text.
        if value.is_string and _TIME_TEXT.match(value.name):
            semi.swap(value, lambda v=value: _cast(v, "TIME", TIME))
        elif kind in semi.SEMI:
            semi.swap(value, lambda v=value: _cast(semi.text(v), "TIMESTAMP", TIMESTAMP))
        elif kind == semi.TEXT:
            semi.swap(value, lambda v=value: _cast(v, "TIMESTAMP", TIMESTAMP))
    rule = _RULES.get(type(node))
    return bool(rule and rule(rewriter, node))


def _unit(unit: exp.Expression | None) -> str:
    name = unit.name.upper() if unit is not None else ""
    return _UNIT_NAMES.get(name.removesuffix("S") or name, name)


def _interval_units(interval: exp.Interval) -> set[str]:
    if interval.args.get("unit") is not None:
        return {_unit(interval.args["unit"])}
    parts = _INTERVAL_PART.findall(interval.this.name) if interval.this is not None else []
    return {_unit(exp.var(unit)) for _, unit in parts} or {""}


def _shifted(kind: str | None, units: set[str]) -> str | None:
    """The kind of a value moved by an interval: a DATE moved by hours is a TIMESTAMP."""
    if kind == DATE and not units <= _DATE_UNITS:
        return TIMESTAMP
    return kind


def _cast(value: exp.Expression, to: str, kind: str) -> exp.Expression:
    return semi.marked(exp.Cast(this=value, to=exp.DataType.build(to, dialect="duckdb")), kind)


def _as_date(node: exp.Expression) -> None:
    """Wrap a node DuckDB computes as a TIMESTAMP into the DATE Snowflake returns."""
    semi.swap(node, lambda: _cast(node, "DATE", DATE))


def _text(rewriter: _Rewriter, value: exp.Expression) -> exp.Expression | None:
    """A timestamp as the text Snowflake converts it to, or None for anything else."""
    kind = rewriter.kind(value)
    if kind == TIMESTAMP:
        pattern = exp.Literal.string(temporal.NTZ_TEXT)
        return semi.marked(exp.Anonymous(this="STRFTIME", expressions=[value, pattern]), semi.TEXT)
    if kind == LTZ:
        return semi.call("sf_ltz_text", value, kind=semi.TEXT)
    return None


# Rules -------------------------------------------------------------------------


def _date_add(rewriter: _Rewriter, node: exp.Expression) -> bool:
    if rewriter.kind(node.this) == DATE and rewriter.kind(node) == DATE:
        _as_date(node)
    return False


def _same_kind(rewriter: _Rewriter, node: exp.Expression) -> bool:
    if rewriter.kind(node.this) == DATE:
        _as_date(node)
    return False


def _interval(rewriter: _Rewriter, node: exp.Interval) -> bool:
    """INTERVAL '1 day, 2 hours': DuckDB takes the parts without the commas."""
    value = node.this
    if node.args.get("unit") is None and isinstance(value, exp.Literal) and "," in value.name:
        value.replace(exp.Literal.string(" ".join(part.strip() for part in value.name.split(","))))
    return False


def _arithmetic(rewriter: _Rewriter, node: exp.Expression) -> bool:
    if rewriter.kind(node) == DATE and isinstance(node.this, exp.Interval) != isinstance(
        node.expression, exp.Interval
    ):
        _as_date(node)
    return False  # a VARIANT operand is still to be converted


def _convert(rewriter: _Rewriter, node: exp.Expression) -> bool:
    """CAST(ts AS VARCHAR), and CAST of text or a number to a timestamp or a date."""
    target = semi.type_kind(node.to)
    value = node.this
    source = rewriter.kind(value)
    safe = isinstance(node, exp.TryCast)
    if target == semi.TEXT and source in STAMPS:
        semi.swap(node, lambda: _text(rewriter, value))
        return True
    if target in STAMPS and source in (semi.TEXT, semi.NUMBER):
        if _written_out(value) and target == TIMESTAMP:
            return False
        semi.swap(node, lambda: _timestamp(value, source, target, safe))
        return True
    if target == DATE and source == semi.TEXT and not _written_out(value):
        semi.swap(node, lambda: _date(value, safe))
        return True
    return False


def _written_out(value: exp.Expression) -> bool:
    """A literal that is a date and no epoch: DuckDB's own cast reads it."""
    return isinstance(value, exp.Literal) and not value.name.strip().lstrip("-").isdigit()


def _date(value: exp.Expression, safe: bool) -> exp.Expression:
    """Text as a DATE: the day of the timestamp it reads as."""
    return _cast(_timestamp(value, semi.TEXT, TIMESTAMP, safe), "DATE", DATE)


def _to_date(rewriter: _Rewriter, node: exp.TsOrDsToDate) -> bool:
    value = node.this
    if node.args.get("format") is not None or rewriter.kind(value) != semi.TEXT:
        return False
    if _written_out(value):
        return False
    semi.swap(node, lambda: _date(value, bool(node.args.get("safe"))))
    return True


def _timestamp(
    value: exp.Expression, source: str | None, target: str, safe: bool
) -> exp.Expression:
    """TO_TIMESTAMP of one argument, by the kind of the argument."""
    if source in semi.TEMPORAL:
        return _cast(value, "TIMESTAMPTZ" if target == LTZ else "TIMESTAMP", target)
    if source == semi.NUMBER:
        return _epoch(value, 0, target)  # seconds, with a fraction if the number has one
    if source in semi.SEMI:
        value = semi.text(value)
    macro = ("sf_try_to_timestamp" if safe else "sf_to_timestamp") + ("_ltz" * (target == LTZ))
    return semi.call(macro, exp.cast(value, "VARCHAR"), kind=target)


def _unix_to_time(rewriter: _Rewriter, node: exp.UnixToTime) -> bool:
    """TO_TIMESTAMP(x) of a literal, and TO_TIMESTAMP(x, scale)."""
    value, scale = node.this, node.args.get("scale")
    target = rewriter.kind(node)
    if scale is None:
        semi.swap(node, lambda: _timestamp(value, rewriter.kind(value), target, False))
        return True
    if not isinstance(scale, exp.Literal) or scale.is_string:
        raise errors.unsupported("TO_TIMESTAMP with a scale that is not a number")
    semi.swap(node, lambda: _epoch(value, int(scale.name), target))
    return True


def _epoch(value: exp.Expression, scale: int, target: str) -> exp.Expression:
    """A count of 10^-scale seconds since the epoch as a timestamp."""
    factor = exp.Literal.number(format(Decimal(10) ** (6 - scale), "f"))
    if target == LTZ:
        moment = semi.call("sf_from_epoch", value, factor, kind=None)
        return semi.marked(exp.AtTimeZone(this=moment, zone=exp.Literal.string("UTC")), LTZ)
    return semi.call("sf_from_epoch", value, factor, kind=TIMESTAMP)


def _anonymous(rewriter: _Rewriter, node: exp.Anonymous) -> bool:
    name = node.name.upper()
    if name not in FUNCTIONS:
        return False
    if len(node.expressions) != 1:
        raise errors.unsupported(f"{name} with {len(node.expressions)} arguments")
    value = node.expressions[0]
    if name in _TO_DATE:
        moment = _timestamp(value, rewriter.kind(value), TIMESTAMP, _TO_DATE[name])
        semi.swap(node, lambda: _cast(moment, "DATE", DATE))
        return True
    target, safe = _TO_TIMESTAMP[name]
    semi.swap(node, lambda: _timestamp(value, rewriter.kind(value), target, safe))
    return True


def _to_char(rewriter: _Rewriter, node: exp.ToChar) -> bool:
    pattern = node.args.get("format")
    if pattern is None:
        text = _text(rewriter, node.this)
        if text is not None:
            semi.swap(node, lambda: text)
        return text is not None
    written = _constant_text(pattern)
    if written is None:
        raise errors.unsupported("TO_CHAR with a format that is not a string literal")
    return _format(rewriter, node, written)


def _constant_text(node: exp.Expression) -> str | None:
    """The text of a string literal, or of literals joined by ||, as dbt writes a format."""
    if isinstance(node, exp.Paren):
        return _constant_text(node.this)
    if isinstance(node, exp.Literal):
        return node.name if node.is_string else None
    if isinstance(node, exp.DPipe):
        parts = [_constant_text(node.this), _constant_text(node.expression)]
        return None if None in parts else "".join(parts)
    return None


def _time_to_str(rewriter: _Rewriter, node: exp.TimeToStr) -> bool:
    """TO_CHAR of what sqlglot saw to be a date.

    A format that is one literal has been turned into a strftime format by
    then, and is turned back; one made of several is still as it was written.
    """
    pattern = node.args["format"]
    if isinstance(pattern, exp.Literal):
        written = format_time(
            pattern.name, Snowflake.INVERSE_TIME_MAPPING, Snowflake.INVERSE_TIME_TRIE
        )
        return _format(rewriter, node, written or pattern.name)
    written = _constant_text(pattern)
    if written is None:
        raise errors.unsupported("TO_CHAR with a format that is not a string literal")
    return _format(rewriter, node, written)


def _format(rewriter: _Rewriter, node: exp.Expression, pattern: str) -> bool:
    value = node.this
    kind = rewriter.kind(value)
    parts, found = temporal.format_model(pattern)
    if kind == semi.NUMBER or not found and kind not in semi.TEMPORAL:
        raise errors.unsupported("TO_CHAR with a numeric format")

    def build() -> exp.Expression:
        moment = value
        if kind == TIME:
            # strftime takes no TIME; any date will do to carry one.
            day = exp.cast(exp.Literal.string("1970-01-01"), "DATE")
            moment = exp.Paren(this=exp.Add(this=day, expression=value))
        pieces = [_piece(moment.copy(), part, text) for part, text in parts]
        result = pieces[0] if pieces else exp.Literal.string("")
        for piece in pieces[1:]:
            result = exp.DPipe(this=result, expression=piece)
        return semi.marked(exp.Paren(this=result), semi.TEXT)

    semi.swap(node, build)
    return True


def _piece(moment: exp.Expression, part: str, text: str) -> exp.Expression:
    def strftime(pattern: str) -> exp.Expression:
        return exp.Anonymous(this="STRFTIME", expressions=[moment, exp.Literal.string(pattern)])

    if part == temporal.STRFTIME:
        return strftime(text) if text else exp.Literal.string("")
    if part == temporal.FRACTION:
        return exp.Anonymous(this="LEFT", expressions=[strftime("%f"), exp.Literal.number(text)])
    # The offset as +HHMM; strftime's %z writes +HH or +HH:MM.
    offset = exp.Anonymous(this="REPLACE", expressions=[strftime("%z"), *_strings(":", "")])
    offset = exp.Anonymous(this="RPAD", expressions=[offset, exp.Literal.number(5), *_strings("0")])
    if text == "TZHTZM":
        return offset
    hours = exp.Anonymous(this="LEFT", expressions=[offset, exp.Literal.number(3)])
    minutes = exp.Anonymous(this="RIGHT", expressions=[offset.copy(), exp.Literal.number(2)])
    if text == "TZH":
        return hours
    if text == "TZM":
        return minutes
    return exp.DPipe(
        this=exp.DPipe(this=hours, expression=exp.Literal.string(":")), expression=minutes
    )


def _strings(*texts: str) -> list[exp.Expression]:
    return [exp.Literal.string(text) for text in texts]


def _to_number(rewriter: _Rewriter, node: exp.ToNumber) -> bool:
    if node.args.get("format") is not None:
        raise errors.unsupported("TO_NUMBER with a format")
    return False


def _concat(rewriter: _Rewriter, node: exp.Expression) -> bool:
    for value in [node.args.get("this"), node.args.get("expression"), *node.expressions]:
        if isinstance(value, exp.Expression) and rewriter.kind(value) in STAMPS:
            semi.swap(value, lambda v=value: _text(rewriter, v))
    return False  # a VARIANT operand is still to be converted


def _time(rewriter: _Rewriter, node: exp.Time) -> bool:
    """TIME(x), which sqlglot reads as BigQuery's function of a time zone."""
    value = node.this
    semi.swap(node, lambda: _cast(value, "TIME", TIME))
    return True


def _convert_timezone(rewriter: _Rewriter, node: exp.ConvertTimezone) -> bool:
    """The two-argument form: the wall clock of the target zone.

    A TIMESTAMP_NTZ is taken to be in the session's zone, as its cast to
    TIMESTAMPTZ does. Snowflake's result is a TIMESTAMP_TZ, which carries the
    target's offset as well; here it is a TIMESTAMP_NTZ.
    """
    if node.args.get("source_tz") is not None:
        return False
    value, zone = node.args["timestamp"], node.args["target_tz"]
    semi.swap(
        node,
        lambda: semi.marked(
            exp.Paren(this=exp.AtTimeZone(this=exp.cast(value, "TIMESTAMPTZ"), zone=zone)),
            TIMESTAMP,
        ),
    )
    return True


_RULES: dict[type[exp.Expression], Callable[[_Rewriter, Any], bool]] = {
    exp.DateAdd: _date_add,
    exp.TimeAdd: _date_add,
    exp.TimestampTrunc: _same_kind,
    exp.DateTrunc: _same_kind,
    exp.AddMonths: _same_kind,
    exp.Interval: _interval,
    exp.Add: _arithmetic,
    exp.Sub: _arithmetic,
    exp.Cast: _convert,
    exp.TryCast: _convert,
    exp.Anonymous: _anonymous,
    exp.TsOrDsToDate: _to_date,
    exp.ToChar: _to_char,
    exp.TimeToStr: _time_to_str,
    exp.UnixToTime: _unix_to_time,
    exp.ToNumber: _to_number,
    exp.DPipe: _concat,
    exp.Concat: _concat,
    exp.Time: _time,
    exp.ConvertTimezone: _convert_timezone,
}
