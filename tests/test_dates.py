"""Dates, times and timestamps beyond what a query returns.

The values of the functions are covered by tests/conformance/dates.sql and
conversion.sql; this is about types, the session's time zone, text formats
and what is refused.
"""

from __future__ import annotations

import datetime as dt

import pytest
from snowflake.connector import ProgrammingError
from snowflake.connector.constants import FIELD_ID_TO_NAME

from sfemu import temporal


@pytest.fixture
def cur(conn):
    cur = conn.cursor()
    cur.execute(
        "create table e (d date, ts timestamp_ntz, tz timestamp_ltz, t time, s varchar, "
        "n number, v variant)"
    )
    cur.execute(
        "insert into e select '2024-01-31', '2024-01-31 23:59:59.5', '2024-01-31 12:00:00', "
        "'10:20:30', '2024-03-05 01:02:03', 1705314030, "
        """parse_json('{"at": "2024-05-06 07:08:09", "epoch": 1705314030}')"""
    )
    return cur


def type_names(cur) -> list[str]:
    return [FIELD_ID_TO_NAME[column.type_code] for column in cur.description]


def test_a_date_moved_by_days_stays_a_date(cur):
    cur.execute(
        "select dateadd(month, 1, d), date_trunc('month', d), d + interval '1 day', "
        "add_months(d, 1), d + 1, dateadd(hour, 1, d), d + interval '2 hours', "
        "dateadd(day, 1, ts), date_trunc('day', tz), dateadd(hour, 1, t) from e"
    )
    assert type_names(cur) == [
        *["DATE"] * 5,
        *["TIMESTAMP_NTZ"] * 3,
        "TIMESTAMP_LTZ",
        "TIME",
    ]
    assert cur.fetchone()[:6] == (
        dt.date(2024, 2, 29),
        dt.date(2024, 1, 1),
        dt.date(2024, 2, 1),
        dt.date(2024, 2, 29),
        dt.date(2024, 2, 1),
        dt.datetime(2024, 1, 31, 1),
    )


def test_the_kind_of_a_date_is_followed_through_tables_views_and_ctes(cur):
    cur.execute(
        "create table moved as select dateadd(day, 1, d) nxt, dateadd(hour, 1, d) soon from e"
    )
    cur.execute("create view seen as select date_trunc('month', nxt) month_start from moved")
    cur.execute("describe table moved")
    assert [row[1] for row in cur] == ["DATE", "TIMESTAMP_NTZ(9)"]
    cur.execute("describe view seen")
    assert [row[1] for row in cur] == ["DATE"]
    cur.execute(
        "with c as (select month_start, max(nxt) latest from seen, moved group by 1) "
        "select dateadd(year, 1, month_start), dateadd(week, 1, x.latest) from c x"
    )
    assert type_names(cur) == ["DATE", "DATE"]
    assert cur.fetchone() == (dt.date(2025, 2, 1), dt.date(2024, 2, 8))


def test_timestamps_become_text_in_snowflakes_default_formats(cur):
    cur.execute(
        "select ts::varchar, to_char(ts), 'at ' || ts, tz::varchar, to_varchar(tz), "
        "d::varchar, t::varchar from e"
    )
    assert cur.fetchone() == (
        "2024-01-31 23:59:59.500",
        "2024-01-31 23:59:59.500",
        "at 2024-01-31 23:59:59.500",
        "2024-01-31 12:00:00.000 +0000",
        "2024-01-31 12:00:00.000 +0000",
        "2024-01-31",
        "10:20:30",
    )


def test_the_session_time_zone_decides_how_an_instant_reads(cur):
    cur.execute("alter session set timezone = 'America/New_York'")
    cur.execute(
        "select tz::varchar, to_char(tz, 'YYYY-MM-DD HH24:MI TZH:TZM'), tz::timestamp_ntz, "
        "convert_timezone('UTC', tz), convert_timezone('Europe/Oslo', ts), "
        "to_timestamp_ltz('2024-07-01 12:00:00 +0200'), '2024-07-01 12:00:00'::timestamp_ltz "
        "from e"
    )
    text, formatted, wall_clock, in_utc, in_oslo, parsed, local = cur.fetchone()
    assert (text, formatted) == ("2024-01-31 07:00:00.000 -0500", "2024-01-31 07:00 -05:00")
    assert wall_clock == dt.datetime(2024, 1, 31, 7)
    assert in_utc == dt.datetime(2024, 1, 31, 12)
    # A TIMESTAMP_NTZ is read as the session's wall clock: 23:59 in New York.
    assert in_oslo == dt.datetime(2024, 2, 1, 5, 59, 59, 500000)
    assert parsed.astimezone(dt.UTC) == dt.datetime(2024, 7, 1, 10, tzinfo=dt.UTC)
    assert local.astimezone(dt.UTC) == dt.datetime(2024, 7, 1, 16, tzinfo=dt.UTC)


def test_to_timestamp_goes_by_the_type_of_its_argument(cur):
    cur.execute(
        "select to_timestamp(s), to_timestamp(n), to_timestamp(ts), to_timestamp(d), "
        "to_timestamp(v:at), to_timestamp(v:epoch), to_timestamp(n::varchar), "
        "try_to_timestamp('junk' || s), to_date(n::varchar || ''), s::timestamp, n::timestamp "
        "from e"
    )
    # A column is named after the call as it was written, not after its rewrite.
    assert cur.description[0].name == "TO_TIMESTAMP(S)"
    assert type_names(cur)[:8] == ["TIMESTAMP_NTZ"] * 8
    epoch = dt.datetime(2024, 1, 15, 10, 20, 30)
    assert cur.fetchone() == (
        dt.datetime(2024, 3, 5, 1, 2, 3),
        epoch,
        dt.datetime(2024, 1, 31, 23, 59, 59, 500000),
        dt.datetime(2024, 1, 31),
        dt.datetime(2024, 5, 6, 7, 8, 9),
        epoch,
        epoch,
        None,
        dt.date(2024, 1, 15),
        dt.datetime(2024, 3, 5, 1, 2, 3),
        epoch,
    )


def test_text_and_variants_are_read_as_dates_where_a_date_is_wanted(cur):
    cur.execute(
        "select datediff(day, d, s), dateadd(day, 1, s), year(s), last_day(s), "
        "datediff(day, v:at, s), dateadd(day, 1, v:at), date_trunc('month', '2024-01-15'), "
        "dateadd(hour, 1, '10:00:00') from e"
    )
    assert cur.fetchone() == (
        34,
        dt.datetime(2024, 3, 6, 1, 2, 3),
        2024,
        dt.date(2024, 3, 31),
        -62,
        dt.datetime(2024, 5, 7, 7, 8, 9),
        dt.datetime(2024, 1, 1),
        dt.time(11),
    )


def test_dates_arrive_through_binds(cur):
    cur.execute(
        "select dateadd(day, %s, d), datediff(day, d, %s), to_char(%s::date, 'DD.MM.YYYY') from e",
        (2, dt.date(2024, 2, 10), dt.date(2024, 2, 10)),
    )
    assert cur.fetchone() == (dt.date(2024, 2, 2), 10, "10.02.2024")


def test_a_format_may_be_literals_joined_together(cur):
    # dbt's catalog query writes its format this way.
    cur.execute(
        "select to_varchar(convert_timezone('UTC', tz), 'yyyy-mm-dd HH24:MI' || 'UTC') from e"
    )
    assert cur.fetchone() == ("2024-01-31 12:00UTC",)


@pytest.mark.parametrize(
    ("sql", "feature"),
    [
        ("select to_char(n, '999.00') from e", "TO_CHAR with a numeric format"),
        ("select to_char(d, s) from e", "TO_CHAR with a format that is not a string literal"),
        ("select to_number('1,234', '9,999')", "TO_NUMBER with a format"),
        ("select try_to_number(s, '999') from e", "TO_NUMBER with a format"),
    ],
)
def test_formats_that_are_not_modelled_are_refused(cur, sql, feature):
    with pytest.raises(ProgrammingError) as caught:
        cur.execute(sql)
    assert caught.value.errno == 2
    assert feature in caught.value.msg


def test_a_conversion_that_fails_is_an_error_and_its_try_form_is_null(cur):
    for sql in ("select to_timestamp('junk' || s) from e", "select ('x' || s)::timestamp from e"):
        with pytest.raises(ProgrammingError) as caught:
            cur.execute(sql)
        assert caught.value.errno == 100038
    cur.execute(
        "select try_to_timestamp('x' || s), try_to_date('x' || s), try_to_number(s), "
        "try_cast('x' || s as timestamp), try_to_timestamp_ltz('x' || s) from e"
    )
    assert cur.fetchone() == (None,) * 5


@pytest.mark.parametrize(
    ("pattern", "parts"),
    [
        ("YYYY-MM-DD", [(temporal.STRFTIME, "%Y-%m-%d")]),
        ("yyyy-mm-dd hh24:mi:ss.ff3", [(temporal.STRFTIME, "%Y-%m-%d %H:%M:%S.%g")]),
        ('DD "of" MMMM, HH12 AM', [(temporal.STRFTIME, "%d of %B, %I %p")]),
        ("FF", [(temporal.STRFTIME, "%f000")]),
        ("SS.FF2", [(temporal.STRFTIME, "%S."), (temporal.FRACTION, "2")]),
        (
            "HH24 TZH:TZM 100%",
            [(temporal.STRFTIME, "%H "), (temporal.ZONE, "TZH:TZM"), (temporal.STRFTIME, " 100%%")],
        ),
    ],
)
def test_format_models_become_strftime_formats(pattern, parts):
    assert temporal.format_model(pattern) == (parts, True)


def test_a_format_without_elements_is_told_apart():
    assert temporal.format_model("999.00") == ([(temporal.STRFTIME, "999.00")], False)
