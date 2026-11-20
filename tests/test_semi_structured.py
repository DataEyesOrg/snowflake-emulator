"""VARIANT, OBJECT and ARRAY as a driver sees them.

What the functions return is covered by tests/conformance/semi_structured.sql;
this is about the protocol side: types, formatting, binds and errors.
"""

from __future__ import annotations

import json

import pytest
from snowflake.connector import ProgrammingError
from snowflake.connector.constants import FIELD_ID_TO_NAME

from sfemu import variant

DOCUMENT = '{"name": "ann", "tags": ["a", "b"], "n": 1.50, "none": null, "empty": {}}'


@pytest.fixture
def cur(conn):
    cur = conn.cursor()
    cur.execute("create table events (id int, v variant)")
    cur.execute("insert into events select 1, parse_json(%s)", (DOCUMENT,))
    return cur


def type_names(cur) -> list[str]:
    return [FIELD_ID_TO_NAME[column.type_code] for column in cur.description]


@pytest.mark.parametrize("result_format", ["ARROW", "JSON"])
def test_values_are_printed_like_snowflake_prints_them(cur, result_format):
    cur.execute(f"alter session set python_connector_query_result_format = '{result_format}'")
    cur.execute("select v, v:tags, v:name, v:n, v:none, v:missing from events")
    document, tags, name, number, json_null, missing = cur.fetchone()
    # Indented by two spaces, keys sorted, no zeros ending a fraction.
    assert document == (
        '{\n  "empty": {},\n  "n": 1.5,\n  "name": "ann",\n  "none": null,\n'
        '  "tags": [\n    "a",\n    "b"\n  ]\n}'
    )
    assert tags == '[\n  "a",\n  "b"\n]'
    assert (name, number) == ('"ann"', "1.5")
    assert (json_null, missing) == ("null", None)  # JSON null is not SQL NULL


def test_result_columns_are_typed(cur):
    cur.execute(
        "select v, v:tags, array_construct(1), object_construct('a', 1), "
        "v:name::string, array_size(v:tags) from events"
    )
    assert type_names(cur) == ["VARIANT", "VARIANT", "ARRAY", "OBJECT", "TEXT", "FIXED"]
    cur.execute("describe table events")
    assert [row[1] for row in cur] == ["NUMBER(38,0)", "VARIANT"]


def test_documents_arrive_through_binds_and_leave_through_result_scan(cur):
    for row in [(2, '{"name": "bob"}'), (3, "[1, 2, 3]")]:
        cur.execute("insert into events select %s, parse_json(%s)", row)
    cur.execute("select id, v from events order by id")
    cur.execute("select id, v:name::string from table(result_scan(last_query_id())) where id < 3")
    assert cur.fetchall() == [(1, "ann"), (2, "bob")]
    cur.execute("select sum(f.value) from events, lateral flatten(input => v) f where id = 3")
    assert cur.fetchall() == [(6.0,)]


def test_text_that_is_not_json_is_an_error(cur):
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select parse_json('{\"a\": ')")
    assert failure.value.errno == 100069
    assert failure.value.sqlstate == "22P02"
    assert "Error parsing JSON" in failure.value.msg
    assert cur.execute("select try_parse_json('{\"a\": ')").fetchall() == [(None,)]


def test_unsupported_forms_say_so(cur):
    for sql in (
        "select object_construct(*) from events",
        "select * from table(flatten(input => parse_json('[1]'), mode => 'ARRAY'))",
    ):
        with pytest.raises(ProgrammingError) as failure:
            cur.execute(sql)
        assert failure.value.errno == 2


def test_canonical_form():
    assert variant.canonical(' {"b": [1, 2.50, 3.0, 1e3], "a": {"d": null, "c": "é"}} ') == (
        '{"a":{"c":"é","d":null},"b":[1,2.5,3,1e3]}'
    )
    assert variant.canonical("12345678901234567890.123456789") == "12345678901234567890.123456789"
    assert json.loads(variant.pretty('{"a":[{"b":[]}]}')) == {"a": [{"b": []}]}
    assert variant.pretty("[]") == "[]"
    assert variant.pretty("not json") == "not json"
    for bad in ("{", "NaN", "[1,]", ""):
        with pytest.raises(ValueError):
            variant.canonical(bad)
