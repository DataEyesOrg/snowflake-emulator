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


@pytest.fixture
def shapes(conn):
    cur = conn.cursor()
    cur.execute("create table shapes (id int, a array, o object, v variant)")
    cur.execute(
        "insert into shapes select 1, array_construct(1, 2), object_construct('k', 1), "
        """parse_json('{"x": [1]}')"""
    )
    return cur


def described(cur, relation: str, kind: str = "table") -> dict[str, str]:
    cur.execute(f"describe {kind} {relation}")
    return {row[0]: row[1] for row in cur}


@pytest.mark.parametrize("result_format", ["ARROW", "JSON"])
def test_array_and_object_columns_are_told_apart(shapes, result_format):
    cur = shapes
    cur.execute(f"alter session set python_connector_query_result_format = '{result_format}'")
    semi = {"A": "ARRAY", "O": "OBJECT", "V": "VARIANT"}
    assert described(cur, "shapes") == {"ID": "NUMBER(38,0)", **semi}
    cur.execute(
        "select column_name, data_type from information_schema.columns "
        "where table_name = 'SHAPES' order by ordinal_position"
    )
    assert dict(cur.fetchall()) == {"ID": "NUMBER", **semi}
    cur.execute("show columns in table shapes")
    assert [json.loads(row[3])["type"] for row in cur] == ["FIXED", "ARRAY", "OBJECT", "VARIANT"]
    # Through a star, a subquery and a set operation, with the rows intact.
    cur.execute("select * from shapes")
    assert type_names(cur) == ["FIXED", "ARRAY", "OBJECT", "VARIANT"]
    assert [json.loads(value) for value in cur.fetchone()[1:]] == [[1, 2], {"k": 1}, {"x": [1]}]
    cur.execute("select x.* from (select o, a from shapes union all select o, a from shapes) x")
    assert type_names(cur) == ["OBJECT", "ARRAY"]
    assert len(cur.fetchall()) == 2


def test_the_kind_of_a_column_follows_it_into_new_tables_and_views(shapes):
    cur = shapes
    cur.execute("create table copied as select * from shapes")
    cur.execute(
        "create table built as select a renamed, object_construct('a', a) made, "
        "array_agg(id) ids, v:x path, a[0] first from shapes group by all"
    )
    cur.execute("create view seen as select o, a from copied")
    cur.execute("create table alike like shapes")
    cur.execute("create table cloned clone shapes")
    semi = {"A": "ARRAY", "O": "OBJECT", "V": "VARIANT"}
    for table in ("copied", "alike", "cloned"):
        assert described(cur, table) == {"ID": "NUMBER(38,0)", **semi}
    assert described(cur, "built") == {
        "RENAMED": "ARRAY",
        "MADE": "OBJECT",
        "IDS": "ARRAY",
        "PATH": "VARIANT",  # an element or a member can be anything
        "FIRST": "VARIANT",
    }
    assert described(cur, "seen", "view") == {"O": "OBJECT", "A": "ARRAY"}


def test_the_kind_of_a_column_survives_alter_table(shapes):
    cur = shapes
    cur.execute("alter table shapes add column extra array")
    cur.execute("alter table shapes rename column a to items")
    cur.execute("alter table shapes drop column o")
    cur.execute("alter table shapes add column o variant")  # the old O was an OBJECT
    expected = {
        "ID": "NUMBER(38,0)",
        "ITEMS": "ARRAY",
        "V": "VARIANT",
        "EXTRA": "ARRAY",
        "O": "VARIANT",
    }
    assert described(cur, "shapes") == expected
    cur.execute("create schema testdb.elsewhere")
    cur.execute("use schema testdb.public")
    cur.execute("alter table shapes rename to elsewhere.moved")
    assert described(cur, "elsewhere.moved") == expected
    cur.execute("create table shapes (v variant)")
    assert described(cur, "shapes") == {"V": "VARIANT"}
    cur.execute("alter table shapes swap with elsewhere.moved")
    assert described(cur, "shapes") == expected
    cur.execute("create or replace table shapes (items variant)")
    assert described(cur, "shapes") == {"ITEMS": "VARIANT"}


def test_a_clone_is_a_copy_of_its_own(shapes):
    cur = shapes
    cur.execute("alter table shapes add primary key (id)")
    cur.execute("comment on column shapes.a is 'the items'")
    cur.execute("create table cloned clone shapes")
    cur.execute("insert into cloned (id) values (2)")
    assert cur.execute("select count(*) from shapes").fetchone() == (1,)
    assert cur.execute("select count(*) from cloned").fetchone() == (2,)
    cur.execute("describe table cloned")
    rows = {row[0]: row for row in cur}
    assert rows["ID"][5] == "Y"  # the primary key
    assert rows["A"][9] == "the items"
    cur.execute("create or replace table cloned clone shapes")
    assert cur.execute("select count(*) from cloned").fetchone() == (1,)
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("create table again clone missing")
    assert failure.value.errno == 2003


def test_columns_are_read_by_position(shapes):
    cur = shapes
    cur.execute("select $1, s.$2, $4:x from shapes s")
    assert [column.name for column in cur.description] == ["$1", "$2", "GET_PATH($4, 'X')"]
    assert type_names(cur) == ["FIXED", "ARRAY", "VARIANT"]
    cur.execute("select * from values (1, 'a')")
    assert [column.name for column in cur.description] == ["COLUMN1", "COLUMN2"]
    with pytest.raises(ProgrammingError) as failure:
        cur.execute("select $9 from shapes")
    assert failure.value.errno == 1003


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
        "select object_construct(*)",
        "create schema copied clone public",
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
