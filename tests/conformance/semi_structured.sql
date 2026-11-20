-- Conformance cases for semi-structured data: VARIANT, OBJECT, ARRAY, FLATTEN.
--
-- A block is one statement. With a "-->" line it is a query and the line holds
-- the rows it must return, as JSON; without one it is setup, and all setup runs
-- before any query. VARIANT, OBJECT and ARRAY cells are compared as parsed
-- JSON, numbers as numbers, dates and timestamps as ISO text.
--
-- The expected rows follow Snowflake's documentation. They have not been
-- recorded from a real account yet.

create or replace table ev (id int, v variant);

insert into ev select 1, parse_json('{"name": "ann", "tags": ["a", "b"], "addr": {"city": "Oslo", "zip": null}, "n": 1.5}');

insert into ev select 2, parse_json('{"name": "bob", "tags": [], "addr": {"city": "Rome"}, "n": 2}');

insert into ev select 3, null;

select parse_json('{"b": 1, "a": [1, 2, {"c": null}]}');
--> [[{"a": [1, 2, {"c": null}], "b": 1}]]

select parse_json('1'), parse_json('"x"'), parse_json('null'), parse_json(null);
--> [[1, "x", null, null]]

select object_construct('a', 1, 'b', 'x', 'c', null);
--> [[{"a": 1, "b": "x"}]]

select object_construct_keep_null('a', 1, 'c', null);
--> [[{"a": 1, "c": null}]]

select object_construct();
--> [[{}]]

select array_construct(1, 'a', null, 2.5);
--> [[[1, "a", null, 2.5]]]

select array_construct();
--> [[[]]]

select [1, 2, 3], {'a': 1, 'b': [true, null]};
--> [[[1, 2, 3], {"a": 1, "b": [true, null]}]]

select to_variant(1), to_variant('a'), to_variant(true), to_variant(1.5);
--> [[1, "a", true, 1.5]]

select to_json(parse_json('{"b": 1, "a": [1, 2]}'));
--> [["{\"a\":[1,2],\"b\":1}"]]

select to_json(object_construct('a', 'x'));
--> [["{\"a\":\"x\"}"]]

select to_array(1), to_array(parse_json('[1,2]')), to_array(null);
--> [[[1], [1, 2], null]]

select array_construct_compact(1, null, 2);
--> [[[1, 2]]]

select v:name, v:name::string, v:n, v:n::float from ev where id = 1;
--> [["ann", "ann", 1.5, 1.5]]

select v:addr.city::string, v:addr:city::varchar, v['addr']['city']::string from ev where id = 1;
--> [["Oslo", "Oslo", "Oslo"]]

select v:tags[0]::string, v:tags[5], v:missing, v:addr.zip from ev where id = 1;
--> [["a", null, null, null]]

select id, v:name::string from ev order by id;
--> [[1, "ann"], [2, "bob"], [3, null]]

select get(v, 'name')::string, get_path(v, 'addr.city')::string, get(v:tags, 1)::string from ev where id = 1;
--> [["ann", "Oslo", "b"]]

select v:n::int, v:n::number(10,2), v:n::string from ev where id = 1;
--> [[2, 1.5, "1.5"]]

select v:name::string = 'ann', v:n > 1, v:n + 1 from ev where id = 1;
--> [[true, true, 2.5]]

select count(*) from ev where v:addr.city = 'Rome';
--> [[1]]

select v:"name"::string, v:NAME from ev where id = 1;
--> [["ann", null]]

select parse_json('{"a": {"b": [10, 20]}}'):a.b[1]::int;
--> [[20]]

select v:addr.zip is null, is_null_value(v:addr.zip), is_null_value(v:missing) from ev where id = 1;
--> [[false, true, null]]

select f.value::string from ev, lateral flatten(input => v:tags) f where id = 1 order by f.index;
--> [["a"], ["b"]]

select id, f.index, f.value::string from ev, lateral flatten(input => ev.v:tags) f order by 1, 2;
--> [[1, 0, "a"], [1, 1, "b"]]

select f.key, f.value::string from ev, lateral flatten(input => v:addr) f where id = 2;
--> [["city", "Rome"]]

select value from table(flatten(input => parse_json('[1, 2, 3]'))) order by index;
--> [[1], [2], [3]]

select seq, key, path, index, value, this from table(flatten(input => parse_json('[7]')));
--> [[1, null, "[0]", 0, 7, [7]]]

select key, path, value from table(flatten(input => parse_json('{"a": {"b": 1}}'), recursive => true)) order by path;
--> [["a", "a", {"b": 1}], ["b", "a.b", 1]]

select value::int from table(flatten(parse_json('[3, 4]'))) order by 1;
--> [[3], [4]]

select f.value:x::int from table(flatten(input => parse_json('[{"x": 1}, {"x": 2}]'))) f order by 1;
--> [[1], [2]]

select count(*) from ev, lateral flatten(input => v, path => 'tags');
--> [[2]]

select t.id, sum(f.value::int) from (select 1 id, array_construct(1,2,3) a) t, lateral flatten(t.a) f group by 1;
--> [[1, 6]]

select array_size(array_construct(1, 2, 3)), array_size(parse_json('[1]')), array_size(v:tags) from ev where id = 1;
--> [[3, 1, 2]]

select array_contains('a'::variant, array_construct('a', 'b')), array_contains(1::variant, array_construct(2));
--> [[true, false]]

select array_to_string(array_construct('a', 'b', 'c'), '-'), array_to_string(v:tags, ',') from ev where id = 1;
--> [["a-b-c", "a,b"]]

select array_slice(array_construct(0,1,2,3,4), 1, 3), array_cat(array_construct(1), array_construct(2));
--> [[[1, 2], [1, 2]]]

select array_append(array_construct(1), 2), array_prepend(array_construct(1), 0), array_insert(array_construct(0, 2), 1, 1);
--> [[[1, 2], [0, 1], [0, 1, 2]]]

select array_compact(array_construct(1, null, 2)), array_distinct(array_construct(1, 1, 2));
--> [[[1, 2], [1, 2]]]

select array_position(2::variant, array_construct(1, 2)), array_remove(array_construct(1, 2, 1), 1::variant);
--> [[1, [2]]]

select array_intersection(array_construct(1,2,3), array_construct(2,3,4)), arrays_overlap(array_construct(1), array_construct(1, 2));
--> [[[2, 3], true]]

select array_flatten(array_construct(array_construct(1), array_construct(2, 3))), array_sort(array_construct(3, 1, 2));
--> [[[1, 2, 3], [1, 2, 3]]]

select array_agg(id) within group (order by id desc) from ev;
--> [[[3, 2, 1]]]

select array_agg(v:name) within group (order by id) from ev;
--> [[["ann", "bob"]]]

select array_agg(distinct x) within group (order by x) from (select 1 x union all select 1 union all select 2);
--> [[[1, 2]]]

select array_max(array_construct(1, 3, 2)), array_min(array_construct(1, 3, 2));
--> [[3, 1]]

select array_generate_range(0, 3), array_reverse(array_construct(1, 2));
--> [[[0, 1, 2], [2, 1]]]

select strtok_to_array('a,b,c', ','), split('a-b', '-');
--> [[["a", "b", "c"], ["a", "b"]]]

select object_keys(parse_json('{"b": 1, "a": 2}')), object_keys(v:addr) from ev where id = 2;
--> [[["a", "b"], ["city"]]]

select object_insert(object_construct('a', 1), 'b', 2), object_insert(object_construct('a', 1), 'a', 9, true);
--> [[{"a": 1, "b": 2}, {"a": 9}]]

select object_delete(object_construct('a', 1, 'b', 2), 'a'), object_pick(object_construct('a', 1, 'b', 2), 'b');
--> [[{"b": 2}, {"b": 2}]]

select object_agg(k, v::variant) from (select 'a' k, 1 v union all select 'b', 2);
--> [[{"a": 1, "b": 2}]]

select object_construct('k', v:tags, 'n', id) from ev where id = 1;
--> [[{"k": ["a", "b"], "n": 1}]]

select typeof(v), typeof(v:name), typeof(v:tags), typeof(v:n), typeof(v:addr.zip), typeof(v:missing) from ev where id = 1;
--> [["OBJECT", "VARCHAR", "ARRAY", "DECIMAL", "NULL_VALUE", null]]

select is_array(v:tags), is_object(v), is_varchar(v:name), is_integer(v:n), is_decimal(v:n), is_boolean(v:n) from ev where id = 1;
--> [[true, true, true, false, true, false]]

select as_varchar(v:name), as_integer(v:n), as_double(v:n), as_array(v:tags), as_object(v:name) from ev where id = 2;
--> [["bob", 2, 2.0, [], null]]

select strip_null_value(v:addr.zip), strip_null_value(v:addr.city) from ev where id = 1;
--> [[null, "Oslo"]]

select check_json('{"a": 1}'), check_json('{bad') is not null, try_parse_json('{bad'), try_parse_json('[1]');
--> [[null, true, null, [1]]]

select v from ev where id = 3;
--> [[null]]

select 1::variant, 'a'::variant, array_construct(1)::variant, parse_json('[1]')::array, parse_json('{"a":1}')::object;
--> [[1, "a", [1], [1], {"a": 1}]]

select v:tags::array, v:addr::object, v::string from ev where id = 2;
--> [[[], {"city": "Rome"}, "{\"addr\":{\"city\":\"Rome\"},\"n\":2,\"name\":\"bob\",\"tags\":[]}"]]

select parse_json('{"d": "2024-02-29", "t": "2024-02-29 10:00:00", "b": true}'):d::date, parse_json('{"t": "2024-02-29 10:00:00"}'):t::timestamp_ntz, parse_json('{"b": true}'):b::boolean;
--> [["2024-02-29", "2024-02-29T10:00:00", true]]

select v:name::string as n, count(*) from ev group by 1 order by 1 nulls last;
--> [["ann", 1], ["bob", 1], [null, 1]]

select id from ev order by v:n::float desc nulls last;
--> [[2], [1], [3]]

select a.id from ev a join ev b on a.v:name = b.v:name order by 1;
--> [[1], [2]]


-- Setup for the cases below: typed columns, a view and a copy.
create or replace table sv (a array, o object, v variant, label varchar);

insert into sv select array_construct(1, 2), object_construct('k', 'v'), to_variant('s'), 'first';

update sv set v = parse_json('{"x": [1]}') where o:k = 'v';

create or replace view ev_names as select id, v:name as name, v:tags as tags from ev;

create or replace table ev_copy as select id, v:addr as addr, v:n::float as n from ev;

select a, o, a[0], o:k::string, v:x[0]::int, label from sv;
--> [[[1, 2], {"k": "v"}, 1, "v", 1, "first"]]

-- JSON null is a value, a missing key is not.
select is_object(v:tags), is_array(v), is_char(v:name), is_real(v:n), is_double(v:n) from ev where id = 1;
--> [[false, false, true, true, true]]

select typeof(parse_json('1')), typeof(parse_json('true')), typeof(parse_json('1.5e0')), typeof(parse_json('1.5'));
--> [["INTEGER", "BOOLEAN", "DOUBLE", "DECIMAL"]]

select id, v:addr.zip is null, v:addr.zip::string is null, v:missing is null from ev where id < 3 order by id;
--> [[1, false, true, true], [2, true, true, true]]

select id, f.value from ev, lateral flatten(input => v:tags, outer => true) f order by id, f.index;
--> [[1, "a"], [1, "b"], [2, null], [3, null]]

select id, f.value::string from ev left join lateral flatten(input => v:tags) f order by id, f.index;
--> [[1, "a"], [1, "b"], [2, null], [3, null]]

select id, f.value::string from ev, flatten(v:tags) f order by 1, 2;
--> [[1, "a"], [1, "b"]]

select e.id, t.value::string, a.key from ev e, lateral flatten(input => e.v:tags) t, lateral flatten(input => e.v:addr) a where a.key = 'city' order by 1, 2;
--> [[1, "a", "city"], [1, "b", "city"]]

-- The kind of a column follows it through views, CTEs, subqueries and stars.
select name::string, name = 'ann', upper(name), tags[0]::string from ev_names where id = 1;
--> [["ann", true, "ANN", "a"]]

with named as (select id, v:name as name, v:n as n from ev) select id from named where name = 'bob' and n >= 2;
--> [[2]]

select count(*) from (select * from ev) e where e.v:name like 'a%';
--> [[1]]

select s.n + 1, s.name || '!' from (select v:n as n, v:name as name from ev where id = 1) s;
--> [[2.5, "ann!"]]

select addr:city::string, n from ev_copy where addr:city in ('Oslo', 'Paris');
--> [["Oslo", 1.5]]

select count(*) from ev where v:n between 1 and 1.9;
--> [[1]]

select sum(v:n), max(v:n::float), listagg(v:name, ',') within group (order by id) from ev;
--> [[3.5, 2.0, "ann,bob"]]

select coalesce(v:nickname, v:name)::string, iff(v:n > 1.7, 'big', 'small') from ev where id < 3 order by id;
--> [["ann", "small"], ["bob", "big"]]

select id from ev where id < 3 order by v:name desc;
--> [[2], [1]]

select v:n from (select parse_json('{"n": 10}') v union all select parse_json('{"n": 9}') union all select parse_json('{"n": 100}')) order by v:n;
--> [[9], [10], [100]]

select array_agg(f.value::int) within group (order by f.value::int desc) from table(flatten(input => parse_json('[2, 10, 9]'))) f;
--> [[[10, 9, 2]]]

select to_json(parse_json('{"b": {"d": 1, "c": 2}, "a": 1.50}')), parse_json('{"k": "é"}')::string;
--> [["{\"a\":1.5,\"b\":{\"c\":2,\"d\":1}}", "{\"k\":\"é\"}"]]

select parse_json('{"b": 1, "a": 2}') = parse_json('{"a": 2, "b": 1}'), count(distinct v) from (select parse_json('{"b": 1, "a": 2}') v union all select parse_json('{"a": 2,   "b": 1}'));
--> [[true, 1]]

select object_construct('id', id, 'name', v:name, 'first_tag', v:tags[0]) from ev where id < 3 order by id;
--> [[{"first_tag": "a", "id": 1, "name": "ann"}], [{"id": 2, "name": "bob"}]]

select array_construct(1, 'two', array_construct(3), object_construct('four', 4), null, true, 5.5);
--> [[[1, "two", [3], {"four": 4}, null, true, 5.5]]]

select json_extract_path_text(v, 'addr.city'), get_path(v, 'tags[1]')::string from ev where id = 1;
--> [["Oslo", "b"]]
