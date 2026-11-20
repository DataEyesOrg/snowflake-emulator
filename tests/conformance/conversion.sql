-- Conformance cases for conversion functions: TRY_CAST, TRY_TO_*, TO_*.
--
-- The format is described in semi_structured.sql. The expected rows follow
-- Snowflake's documentation. They have not been recorded from a real account yet.

create or replace table raw (id int, n varchar, d varchar, b varchar);

insert into raw values (1, '12', '2024-01-15', 'yes'), (2, ' 7.5 ', '2024-02-30', 'off'), (3, 'abc', 'soon', 'maybe'), (4, null, null, null);

select try_cast('12' as int), try_cast('x' as int), try_cast('1.5' as number(10, 2));
--> [[12, null, 1.5]]

select try_cast('2024-02-29' as date), try_cast('2024-02-30' as date), try_cast('nope' as date);
--> [["2024-02-29", null, null]]

select try_cast('true' as boolean), try_cast('maybe' as boolean);
--> [[true, null]]

select try_cast('1.5e2' as float), try_cast('one' as double);
--> [[150.0, null]]

select try_cast('abc' as varchar), try_cast('12:30:00' as time), try_cast('25:00:00' as time);
--> [["abc", "12:30:00", null]]

select try_cast('2024-01-02 03:04:05' as timestamp), try_cast('2024-13-02' as timestamp);
--> [["2024-01-02T03:04:05", null]]

select try_cast('999' as number(2, 0)), try_cast('99' as number(2, 0));
--> [[null, 99]]

select id, try_cast(n as number(10, 1)), try_cast(d as date) from raw order by id;
--> [[1, 12, "2024-01-15"], [2, 7.5, null], [3, null, null], [4, null, null]]

select count(*) from raw where try_cast(n as int) is null;
--> [[2]]

select try_to_number('12'), try_to_number('12.7'), try_to_number('abc'), try_to_number(null);
--> [[12, 13, null, null]]

select try_to_number('12.345', 10, 2), try_to_number('-0.5', 10, 1), try_to_number('1234.5', 4, 1);
--> [[12.35, -0.5, null]]

select try_to_decimal('3.14159', 10, 3), try_to_numeric('42'), try_to_decimal('x', 10, 3);
--> [[3.142, 42, null]]

select try_to_double('1.5'), try_to_double('1e3'), try_to_double('x');
--> [[1.5, 1000.0, null]]

select try_to_boolean('yes'), try_to_boolean('ON'), try_to_boolean('0'), try_to_boolean('f'), try_to_boolean('x');
--> [[true, true, false, false, null]]

select id, try_to_boolean(b) from raw order by id;
--> [[1, true], [2, false], [3, null], [4, null]]

select try_to_date('2024-01-15'), try_to_date('2024-02-30'), try_to_date('bad');
--> [["2024-01-15", null, null]]

select try_to_date('15/01/2024', 'DD/MM/YYYY'), try_to_date('2024-01-15', 'DD/MM/YYYY');
--> [["2024-01-15", null]]

select try_to_timestamp('2024-01-15 10:20:30'), try_to_timestamp('nope');
--> [["2024-01-15T10:20:30", null]]

select try_to_timestamp('15.01.2024 10:20', 'DD.MM.YYYY HH24:MI'), try_to_timestamp_ntz('2024-01-15T10:20:30');
--> [["2024-01-15T10:20:00", "2024-01-15T10:20:30"]]

select try_to_time('10:20:30'), try_to_time('x');
--> [["10:20:30", null]]

select to_number('12.7'), to_number('12.345', 10, 2), to_decimal('1.25', 10, 1), to_numeric('7');
--> [[13, 12.35, 1.3, 7]]

select to_number(12.5), to_number(-12.5), to_number(true::int);
--> [[13, -13, 1]]

select to_double('1.5'), to_double(2), to_double('1e2');
--> [[1.5, 2.0, 100.0]]

select to_boolean('Yes'), to_boolean('n'), to_boolean(1), to_boolean(0), to_boolean('TRUE');
--> [[true, false, true, false, true]]

select to_varchar(12.50), to_char(123), to_varchar(true), to_varchar('x');
--> [["12.50", "123", "true", "x"]]

select '12'::int + 1, '1.5'::float * 2, '2024-01-15'::date, 'true'::boolean;
--> [[13, 3.0, "2024-01-15", true]]

select coalesce(try_to_number(n), 0) from raw order by id;
--> [[12], [8], [0], [0]]

select sum(try_to_number(n, 10, 1)) from raw;
--> [[19.5]]

select nvl(try_to_date(d), '1970-01-01'::date) from raw order by id;
--> [["2024-01-15"], ["1970-01-01"], ["1970-01-01"], ["1970-01-01"]]

select iff(try_to_number('5') > 3, 'big', 'small'), zeroifnull(try_to_number('x')), nullifzero(0);
--> [["big", 0, null]]
