-- Conformance cases for dates, times and timestamps.
--
-- The format is described in semi_structured.sql. The session time zone is
-- UTC. The expected rows follow Snowflake's documentation. They have not been
-- recorded from a real account yet.

create or replace table t (id int, d date, ts timestamp_ntz);

insert into t values (1, '2024-01-31', '2024-01-31 23:59:59'), (2, '2024-02-29', '2024-02-29 12:00:00'), (3, '2023-12-31', '2023-12-31 00:00:00.123'), (4, null, null);

select dateadd(day, 5, '2024-02-27'::date), dateadd(month, 1, '2024-01-31'::date), dateadd(year, -1, '2024-02-29'::date);
--> [["2024-03-03", "2024-02-29", "2023-02-28"]]

select dateadd(week, 2, '2024-01-01'::date), dateadd(quarter, 1, '2024-01-15'::date);
--> [["2024-01-15", "2024-04-15"]]

select dateadd(hour, 3, '2024-01-01'::date), dateadd(minute, 90, '2024-01-01 10:00:00'::timestamp);
--> [["2024-01-01T03:00:00", "2024-01-01T11:30:00"]]

select dateadd('dd', 1, '2024-01-01'::date), dateadd(mm, 1, '2024-01-01'::date), dateadd(yy, 1, '2024-01-01'::date), dateadd(days, 1, '2024-01-01'::date);
--> [["2024-01-02", "2024-02-01", "2025-01-01", "2024-01-02"]]

select dateadd(second, 30, '2024-01-01 00:00:00'::timestamp), dateadd(ms, 1500, '2024-01-01 00:00:00'::timestamp);
--> [["2024-01-01T00:00:30", "2024-01-01T00:00:01.500000"]]

select dateadd(hour, 2, '10:00:00'::time), timestampadd(day, 1, '2024-01-01'::date), timeadd(hour, 1, '2024-01-01 00:00:00'::timestamp);
--> [["12:00:00", "2024-01-02", "2024-01-01T01:00:00"]]

select id, dateadd(month, 1, d), dateadd(day, 1, ts) from t order by id;
--> [[1, "2024-02-29", "2024-02-01T23:59:59"], [2, "2024-03-29", "2024-03-01T12:00:00"], [3, "2024-01-31", "2024-01-01T00:00:00.123000"], [4, null, null]]

select datediff(day, '2024-01-01'::date, '2024-03-01'::date), datediff(day, '2024-03-01'::date, '2024-01-01'::date);
--> [[60, -60]]

select datediff(month, '2024-01-31'::date, '2024-02-01'::date), datediff(year, '2023-12-31'::date, '2024-01-01'::date), datediff(quarter, '2024-03-31'::date, '2024-04-01'::date);
--> [[1, 1, 1]]

select datediff(hour, '2024-01-01 10:59:00'::timestamp, '2024-01-01 11:01:00'::timestamp), datediff(minute, '2024-01-01 10:00:59'::timestamp, '2024-01-01 10:01:00'::timestamp);
--> [[1, 1]]

select datediff(week, '2024-01-07'::date, '2024-01-08'::date), datediff(week, '2024-01-08'::date, '2024-01-14'::date);
--> [[1, 0]]

select datediff(day, '2024-01-01', '2024-01-10'), datediff('day', '2024-01-01 23:00:00'::timestamp, '2024-01-02 01:00:00'::timestamp);
--> [[9, 1]]

select datediff(second, '2024-01-01 00:00:00'::timestamp, '2024-01-02 00:00:00'::timestamp), timestampdiff(hour, '2024-01-01'::date, '2024-01-02'::date), timediff(minute, '10:00:00'::time, '11:30:00'::time);
--> [[86400, 24, 90]]

select id, datediff(day, d, '2024-03-01'::date), datediff(month, ts, d) from t order by id;
--> [[1, 30, 0], [2, 1, 0], [3, 61, 0], [4, null, null]]

select date_trunc('month', '2024-02-17'::date), date_trunc('year', '2024-02-17'::date), date_trunc('quarter', '2024-05-17'::date), date_trunc('week', '2024-01-10'::date);
--> [["2024-02-01", "2024-01-01", "2024-04-01", "2024-01-08"]]

select date_trunc('hour', '2024-02-17 10:45:30'::timestamp), date_trunc(day, '2024-02-17 10:45:30'::timestamp), date_trunc('minute', '2024-02-17 10:45:30'::timestamp);
--> [["2024-02-17T10:00:00", "2024-02-17T00:00:00", "2024-02-17T10:45:00"]]

select date_trunc('month', d), count(*) from t where d is not null group by 1 order by 1;
--> [["2023-12-01", 1], ["2024-01-01", 1], ["2024-02-01", 1]]

select trunc('2024-02-17'::date, 'month'), date_trunc('hour', '10:45:30'::time);
--> [["2024-02-01", "10:00:00"]]

select last_day('2024-02-10'::date), last_day('2024-02-10'::date, 'year'), last_day('2024-02-10'::date, 'quarter'), last_day('2024-01-10'::date, 'week');
--> [["2024-02-29", "2024-12-31", "2024-03-31", "2024-01-14"]]

select year('2024-03-15'::date), month('2024-03-15'::date), day('2024-03-15'::date), dayofmonth('2024-03-15'::date), quarter('2024-03-15'::date);
--> [[2024, 3, 15, 15, 1]]

select hour('2024-03-15 10:20:30'::timestamp), minute('2024-03-15 10:20:30'::timestamp), second('2024-03-15 10:20:30'::timestamp), hour('10:20:30'::time);
--> [[10, 20, 30, 10]]

select dayofweek('2024-03-17'::date), dayofweek('2024-03-15'::date), dayofweekiso('2024-03-17'::date), dayofweekiso('2024-03-15'::date), dayofyear('2024-03-15'::date);
--> [[0, 5, 7, 5, 75]]

select week('2024-01-01'::date), weekofyear('2024-12-30'::date), weekiso('2021-01-03'::date), yearofweek('2024-12-30'::date), yearofweekiso('2021-01-03'::date);
--> [[1, 1, 53, 2025, 2020]]

select extract(year from '2024-03-15'::date), extract(month from '2024-03-15 10:20:30'::timestamp), extract(epoch_second from '2024-01-01 00:00:00'::timestamp), extract(dayofweek from '2024-03-15'::date);
--> [[2024, 3, 1704067200, 5]]

select date_part(quarter, '2024-08-15'::date), date_part('week', '2024-01-08'::date), date_part(epoch_millisecond, '2024-01-01 00:00:00.250'::timestamp), date_part(doy, '2024-12-31'::date);
--> [[3, 2, 1704067200250, 366]]

select dayname('2024-01-10'::date), monthname('2024-01-10'::date), dayname('2024-01-14 10:00:00'::timestamp), monthname('2024-12-01'::date);
--> [["Wed", "Jan", "Sun", "Dec"]]

select add_months('2024-01-31'::date, 1), add_months('2024-02-29'::date, 1), add_months('2024-01-15'::date, -2), add_months('2024-02-28'::date, 1);
--> [["2024-02-29", "2024-03-31", "2023-11-15", "2024-03-28"]]

select add_months('2024-01-31 10:00:00'::timestamp, 1);
--> [["2024-02-29T10:00:00"]]

select months_between('2024-03-15'::date, '2024-01-15'::date), months_between('2024-02-29'::date, '2024-01-31'::date), months_between('2024-02-15'::date, '2024-01-01'::date), months_between('2024-01-01'::date, '2024-02-15'::date);
--> [[2, 1, 1.4516129032258065, -1.4516129032258065]]

select next_day('2024-01-10'::date, 'fr'), next_day('2024-01-10'::date, 'wednesday'), previous_day('2024-01-10'::date, 'mo'), previous_day('2024-01-10'::date, 'we');
--> [["2024-01-12", "2024-01-17", "2024-01-08", "2024-01-03"]]

select date_from_parts(2024, 3, 15), date_from_parts(2024, 14, 1), date_from_parts(2024, 1, 0), date_from_parts(2024, 2, 30);
--> [["2024-03-15", "2025-02-01", "2023-12-31", "2024-03-01"]]

select time_from_parts(12, 34, 56), timestamp_from_parts(2024, 1, 2, 3, 4, 5), timestamp_from_parts('2024-01-02'::date, '03:04:05'::time), timestamp_ntz_from_parts(2024, 1, 2, 3, 4, 5, 500000000);
--> [["12:34:56", "2024-01-02T03:04:05", "2024-01-02T03:04:05", "2024-01-02T03:04:05.500000"]]

select to_date('2024-01-15'), to_date('15.01.2024', 'DD.MM.YYYY'), to_date('20240115', 'YYYYMMDD'), date('2024-01-15');
--> [["2024-01-15", "2024-01-15", "2024-01-15", "2024-01-15"]]

select to_date('Jan 15, 2024', 'MON DD, YYYY'), to_date('15-January-2024', 'DD-MMMM-YYYY'), to_date('01/15/24', 'MM/DD/YY'), to_date('2024-01-15 10:20:30');
--> [["2024-01-15", "2024-01-15", "2024-01-15", "2024-01-15"]]

select to_date('2024-01-15 10:20:30'::timestamp), '2024-01-15 10:20:30'::timestamp::date, to_date('2024-1-5', 'YYYY-MM-DD');
--> [["2024-01-15", "2024-01-15", "2024-01-05"]]

select to_timestamp('2024-01-15 10:20:30'), to_timestamp('2024-01-15T10:20:30.123'), to_timestamp('2024-01-15'), to_timestamp('15/01/2024 10:20', 'DD/MM/YYYY HH24:MI');
--> [["2024-01-15T10:20:30", "2024-01-15T10:20:30.123000", "2024-01-15T00:00:00", "2024-01-15T10:20:00"]]

select to_timestamp(1705314030), to_timestamp(1705314030123, 3), to_timestamp('1705314030'), to_timestamp('1705314030123');
--> [["2024-01-15T10:20:30", "2024-01-15T10:20:30.123000", "2024-01-15T10:20:30", "2024-01-15T10:20:30.123000"]]

select to_timestamp('01/15/2024 02:30:00 PM', 'MM/DD/YYYY HH12:MI:SS AM'), to_timestamp_ntz('2024-01-15 10:20:30.5', 'YYYY-MM-DD HH24:MI:SS.FF'), to_timestamp('2024-01-15'::date);
--> [["2024-01-15T14:30:00", "2024-01-15T10:20:30.500000", "2024-01-15T00:00:00"]]

select to_time('10:20:30'), to_time('10.20', 'HH24.MI'), time('23:59:59'), to_time('2024-01-15 10:20:30'::timestamp);
--> [["10:20:30", "10:20:00", "23:59:59", "10:20:30"]]

select to_char('2024-01-05'::date, 'YYYY-MM-DD'), to_char('2024-01-05'::date, 'DD/MM/YY'), to_char('2024-01-05'::date, 'Mon DD, YYYY'), to_char('2024-01-05'::date, 'YYYYMMDD');
--> [["2024-01-05", "05/01/24", "Jan 05, 2024", "20240105"]]

select to_char('2024-01-05'::date, 'MON'), to_char('2024-01-05'::date, 'mon'), to_char('2024-01-05'::date, 'DY'), to_char('2024-01-05'::date, 'Dy'), to_char('2024-01-05'::date, 'MMMM');
--> [["Jan", "Jan", "Fri", "Fri", "January"]]

select to_char('2024-01-05 14:07:09.123'::timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'), to_char('2024-01-05 14:07:09.123'::timestamp, 'HH12:MI AM'), to_char('2024-01-05 14:07:09.123'::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS');
--> [["2024-01-05 14:07:09.123", "02:07 PM", "2024-01-05T14:07:09"]]

select to_char('2024-01-05 14:07:09.123456'::timestamp, 'FF'), to_char('2024-01-05 14:07:09.123456'::timestamp, 'FF6'), to_char('2024-01-05 14:07:09'::timestamp, 'FF0'), to_char('14:07:09'::time, 'HH24MISS');
--> [["123456000", "123456", "", "140709"]]

select to_char('2024-01-05'::date), to_varchar('2024-01-05 14:07:09'::timestamp), '2024-01-05 14:07:09.5'::timestamp::varchar, '14:07:09'::time::varchar, '2024-01-05'::date::string;
--> [["2024-01-05", "2024-01-05 14:07:09.000", "2024-01-05 14:07:09.500", "14:07:09", "2024-01-05"]]

select to_varchar(d, 'DD.MM.YYYY'), to_char(ts, 'YYYY-MM') from t order by id;
--> [["31.01.2024", "2024-01"], ["29.02.2024", "2024-02"], ["31.12.2023", "2023-12"], [null, null]]

select 'at ' || '2024-01-05 14:07:09'::timestamp, concat('d=', '2024-01-05'::date);
--> [["at 2024-01-05 14:07:09.000", "d=2024-01-05"]]

select convert_timezone('UTC', 'America/New_York', '2024-01-15 12:00:00'::timestamp_ntz), convert_timezone('America/New_York', 'Europe/Oslo', '2024-07-01 12:00:00'::timestamp_ntz);
--> [["2024-01-15T07:00:00", "2024-07-01T18:00:00"]]

select convert_timezone('Europe/Oslo', 'UTC', '2024-03-31 03:30:00'::timestamp_ntz), convert_timezone('UTC', 'Asia/Kolkata', '2024-01-15 12:00:00');
--> [["2024-03-31T01:30:00", "2024-01-15T17:30:00"]]

select '2024-01-15 12:00:00'::timestamp_ltz, to_timestamp_ltz('2024-01-15 12:00:00 +02:00'), '2024-01-15 12:00:00'::timestamp_ltz::timestamp_ntz;
--> [["2024-01-15T12:00:00+00:00", "2024-01-15T10:00:00+00:00", "2024-01-15T12:00:00"]]

select '2024-01-10'::date + 5, '2024-01-10'::date - 1, '2024-01-10'::date - '2024-01-01'::date;
--> [["2024-01-15", "2024-01-09", 9]]

select '2024-01-31'::date + interval '1 month', '2024-01-31'::date - interval '1 day', '2024-01-31'::date + interval '2 hours';
--> [["2024-02-29", "2024-01-30", "2024-01-31T02:00:00"]]

select '2024-01-31 10:00:00'::timestamp + interval '1 day, 2 hours', '2024-01-31 10:00:00'::timestamp - interval '30 minutes';
--> [["2024-02-01T12:00:00", "2024-01-31T09:30:00"]]

select current_date = current_timestamp::date, current_date() = to_date(sysdate()), getdate() <= current_timestamp(), datediff(day, current_date, dateadd(day, 7, current_date));
--> [[true, true, true, 7]]

select id from t where d >= '2024-01-01' and d < '2024-02-01', (select 1) x;
--> [[1]]

select id from t where ts between '2024-02-01' and '2024-03-01' order by id;
--> [[2]]

select max(d), min(ts), greatest('2024-01-01'::date, '2024-06-01'::date), least('2024-01-01'::date, '2024-06-01'::date) from t;
--> [["2024-02-29", "2023-12-31T00:00:00.123000", "2024-06-01", "2024-01-01"]]

select d, row_number() over (order by d) from t where d is not null order by d desc limit 1;
--> [["2024-02-29", 3]]

select to_date(d::varchar), d::timestamp, ts::date, ts::time from t where id = 1;
--> [["2024-01-31", "2024-01-31T00:00:00", "2024-01-31", "23:59:59"]]
