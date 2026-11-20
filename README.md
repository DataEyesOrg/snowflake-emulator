# snowflake-emulator

A local Snowflake emulator. It speaks the protocol Snowflake drivers use, so
existing code connects to it unchanged, and runs the SQL on DuckDB.

The goal is to develop and test Snowflake pipelines on a laptop or in CI
without an account and without spending credits.

**Status: early.** The Python connector, Snowflake CLI and dbt work against
it; see [what works](#what-works) and the [implementation plan](docs/PLAN.md).

## Quick start

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```sh
uv sync
uv run sfemu serve            # listens on 127.0.0.1:8085
```

Point the Snowflake connector at it by adding `host`, `port` and `protocol`:

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user="anyone",
    password="anything",
    account="local",
    host="127.0.0.1",
    port=8085,
    protocol="http",
    database="demo",
)
cur = conn.cursor()
cur.execute("create table orders as select 1 id, 9.99::number(10, 2) amount")
print(cur.execute("select * from orders").fetchall())
```

Credentials are not checked. The database and schema named at login are
created if they are missing; pass `--no-auto-create` to get Snowflake's
behaviour instead (the login succeeds with no current database).

State lives in memory and is gone when the server stops.

### In tests

```python
from sfemu.testing import running_server

with running_server() as server:
    conn = snowflake.connector.connect(**server.connection_parameters(database="demo"))
```

Each `running_server()` is a fresh, empty emulator on a free port.

### In Docker

```sh
docker compose up        # or: docker build -t sfemu . && docker run -p 8085:8085 sfemu
```

Point drivers at `host: 127.0.0.1`, `port: 8085`, `protocol: http`. State is
in memory; restarting the container resets the emulator.

## What works

Verified by the test suite with `snowflake-connector-python` 5.2, 3.15 and 4.8,
Snowflake CLI 3.34 and dbt (dbt Core 1.11 with dbt-snowflake, and the dbt
Fusion engine 2.0): `dbt seed`, `run`, `test`, `snapshot`, `build` and
`docs generate`, with table, view and incremental (merge, delete+insert,
append) models.

| Area | Supported |
|---|---|
| Protocol | login, query, logout; JSON and Arrow result formats; multi-statement requests (`num_statements`, `MULTI_STATEMENT_COUNT`); results and status of a recent query by its id |
| Queries | `SELECT` and set operations, translated by sqlglot; `RESULT_SCAN` and `LAST_QUERY_ID` over the results of recent statements |
| Semi-structured | `VARIANT`, `OBJECT` and `ARRAY` columns and values; paths (`v:a.b[0]`, `v['a']`, `GET`, `GET_PATH`); `PARSE_JSON`, `TRY_PARSE_JSON`, `CHECK_JSON`, `TO_VARIANT`, `TO_JSON`, `TO_ARRAY`; `OBJECT_CONSTRUCT` (also `_KEEP_NULL`), `OBJECT_INSERT` / `DELETE` / `PICK` / `KEYS`, `OBJECT_AGG`; `ARRAY_CONSTRUCT`, `ARRAY_AGG` and the `ARRAY_*` functions; `FLATTEN` with `PATH`, `OUTER` and `RECURSIVE`; `TYPEOF`, `IS_*`, `AS_*`; the implicit conversion of a `VARIANT` compared with or used as a typed value |
| Conversion | `TRY_CAST`, `TRY_TO_NUMBER` / `DECIMAL` / `DOUBLE` / `BOOLEAN` / `DATE` / `TIME` / `TIMESTAMP*` and their `TO_*` forms; `TO_TIMESTAMP` of text, of an epoch number and of a string of digits |
| Dates and times | `DATEADD`, `DATEDIFF`, `DATE_TRUNC`, `LAST_DAY`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `NEXT_DAY`, `*_FROM_PARTS`, `EXTRACT` and the `YEAR` ... `DAYOFWEEKISO` family, `INTERVAL` arithmetic, with a `DATE` staying a `DATE`; `TO_CHAR` and `TO_DATE` / `TO_TIME` / `TO_TIMESTAMP` with format models; `CONVERT_TIMEZONE`; timestamps as text in Snowflake's default formats |
| DML | `INSERT`, `INSERT OVERWRITE`, `UPDATE`, `DELETE`, `MERGE`, with Snowflake's row-count result sets |
| DDL | `CREATE` / `DROP` for databases, schemas, tables (also transient and temporary), views; `TRUNCATE`; `ALTER TABLE` / `VIEW` (rename, also into another schema; `SWAP WITH`; add / drop / rename / retype column; primary keys; comments); `COMMENT ON` |
| Catalog | `SHOW` databases, schemas, objects, tables, views (with `TERSE`, `LIKE`, `STARTS WITH`, `LIMIT ... FROM`), columns, primary, unique and imported keys, session parameters; `DESCRIBE TABLE` / `VIEW`; `INFORMATION_SCHEMA` views `SCHEMATA`, `TABLES`, `VIEWS`, `COLUMNS` |
| Access control | none: `GRANT` and `REVOKE` are accepted and ignored, `SHOW GRANTS` is empty |
| Session | `USE`, `ALTER SESSION`, login session parameters, `BEGIN` / `COMMIT` / `ROLLBACK`, `CURRENT_*()` |
| Binding | client-side (pyformat) and positional `?` for numbers, text, booleans, dates, times, timestamps and binary; `executemany` as an array bind |
| Results | Arrow for `fetch_pandas_all` / `get_result_batches`; large results in downloadable chunks |
| Errors | Snowflake error codes and SQLSTATEs for the common failures |

Token renewal and heartbeat are implemented and tested over plain HTTP, but
no driver has exercised them yet.

Statements outside this list are rejected with Snowflake's
`000002 (0A000): Unsupported feature` instead of being passed to DuckDB. A
statement that passes here by accident and fails on Snowflake is worse than
one that fails here honestly. The same goes for what hides inside an accepted
statement: functions the emulator has no Snowflake model for and DuckDB's
table functions (`read_csv`, `range`, `unnest`, ...). Table and view names are
case-sensitive, as on Snowflake.

SQL has no access to the server's files or network: DuckDB runs with external
access switched off.

Results follow the format the driver negotiated at login: a driver asking for
Arrow (`PYTHON_CONNECTOR_QUERY_RESULT_FORMAT=arrow`, the Python connector's
default when pyarrow is installed) gets Arrow for `SELECT`s, including large
results split into downloadable chunks; other statements answer in JSON, as
they do on Snowflake.

## Known differences from Snowflake

- The session time zone defaults to `UTC`, not `America/Los_Angeles`.
- `TIMESTAMP_TZ` values lose their offset and come back as `TIMESTAMP_LTZ`.
  For the same reason `CONVERT_TIMEZONE` with two arguments returns the
  target zone's wall clock as a `TIMESTAMP_NTZ`.
- Integer types are `NUMBER(38, 0)`, but an integer literal is still a DuckDB
  integer, so arithmetic on literals alone can overflow (`2147483647 + 1`).
- Column names are matched case-insensitively (`"id"` finds `ID`); table and
  view names are not.
- A function that sqlglot models but Snowflake does not have (a handful of
  DuckDB and other dialects' functions) is not rejected.
- An unaliased expression is named after sqlglot's rendering of it
  (`1 + 1`), which can differ in spacing from the text as typed (`1+1`).
- Error messages for cases not yet mapped carry DuckDB's wording.
- `AUTOCOMMIT = FALSE` is accepted but not applied.
- A statement that fails inside an explicit transaction aborts it: everything
  until `ROLLBACK` fails too. Snowflake would carry on with the transaction.
- Primary, unique and foreign keys are recorded and, as on Snowflake, not
  enforced. A foreign key may name a table that does not exist; it is then
  left out of `SHOW IMPORTED KEYS`. A key is dropped by constraint name or
  with `DROP PRIMARY KEY`; `DROP UNIQUE (...)`, `DROP FOREIGN KEY (...)` and
  `SHOW EXPORTED KEYS` are not supported.
- A table or view can be renamed into another schema but not into another
  database. Moving it recreates it, so views that select from it must be
  recreated, as on Snowflake.
- `RESULT_SCAN` reads the results of the last 256 statements, from any
  session; Snowflake keeps results for 24 hours and only shows them to the
  user who ran the query. `LAST_QUERY_ID` does not count failed statements.
- Temporary tables are dropped when their session ends, but until then other
  sessions can see them.
- The catalog has no history: every object reports the emulator's start time
  as its creation time, `SYSADMIN` as its owner, and transient tables are
  listed as ordinary ones. View definitions are shown as DuckDB rewrote them.
- `/` on integers gives a `FLOAT`, where Snowflake gives a `NUMBER` with six
  decimal places.

Dates and conversions:

- Timestamps and times keep microseconds, not nanoseconds.
- `DATEADD`, `DATE_TRUNC` and `+ INTERVAL` return a `DATE` for a `DATE` when
  the argument is known to be one, which is followed the same way as for
  `VARIANT` (see below). Where it is not known the result is a
  `TIMESTAMP_NTZ` with the same value.
- A timestamp becomes text in Snowflake's default formats. The session's
  `*_OUTPUT_FORMAT` and `*_INPUT_FORMAT` parameters are accepted but not
  applied, and neither are `WEEK_START` and `WEEK_OF_YEAR_POLICY`.
- Text is read as a date by DuckDB's rules: ISO dates and timestamps, and a
  string of digits as an epoch. Snowflake's guessing of other layouts
  (`15-Jan-2024`, `01/15/2024`) is not there; give `TO_DATE` a format.
- Format models know `YYYY`, `YY`, `MM`, `MON`, `MMMM`, `DD`, `DY`, `HH24`,
  `HH12`, `AM` / `PM`, `MI`, `SS`, `FF0`-`FF9`, `TZH` and `TZM`; anything else
  in a format stands for itself. A format must be a literal.
- Not supported: numeric formats in `TO_CHAR` and `TO_NUMBER`
  (`'999,999.00'`), `TRY_TO_BINARY`.

Semi-structured data:

- `VARIANT`, `OBJECT` and `ARRAY` are stored alike, so `DESCRIBE`, `SHOW
  COLUMNS` and `INFORMATION_SCHEMA` report all three as `VARIANT`. A result
  column is typed `ARRAY` or `OBJECT` when the query itself shows it to be
  one (`ARRAY_CONSTRUCT(...)`, `OBJECT_CONSTRUCT(...)`), else `VARIANT`.
- Arithmetic, `SUM` and `AVG` on a `VARIANT` give a `FLOAT`. `MIN` and `MAX`
  of a `VARIANT` compare the values as text.
- A value put into a `VARIANT` keeps no type of its own beyond JSON's: a date
  or timestamp becomes a string, and `TYPEOF` reports a `FLOAT` as `DECIMAL`.
  A number taken out of a document keeps about 17 significant digits.
- SQL `NULL` inside `ARRAY_CONSTRUCT` becomes JSON `null`; Snowflake keeps it
  apart as `undefined`.
- The conversion of a `VARIANT` in a comparison relies on knowing that the
  operand is one. That is followed through tables, views, CTEs, subqueries
  and `FLATTEN`; where it cannot be, the comparison fails rather than
  compares wrongly.
- An unaliased path is named after sqlglot's rendering (`GET_PATH(V, 'a')`),
  where Snowflake names it `V:a`.
- Not supported: `OBJECT_CONSTRUCT(*)`, `FLATTEN` with a `MODE`, structured
  types (`ARRAY(NUMBER)`, `OBJECT(a INT)`, `MAP`).

## Development

```sh
uv run pytest        # runs the real Snowflake connector against the emulator
uv run pytest tests/test_conformance.py   # only the corpus in tests/conformance
uv run ruff check .
uv run ruff format .
```

## Licence and trademarks

Apache-2.0. This project is not affiliated with or endorsed by Snowflake Inc.
