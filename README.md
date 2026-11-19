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
| Protocol | login, query, logout; JSON and Arrow result formats |
| Queries | `SELECT` and set operations, translated by sqlglot |
| DML | `INSERT`, `INSERT OVERWRITE`, `UPDATE`, `DELETE`, `MERGE`, with Snowflake's row-count result sets |
| DDL | `CREATE` / `DROP` for databases, schemas, tables (also transient and temporary), views; `TRUNCATE`; `ALTER TABLE` / `VIEW` (rename, add / drop / rename / retype column, comments); `COMMENT ON` |
| Catalog | `SHOW` databases, schemas, objects, tables, views (with `TERSE`, `LIKE`, `STARTS WITH`, `LIMIT ... FROM`); `DESCRIBE TABLE` / `VIEW`; `INFORMATION_SCHEMA` views `SCHEMATA`, `TABLES`, `VIEWS`, `COLUMNS` |
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
- Temporary tables are dropped when their session ends, but until then other
  sessions can see them.
- The catalog has no history: every object reports the emulator's start time
  as its creation time, `SYSADMIN` as its owner, and transient tables are
  listed as ordinary ones. View definitions are shown as DuckDB rewrote them.
- `/` on integers gives a `FLOAT`, where Snowflake gives a `NUMBER` with six
  decimal places.

## Development

```sh
uv run pytest        # runs the real Snowflake connector against the emulator
uv run ruff check .
uv run ruff format .
```

## Licence and trademarks

Apache-2.0. This project is not affiliated with or endorsed by Snowflake Inc.
