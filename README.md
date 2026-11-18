# snowflake-emulator

A local Snowflake emulator. It speaks the protocol Snowflake drivers use, so
existing code connects to it unchanged, and runs the SQL on DuckDB.

The goal is to develop and test Snowflake pipelines on a laptop or in CI
without an account and without spending credits.

**Status: early skeleton.** One driver and a small SQL surface work today;
see [what works](#what-works) and the [implementation plan](docs/PLAN.md).

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

## What works

Verified with `snowflake-connector-python` 5.2:

| Area | Supported |
|---|---|
| Protocol | login, query, logout; JSON result format |
| Queries | `SELECT` and set operations, translated by sqlglot |
| DML | `INSERT`, `UPDATE`, `DELETE`, with Snowflake's row-count result sets |
| DDL | `CREATE` / `DROP` for databases, schemas, tables, views; `TRUNCATE` |
| Session | `USE`, `ALTER SESSION`, `BEGIN` / `COMMIT` / `ROLLBACK`, `CURRENT_*()` |
| Binding | client-side (pyformat) and positional `?` for numbers, text, booleans |
| Errors | Snowflake error codes and SQLSTATEs for the common failures |

Token renewal and heartbeat are implemented and tested over plain HTTP, but
no driver has exercised them yet.

Statements outside this list are rejected with Snowflake's
`000002 (0A000): Unsupported feature` instead of being passed to DuckDB. A
statement that passes here by accident and fails on Snowflake is worse than
one that fails here honestly.

## Known differences from Snowflake

- The session time zone defaults to `UTC`, not `America/Los_Angeles`.
- `TIMESTAMP_TZ` values lose their offset and come back as `TIMESTAMP_LTZ`.
- Integer columns are DuckDB integers, not `NUMBER(38, 0)`, so they can
  overflow where Snowflake would not.
- An unaliased expression is named after sqlglot's rendering of it
  (`1 + 1`), which can differ in spacing from the text as typed (`1+1`).
- Error messages for cases not yet mapped carry DuckDB's wording.
- `AUTOCOMMIT = FALSE` is accepted but not applied.

## Development

```sh
uv run pytest        # runs the real Snowflake connector against the emulator
uv run ruff check .
uv run ruff format .
```

## Licence and trademarks

Apache-2.0. This project is not affiliated with or endorsed by Snowflake Inc.
