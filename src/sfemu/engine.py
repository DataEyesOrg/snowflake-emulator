"""The DuckDB instance behind the emulator.

One in-memory DuckDB holds everything. A Snowflake database is an attached
DuckDB database, so three-part names resolve natively, and every session gets
its own DuckDB cursor, so the current database and schema are per session.
"""

from __future__ import annotations

import threading

import duckdb

DEFAULT_SCHEMA = "PUBLIC"
# DuckDB's own catalogs, never visible as Snowflake databases.
_INTERNAL_DATABASES = ("memory", "system", "temp")


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class Engine:
    def __init__(self) -> None:
        self._root = duckdb.connect(":memory:")
        self._catalog_lock = threading.Lock()

    def cursor(self) -> duckdb.DuckDBPyConnection:
        cursor = self._root.cursor()
        cursor.execute("SET TimeZone = 'UTC'")
        return cursor

    def close(self) -> None:
        self._root.close()

    def _scalar(self, sql: str, params: list[str]) -> bool:
        cursor = self._root.cursor()
        try:
            return cursor.execute(sql, params).fetchone() is not None
        finally:
            cursor.close()

    def has_database(self, name: str) -> bool:
        if name in _INTERNAL_DATABASES:
            return False
        return self._scalar("SELECT 1 FROM duckdb_databases() WHERE database_name = ?", [name])

    def has_schema(self, database: str, schema: str) -> bool:
        return self._scalar(
            "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?",
            [database, schema],
        )

    def has_relation(self, kind: str, database: str, schema: str, name: str) -> bool:
        source = "duckdb_views()" if kind == "VIEW" else "duckdb_tables()"
        column = "view_name" if kind == "VIEW" else "table_name"
        return self._scalar(
            f"SELECT 1 FROM {source} WHERE database_name = ? AND schema_name = ? AND {column} = ?",
            [database, schema, name],
        )

    def create_database(self, name: str) -> None:
        with self._catalog_lock:
            cursor = self._root.cursor()
            try:
                cursor.execute(f"ATTACH ':memory:' AS {quote(name)}")
                cursor.execute(f"CREATE SCHEMA {quote(name)}.{quote(DEFAULT_SCHEMA)}")
            finally:
                cursor.close()

    def drop_database(self, name: str) -> None:
        with self._catalog_lock:
            cursor = self._root.cursor()
            try:
                cursor.execute(f"DETACH {quote(name)}")
            finally:
                cursor.close()
