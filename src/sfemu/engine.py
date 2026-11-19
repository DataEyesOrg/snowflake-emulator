"""The DuckDB instance behind the emulator.

One in-memory DuckDB holds everything. A Snowflake database is an attached
DuckDB database, so three-part names resolve natively, and every session gets
its own DuckDB cursor, so the current database and schema are per session.
"""

from __future__ import annotations

import datetime as dt
import threading
from dataclasses import dataclass, field

import duckdb

DEFAULT_SCHEMA = "PUBLIC"
# DuckDB's own catalogs, never visible as Snowflake databases.
INTERNAL_DATABASES = ("memory", "system", "temp")


PRIMARY_KEY = "PRIMARY KEY"
UNIQUE = "UNIQUE"
FOREIGN_KEY = "FOREIGN KEY"

TableKey = tuple[str, str, str]  # (database, schema, table)


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass
class Constraint:
    """A key constraint of a table: PRIMARY KEY, UNIQUE or FOREIGN KEY."""

    kind: str
    name: str
    columns: list[str]
    # A foreign key's target. Without columns it points at the target's primary key.
    references: TableKey | None = None
    referenced_columns: list[str] = field(default_factory=list)


class Engine:
    def __init__(self) -> None:
        # SQL sent by a client must never reach the server's files or network:
        # this turns off read_csv, COPY, ATTACH of a file, extension installs.
        # It cannot be switched back on while the database is running.
        self._root = duckdb.connect(":memory:", config={"enable_external_access": False})
        self._catalog_lock = threading.Lock()
        # DuckDB keeps no creation times; every object is reported as created now.
        self.started = dt.datetime.now(dt.UTC).replace(microsecond=0)
        # Key constraints by table. Snowflake records them without enforcing
        # them, so they are kept here and never reach DuckDB, which would
        # enforce them.
        self.constraints: dict[TableKey, list[Constraint]] = {}

    def primary_key(self, table: TableKey) -> Constraint | None:
        return next((c for c in self.constraints.get(table, []) if c.kind == PRIMARY_KEY), None)

    def add_constraint(self, table: TableKey, constraint: Constraint) -> None:
        kept = self.constraints.setdefault(table, [])
        if constraint.kind == PRIMARY_KEY:  # a table has one; the new one replaces it
            kept[:] = [c for c in kept if c.kind != PRIMARY_KEY]
        kept.append(constraint)

    def drop_constraints(self, table: TableKey, *, name: str | None = None) -> None:
        """Forget the constraint of this name, or the primary key when no name is given."""
        kept = self.constraints.get(table, [])
        if name is None:
            kept[:] = [c for c in kept if c.kind != PRIMARY_KEY]
        else:
            kept[:] = [c for c in kept if c.name != name]

    def move_constraints(self, old: TableKey, new: TableKey) -> None:
        """A table was renamed: its constraints follow, and so do foreign keys to it."""
        moved = self.constraints.pop(old, None)
        if moved:
            self.constraints[new] = moved
        for constraints in self.constraints.values():
            for constraint in constraints:
                if constraint.references == old:
                    constraint.references = new

    def rename_column(self, table: TableKey, old: str, new: str) -> None:
        for key, constraints in self.constraints.items():
            for constraint in constraints:
                if key == table:
                    constraint.columns = [new if c == old else c for c in constraint.columns]
                if constraint.references == table:
                    constraint.referenced_columns = [
                        new if c == old else c for c in constraint.referenced_columns
                    ]

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
        if name in INTERNAL_DATABASES:
            return False
        return self._scalar("SELECT 1 FROM duckdb_databases() WHERE database_name = ?", [name])

    def has_schema(self, database: str, schema: str) -> bool:
        return self._scalar(
            "SELECT 1 FROM duckdb_schemas() "
            "WHERE database_name = ? AND schema_name = ? AND NOT internal",
            [database, schema],
        )

    @staticmethod
    def relation_kind(
        cursor: duckdb.DuckDBPyConnection, database: str | None, schema: str | None, name: str
    ) -> str | None:
        """'TABLE' or 'VIEW' when a relation with exactly this name exists, else None.

        DuckDB resolves identifiers case-insensitively and Snowflake does not,
        so a reference is checked here before DuckDB gets to resolve it. The
        lookup runs on the session's own cursor, which also sees whatever its
        open transaction has created.
        """
        if database is None or schema is None or database in INTERNAL_DATABASES:
            return None
        found = cursor.execute(
            "SELECT 'TABLE' FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = ? AND table_name = ? "
            "UNION ALL SELECT 'VIEW' FROM duckdb_views() "
            "WHERE database_name = ? AND schema_name = ? AND view_name = ?",
            [database, schema, name] * 2,
        ).fetchone()
        return found[0] if found else None

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
