"""Errors in the shape Snowflake drivers expect.

Codes and SQLSTATEs follow Snowflake's for the cases we can identify. The
mapping from DuckDB errors is best-effort until the conformance suite pins
each case against a real account.
"""

from __future__ import annotations

import re

import duckdb

_MISSING_RELATION = re.compile(r"(?:Table|View) with name (\S+) does not exist")


class SnowflakeError(Exception):
    def __init__(self, code: str, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.sqlstate = sqlstate
        self.message = message


def syntax_error(detail: str) -> SnowflakeError:
    return SnowflakeError("001003", "42000", f"SQL compilation error:\n{detail}")


def unsupported(feature: str) -> SnowflakeError:
    return SnowflakeError("000002", "0A000", f"Unsupported feature '{feature}'.")


def object_not_found(detail: str) -> SnowflakeError:
    return SnowflakeError("002003", "42S02", f"SQL compilation error:\n{detail}")


def session_gone() -> SnowflakeError:
    return SnowflakeError(
        "390111", "08001", "Session no longer exists. New login required to access the service."
    )


def from_duckdb(exc: duckdb.Error) -> SnowflakeError:
    detail = str(exc).strip()
    if isinstance(exc, duckdb.CatalogException):
        missing = _MISSING_RELATION.search(detail)
        if missing:
            detail = f"Object '{missing.group(1)}' does not exist or not authorized."
        return object_not_found(detail)
    if isinstance(exc, duckdb.ParserException):
        return syntax_error(detail)
    if isinstance(exc, duckdb.BinderException):
        return SnowflakeError("000904", "42000", f"SQL compilation error:\n{detail}")
    if isinstance(exc, duckdb.ConversionException):
        return SnowflakeError("100038", "22018", detail)
    if isinstance(exc, duckdb.ConstraintException):
        return SnowflakeError("100072", "22000", detail)
    return SnowflakeError("000603", "XX000", f"SQL execution error:\n{detail}")
