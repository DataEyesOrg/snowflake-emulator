"""Mapping of DuckDB result types to Snowflake result metadata and JSON values.

Snowflake's JSON result format sends every value as a string (or null) and
relies on the ``rowtype`` metadata for decoding, so both halves live here.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

MAX_TEXT_LENGTH = 16_777_216
MAX_BINARY_LENGTH = 8_388_608

_INTEGER_IDS = {
    "tinyint", "smallint", "integer", "bigint", "hugeint",
    "utinyint", "usmallint", "uinteger", "ubigint", "uhugeint",
}  # fmt: skip
_TIMESTAMP_NTZ_IDS = {"timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns"}
_EPOCH_DATE = dt.date(1970, 1, 1)
_EPOCH_NAIVE = dt.datetime(1970, 1, 1)
_EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    precision: int | None = None
    scale: int | None = None
    length: int | None = None
    byte_length: int | None = None

    def to_rowtype(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "database": "",
            "schema": "",
            "table": "",
            "type": self.type,
            "nullable": self.nullable,
            "precision": self.precision,
            "scale": self.scale,
            "length": self.length,
            "byteLength": self.byte_length,
            "collation": None,
        }


def text_column(name: str, nullable: bool = True) -> Column:
    return Column(name, "text", nullable, length=MAX_TEXT_LENGTH, byte_length=MAX_TEXT_LENGTH)


def fixed_column(name: str, nullable: bool = True) -> Column:
    return Column(name, "fixed", nullable, precision=38, scale=0)


def column_from_duckdb(name: str, dtype: Any) -> Column:
    """Describe a DuckDB result column the way Snowflake would report it."""
    type_id = dtype.id
    if type_id in _INTEGER_IDS:
        return fixed_column(name)
    if type_id == "decimal":
        params = dict(dtype.children)
        return Column(name, "fixed", precision=params["precision"], scale=params["scale"])
    if type_id in ("float", "double"):
        return Column(name, "real")
    if type_id == "boolean":
        return Column(name, "boolean")
    if type_id == "varchar":
        # DuckDB's JSON is an alias over VARCHAR; Snowflake has no JSON type, only VARIANT.
        return Column(name, "variant") if str(dtype) == "JSON" else text_column(name)
    if type_id == "blob":
        return Column(name, "binary", length=MAX_BINARY_LENGTH, byte_length=MAX_BINARY_LENGTH)
    if type_id == "date":
        return Column(name, "date")
    if type_id == "time":
        return Column(name, "time", scale=9)
    if type_id in _TIMESTAMP_NTZ_IDS:
        return Column(name, "timestamp_ntz", scale=9)
    if type_id == "timestamp with time zone":
        # A DuckDB TIMESTAMPTZ is an instant with no stored offset, which is LTZ semantics.
        return Column(name, "timestamp_ltz", scale=9)
    if type_id in ("list", "array"):
        return Column(name, "array")
    if type_id in ("struct", "map"):
        return Column(name, "object")
    if type_id in ("variant", "union"):
        return Column(name, "variant")
    # NULL, UUID, INTERVAL, ENUM, BIT and anything new: Snowflake has no counterpart.
    return text_column(name)


def encode_value(value: Any, column: Column) -> str | None:
    """Encode one Python value from DuckDB as a Snowflake JSON-format cell."""
    if value is None:
        return None
    kind = column.type
    if kind == "fixed":
        return format(value, "f") if isinstance(value, Decimal) else str(int(value))
    if kind == "real":
        return _encode_real(value)
    if kind == "boolean":
        return "1" if value else "0"
    if kind == "date":
        return str((value - _EPOCH_DATE).days)
    if kind == "time":
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        return f"{seconds}.{value.microsecond:06d}000"
    if kind == "timestamp_ntz":
        return _encode_epoch(value - _EPOCH_NAIVE)
    if kind == "timestamp_ltz":
        return _encode_epoch(value - _EPOCH_UTC)
    if kind == "binary":
        return bytes(value).hex().upper()
    if kind in ("variant", "array", "object"):
        return value if isinstance(value, str) else json.dumps(value, indent=2, default=str)
    return str(value)


def _encode_real(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return repr(float(value))


def _encode_epoch(delta: dt.timedelta) -> str:
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    sign = "-" if micros < 0 else ""
    seconds, fraction = divmod(abs(micros), 1_000_000)
    return f"{sign}{seconds}.{fraction:06d}000"
