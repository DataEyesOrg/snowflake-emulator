"""The Arrow result format Snowflake drivers request for tabular results.

The wire shape is an Arrow IPC stream, base64-encoded into ``rowsetBase64``
(and, for large results, further batches behind chunk URLs). The encodings
here follow what Snowflake's server emits, as observed from the drivers'
decoders — the nanoarrow C++ in snowflake-connector-python, arrow-rs in the
5.x native core and Arrow in gosnowflake:

- each field carries Snowflake metadata: ``logicalType`` plus ``precision``,
  ``scale`` and ``charLength``;
- ``FIXED`` is an int64 (scaled when ``scale`` > 0) or a decimal128;
- ``TIME`` is an int64 count of nanoseconds since midnight (``scale`` 9);
- ``TIMESTAMP_NTZ`` / ``TIMESTAMP_LTZ`` are a struct of ``epoch`` (int64
  seconds) and ``fraction`` (int32 nanoseconds), which is what lets Snowflake
  cover years 0001-9999 despite the 1970-2262 int64 nanosecond range.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from sfemu.types import Column, encode_value

_EPOCH = pa.field("epoch", pa.int64(), nullable=False)
_FRACTION = pa.field("fraction", pa.int32(), nullable=False)


def snowflake_table(table: pa.Table, columns: list[Column]) -> pa.Table:
    """Convert a DuckDB Arrow table into Snowflake's Arrow representation."""
    arrays = [
        _column(column, column_data.combine_chunks())
        for column, column_data in zip(columns, table.columns, strict=True)
    ]
    schema = pa.schema(
        [_field(column, array.type) for column, array in zip(columns, arrays, strict=True)]
    )
    return pa.Table.from_arrays(arrays, schema=schema)


def ipc_stream(batches: list[pa.RecordBatch]) -> bytes:
    """Encode record batches as one self-contained Arrow IPC stream.

    No stream magic is written: pyarrow's reader does not need it, and the
    connectors' nanoarrow decoders expect the first message's continuation
    word right at the start of the body.
    """
    if not batches:
        raise ValueError("an IPC stream needs at least one record batch")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _field(column: Column, arrow_type: pa.DataType) -> pa.Field:
    return pa.field(
        column.name,
        arrow_type,
        nullable=column.nullable,
        metadata={
            "logicalType": column.type.upper(),
            "precision": str(column.precision or 38),
            "scale": str(column.scale or 0),
            "charLength": str(column.length or 0),
        },
    )


def _column(column: Column, data: pa.Array) -> pa.Array:
    """Re-encode one DuckDB Arrow column the way Snowflake encodes it."""
    kind = column.type
    if kind == "fixed":
        if pa.types.is_signed_integer(data.type):
            return data
        if pa.types.is_decimal(data.type) and data.type.scale > 0:
            return data  # decimal128 is understood as-is
        # NUMBER(p, 0), which every Snowflake integer type is: like Snowflake,
        # send an int64 while the values fit, so pandas gets an integer column
        # and not Decimal objects, and a decimal128 only when they do not.
        try:
            return data.cast(pa.int64())
        except pa.ArrowInvalid:
            return data.cast(pa.decimal128(38, 0))
    if kind == "real":
        return data.cast(pa.float64())
    if kind in ("text", "variant", "array", "object"):
        # Semi-structured values travel as JSON text, exactly like in the
        # JSON result format; untyped NULL and enums become plain text.
        if pa.types.is_string(data.type) or pa.types.is_large_string(data.type):
            return data.cast(pa.string())
        return pa.array(
            [encode_value(value, column) for value in data.to_pylist()], type=pa.string()
        )
    if kind == "time":
        return pc.multiply(data.cast(pa.int64()), _subsecond_multiple(data.type))
    if kind in ("timestamp_ntz", "timestamp_ltz"):
        return _timestamp_struct(data)
    return data  # boolean, date, binary: DuckDB's encoding already matches


def _timestamp_struct(data: pa.Array) -> pa.Array:
    """A DuckDB timestamp as the epoch/fraction struct Snowflake expects."""
    seconds = pc.floor_temporal(data, unit="second").cast(pa.int64())
    fraction = pc.round(pc.multiply(pc.subsecond(data), 1_000_000_000)).cast(pa.int32())
    mask = data.is_null()
    return pa.StructArray.from_arrays(
        [pc.divide(seconds, _unit_multiple(data.type)), fraction],
        fields=[_EPOCH, _FRACTION],
        mask=mask,
    )


def _subsecond_multiple(time_type: pa.DataType) -> int:
    if pa.types.is_time64(time_type) and time_type.unit == "ns":
        return 1
    return 1_000  # DuckDB's TIME comes as microseconds; Snowflake wants nanoseconds


def _unit_multiple(timestamp_type: pa.DataType) -> int:
    unit = timestamp_type.unit
    return {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
