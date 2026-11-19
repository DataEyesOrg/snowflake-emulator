"""The DuckDB instance itself, below the SQL layer."""

from __future__ import annotations

import duckdb
import pytest

from sfemu.engine import Engine


@pytest.mark.parametrize(
    "sql",
    [
        "select * from read_text('/etc/hosts')",
        "copy (select 1) to '/tmp/sfemu-must-not-exist.csv'",
        "attach '/tmp/sfemu-must-not-exist.db' as leak",
        "set enable_external_access = true",
    ],
)
def test_the_engine_has_no_access_to_files(sql):
    # The SQL layer rejects such statements earlier; this is the backstop.
    engine = Engine()
    try:
        with pytest.raises(duckdb.Error):
            engine.cursor().execute(sql)
    finally:
        engine.close()
