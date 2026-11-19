"""Snowflake CLI (`snow`) against the emulator. Skipped when the CLI is not installed."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from sfemu.testing import RunningServer

SNOW = shutil.which("snow")
pytestmark = pytest.mark.skipif(SNOW is None, reason="Snowflake CLI (snow) is not on PATH")


@pytest.fixture
def snow(server: RunningServer, tmp_path: Path):
    config = tmp_path / "config.toml"
    config.write_text(
        'default_connection_name = "emu"\n'
        "[connections.emu]\n"
        'account = "test"\nuser = "test"\npassword = "test"\n'
        f'host = "{server.host}"\nport = {server.port}\nprotocol = "http"\n'
        'database = "demo"\nschema = "public"\nwarehouse = "wh"\n'
    )
    config.chmod(0o600)  # the CLI refuses a config readable by others
    env = {**os.environ, "SNOWFLAKE_HOME": str(tmp_path), "NO_COLOR": "1"}

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        command = [SNOW, "--config-file", str(config), *args]
        return subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)

    return run


def test_connection_test(snow):
    result = snow("connection", "test")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout
    assert "DEMO" in result.stdout


def test_sql_script(snow):
    script = "create table t (id int); insert into t values (1), (2); select sum(id) total from t"
    result = snow("sql", "-q", script, "--format", "json")
    assert result.returncode == 0, result.stdout + result.stderr
    created, inserted, selected = json.loads(result.stdout)
    assert inserted == [{"number of rows inserted": 2}]
    assert selected == [{"TOTAL": 3}]


def test_sql_error_is_reported(snow):
    result = snow("sql", "-q", "select 'abc")
    assert result.returncode != 0
    assert "001003" in result.stdout + result.stderr
