"""dbt against the emulator: the phase 2 acceptance test. Skipped when dbt is not installed."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import snowflake.connector

from sfemu.testing import RunningServer

DBT = shutil.which("dbt")
PROJECT = Path(__file__).parent / "dbt_project"
pytestmark = pytest.mark.skipif(DBT is None, reason="dbt is not on PATH")


@pytest.fixture
def dbt(server: RunningServer, tmp_path: Path):
    project = tmp_path / "project"
    shutil.copytree(PROJECT, project)
    (tmp_path / "profiles.yml").write_text(
        "sfemu:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: snowflake\n      account: test\n      user: test\n      password: test\n"
        "      role: dev\n      database: analytics\n      warehouse: wh\n      schema: shop\n"
        f"      threads: 4\n      host: {server.host}\n      port: {server.port}\n"
        "      protocol: http\n"
    )
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(tmp_path),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "NO_COLOR": "1",
    }

    def run(*args: str) -> None:
        command = [DBT, *args, "--project-dir", str(project)]
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=300)
        assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]

    return run


@pytest.fixture
def cur(server: RunningServer):
    parameters = server.connection_parameters(database="analytics", schema="shop")
    with snowflake.connector.connect(**parameters) as conn:
        yield conn.cursor()


def test_seed_run_test_snapshot(dbt, cur):
    dbt("seed")
    dbt("run")
    dbt("test")
    dbt("snapshot")

    cur.execute("select name, order_count, total_cents from customers order by customer_id")
    assert cur.fetchall() == [("Ada", 2, 2040), ("Bob", 1, 2500), ("Cyd", 1, 400)]
    objects = {row[1]: row[4] for row in cur.execute("show objects in schema analytics.shop")}
    assert objects == {
        "CUSTOMERS": "TABLE",
        "ORDERS_MERGED": "TABLE",
        "ORDERS_REPLACED": "TABLE",
        "RAW_CUSTOMERS": "TABLE",
        "RAW_ORDERS": "TABLE",
        "STG_ORDERS": "VIEW",
    }
    # persist_docs: the descriptions from schema.yml became comments.
    cur.execute("select comment from information_schema.tables where table_name = 'CUSTOMERS'")
    assert cur.fetchone() == ("One row per customer.",)
    described = {row[0]: row[9] for row in cur.execute("describe table customers")}
    assert described["CUSTOMER_ID"] == "The customer's key."


def test_a_second_run_is_incremental(dbt, cur):
    dbt("seed")
    dbt("run")
    dbt("snapshot")

    cur.execute("update raw_orders set status = 'completed' where id = 3")
    cur.execute("insert into raw_orders values (5, 2, '2024-04-04', 'placed', 700)")
    dbt("run")
    dbt("snapshot")
    dbt("test")

    for model in ("orders_merged", "orders_replaced"):
        cur.execute(f"select order_id, status from {model} order by order_id")
        assert cur.fetchall() == [
            (1, "completed"),
            (2, "completed"),
            (3, "completed"),
            (4, "returned"),
            (5, "placed"),
        ], model
    # The snapshot closed the old version of order 3 and opened a new one.
    cur.execute(
        "select status, dbt_valid_to is null from analytics.history.orders_history "
        "where order_id = 3 order by dbt_valid_from, dbt_valid_to nulls last"
    )
    assert cur.fetchall() == [("placed", False), ("completed", True)]
    cur.execute("select count(*) from analytics.history.orders_history")
    assert cur.fetchone() == (6,)


def test_full_refresh_build_and_docs(dbt, cur, tmp_path):
    dbt("build")
    dbt("build", "--full-refresh")
    dbt("docs", "generate")
    assert cur.execute("select count(*) from orders_merged").fetchone() == (4,)
    # dbt Core writes the catalog it read from INFORMATION_SCHEMA to catalog.json;
    # the Fusion engine keeps it elsewhere, and a successful run is all we see.
    catalog = tmp_path / "project" / "target" / "catalog.json"
    if catalog.exists():
        assert '"model.shop.customers"' in catalog.read_text()
