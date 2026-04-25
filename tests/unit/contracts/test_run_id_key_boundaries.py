from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb

from astock_lifespan_alpha.alpha.schema import initialize_alpha_signal_schema, initialize_alpha_trigger_schema
from astock_lifespan_alpha.data.schema import initialize_market_base_schema, initialize_raw_market_schema
from astock_lifespan_alpha.malf.schema import initialize_malf_schema
from astock_lifespan_alpha.pipeline.schema import initialize_pipeline_schema
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema
from astock_lifespan_alpha.position.schema import initialize_position_schema
from astock_lifespan_alpha.system.schema import initialize_system_schema
from astock_lifespan_alpha.trade.schema import initialize_trade_schema


SchemaInitializer = Callable[[Path], None]


LEDGER_TABLES = {
    "stock_daily_bar",
    "stock_weekly_bar",
    "stock_monthly_bar",
    "stock_daily_adjusted",
    "stock_weekly_adjusted",
    "stock_monthly_adjusted",
    "malf_pivot_ledger",
    "malf_wave_ledger",
    "malf_state_snapshot",
    "malf_wave_scale_snapshot",
    "malf_wave_scale_profile",
    "alpha_trigger_event",
    "alpha_trigger_profile",
    "alpha_signal",
    "position_candidate_audit",
    "position_capacity_snapshot",
    "position_sizing_snapshot",
    "position_exit_plan",
    "position_exit_leg",
    "portfolio_plan_snapshot",
    "trade_order_intent",
    "trade_order_execution",
    "trade_position_leg",
    "trade_carry_snapshot",
    "trade_exit_execution",
    "system_trade_readout",
    "system_portfolio_trade_summary",
}

CHECKPOINT_TABLES = {
    "stock_file_registry",
    "base_dirty_instrument",
    "malf_checkpoint",
    "alpha_checkpoint",
    "alpha_signal_checkpoint",
    "position_checkpoint",
    "portfolio_plan_checkpoint",
    "trade_checkpoint",
    "system_checkpoint",
    "pipeline_step_checkpoint",
}

QUEUE_TABLES = {
    "malf_work_queue",
    "alpha_work_queue",
    "alpha_signal_work_queue",
    "position_work_queue",
    "portfolio_plan_work_queue",
    "trade_work_queue",
    "system_work_queue",
}

RUN_AUDIT_TABLES = {
    "raw_ingest_run",
    "raw_ingest_file",
    "base_build_run",
    "malf_run",
    "alpha_run",
    "alpha_signal_run",
    "position_run",
    "portfolio_plan_run",
    "portfolio_plan_run_snapshot",
    "trade_run",
    "trade_run_order_intent",
    "system_run",
    "pipeline_run",
    "pipeline_step_run",
}

TABLE_CLASSIFICATIONS = {
    "ledger": LEDGER_TABLES,
    "checkpoint": CHECKPOINT_TABLES,
    "queue": QUEUE_TABLES,
    "run_audit": RUN_AUDIT_TABLES,
}

RUN_ID_KEY_COLUMNS = {
    "run_id",
    "runner_run_id",
    "pipeline_run_id",
    "last_run_id",
    "last_pipeline_run_id",
}


def test_formal_tables_have_exhaustive_key_boundary_classification(tmp_path):
    table_primary_keys = _initialize_and_collect_primary_keys(tmp_path)
    classified_tables = set().union(*TABLE_CLASSIFICATIONS.values())

    assert table_primary_keys.keys() == classified_tables


def test_ledger_and_checkpoint_primary_keys_do_not_include_run_id(tmp_path):
    table_primary_keys = _initialize_and_collect_primary_keys(tmp_path)

    for table_name in sorted(LEDGER_TABLES | CHECKPOINT_TABLES):
        primary_key_columns = set(table_primary_keys[table_name])
        assert primary_key_columns, f"{table_name} must define a stable primary key."
        assert primary_key_columns.isdisjoint(RUN_ID_KEY_COLUMNS), (
            f"{table_name} primary key must not contain run lineage columns: {primary_key_columns}"
        )


def test_work_queue_tables_are_run_trace_not_ledger_or_checkpoint(tmp_path):
    table_primary_keys = _initialize_and_collect_primary_keys(tmp_path)

    assert QUEUE_TABLES.isdisjoint(LEDGER_TABLES)
    assert QUEUE_TABLES.isdisjoint(CHECKPOINT_TABLES)
    for table_name in sorted(QUEUE_TABLES):
        assert table_primary_keys[table_name] == ("queue_id",)


def test_run_audit_tables_are_only_place_run_id_primary_keys_are_allowed(tmp_path):
    table_primary_keys = _initialize_and_collect_primary_keys(tmp_path)

    run_id_keyed_tables = {
        table_name
        for table_name, primary_key_columns in table_primary_keys.items()
        if set(primary_key_columns) & RUN_ID_KEY_COLUMNS
    }

    assert run_id_keyed_tables <= RUN_AUDIT_TABLES
    assert {"malf_run", "alpha_run", "trade_run", "pipeline_step_run"} <= run_id_keyed_tables


def _initialize_and_collect_primary_keys(tmp_path: Path) -> dict[str, tuple[str, ...]]:
    database_initializers: list[tuple[Path, SchemaInitializer]] = [
        (tmp_path / "malf.duckdb", initialize_malf_schema),
        (tmp_path / "alpha.duckdb", _initialize_alpha_schemas),
        (tmp_path / "position.duckdb", initialize_position_schema),
        (tmp_path / "portfolio_plan.duckdb", initialize_portfolio_plan_schema),
        (tmp_path / "trade.duckdb", initialize_trade_schema),
        (tmp_path / "system.duckdb", initialize_system_schema),
        (tmp_path / "pipeline.duckdb", initialize_pipeline_schema),
    ]
    for database_path, initializer in database_initializers:
        initializer(database_path)

    data_root = tmp_path / "data"
    data_database_paths = []
    for timeframe in ("day", "week", "month"):
        data_database_paths.append(initialize_raw_market_schema(data_root, timeframe=timeframe))
        data_database_paths.append(initialize_market_base_schema(data_root, timeframe=timeframe))

    table_primary_keys: dict[str, tuple[str, ...]] = {}
    for database_path, _initializer in database_initializers:
        table_primary_keys.update(_primary_keys_by_table(database_path))
    for database_path in data_database_paths:
        table_primary_keys.update(_primary_keys_by_table(database_path))

    return table_primary_keys


def _initialize_alpha_schemas(database_path: Path) -> None:
    initialize_alpha_trigger_schema(database_path)
    initialize_alpha_signal_schema(database_path)


def _primary_keys_by_table(database_path: Path) -> dict[str, tuple[str, ...]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = [
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                    AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        return {table_name: _primary_key_columns(connection, table_name) for table_name in table_names}


def _primary_key_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> tuple[str, ...]:
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    primary_key_rows = sorted((row for row in rows if row[5]), key=lambda row: row[5])
    return tuple(row[1] for row in primary_key_rows)
