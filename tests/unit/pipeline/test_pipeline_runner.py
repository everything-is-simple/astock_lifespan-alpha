from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from astock_lifespan_alpha.pipeline import PipelineRunSummary, run_data_to_system_pipeline
from astock_lifespan_alpha.pipeline.schema import PIPELINE_TABLES, initialize_pipeline_schema


EXPECTED_STEP_ORDER = [
    "run_malf_day_build",
    "run_malf_week_build",
    "run_malf_month_build",
    "run_alpha_bof_build",
    "run_alpha_tst_build",
    "run_alpha_pb_build",
    "run_alpha_cpb_build",
    "run_alpha_bpb_build",
    "run_alpha_signal_build",
    "run_position_from_alpha_signal",
    "run_portfolio_plan_build",
    "run_trade_from_portfolio_plan",
    "run_system_from_trade",
]


def test_pipeline_schema_initializes_formal_tables(tmp_path):
    database_path = tmp_path / "pipeline.duckdb"

    initialize_pipeline_schema(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}

    assert set(PIPELINE_TABLES).issubset(table_names)


def test_pipeline_runner_records_all_steps_on_empty_workspace(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)

    summary = run_data_to_system_pipeline()

    assert isinstance(summary, PipelineRunSummary)
    assert summary.runner_name == "run_data_to_system_pipeline"
    assert summary.status == "completed"
    assert summary.step_count == 13
    assert [step.runner_name for step in summary.steps] == EXPECTED_STEP_ORDER
    assert all(step.status == "completed" for step in summary.steps)
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "pipeline" / "pipeline.duckdb"),
        read_only=True,
    ) as connection:
        step_rows = connection.execute(
            """
            SELECT step_order, runner_name, runner_status
            FROM pipeline_step_run
            ORDER BY step_order
            """
        ).fetchall()
        run_row = connection.execute(
            "SELECT status, portfolio_id, step_count, pipeline_contract_version FROM pipeline_run"
        ).fetchone()

    assert [row[1] for row in step_rows] == EXPECTED_STEP_ORDER
    assert all(row[2] == "completed" for row in step_rows)
    assert run_row == ("completed", "core", 13, "stage8_pipeline_v1")


def test_pipeline_runner_uses_temp_workspace_and_reaches_system(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted(workspace / "data" / "base" / "market_base.duckdb")

    summary = run_data_to_system_pipeline(portfolio_id="core")

    assert summary.steps[-1].runner_name == "run_system_from_trade"
    assert summary.steps[-1].status == "completed"
    assert Path(summary.target_path) == workspace / "data" / "astock_lifespan_alpha" / "pipeline" / "pipeline.duckdb"
    assert (workspace / "data" / "astock_lifespan_alpha" / "system" / "system.duckdb").exists()


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_stock_daily_adjusted(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        ("AAA", date(2026, 1, 2), 10.0, 11.0, 9.5, 10.8),
        ("AAA", date(2026, 1, 3), 10.8, 12.2, 10.1, 11.2),
        ("AAA", date(2026, 1, 4), 12.0, 12.4, 11.8, 12.3),
    ]
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS stock_daily_adjusted")
        connection.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

