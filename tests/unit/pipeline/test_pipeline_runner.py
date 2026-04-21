from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from astock_lifespan_alpha.pipeline import PipelineRunSummary, repair_pipeline_schema, run_data_to_system_pipeline
from astock_lifespan_alpha.pipeline import runner as pipeline_runner
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
    assert summary.resume_summary.resumed_from_run_id is None
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


def test_pipeline_runner_resumes_from_interrupted_trade_step(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted(workspace / "data" / "base" / "market_base.duckdb")
    attempts = {"count": 0}

    def flaky_trade(*, portfolio_id: str = "core", settings=None):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("trade step failed")

        class _Summary:
            def as_dict(self):
                return {
                    "runner_name": "run_trade_from_portfolio_plan",
                    "run_id": "trade-resume-success",
                    "status": "completed",
                    "target_path": str(settings.databases.trade),
                    "source_paths": {"portfolio_plan": str(settings.databases.portfolio_plan)},
                    "message": "trade resume completed.",
                    "materialization_counts": {},
                    "checkpoint_summary": {
                        "work_units_seen": 0,
                        "work_units_updated": 0,
                        "latest_reference_trade_date": None,
                    },
                }

        return _Summary()

    monkeypatch.setattr(pipeline_runner, "run_trade_from_portfolio_plan", flaky_trade)

    with pytest.raises(RuntimeError, match="trade step failed"):
        run_data_to_system_pipeline()

    resumed_summary = run_data_to_system_pipeline()

    assert resumed_summary.resume_summary.resume_start_step == 12
    assert resumed_summary.resume_summary.reused_step_count == 11
    assert resumed_summary.resume_summary.executed_step_count == 2
    pipeline_path = workspace / "data" / "astock_lifespan_alpha" / "pipeline" / "pipeline.duckdb"
    with duckdb.connect(str(pipeline_path), read_only=True) as connection:
        run_rows = connection.execute(
            "SELECT status, step_count FROM pipeline_run ORDER BY started_at"
        ).fetchall()
        reused_step = connection.execute(
            """
            SELECT summary_json
            FROM pipeline_step_run
            WHERE pipeline_run_id = ? AND step_order = 1
            """,
            [resumed_summary.run_id],
        ).fetchone()[0]
        resumed_trade_step = connection.execute(
            """
            SELECT runner_run_id, runner_status
            FROM pipeline_step_run
            WHERE pipeline_run_id = ? AND step_order = 12
            """,
            [resumed_summary.run_id],
        ).fetchone()

    assert run_rows[0] == ("interrupted", 11)
    assert run_rows[1] == ("completed", 13)
    assert '"pipeline_action": "reused_checkpoint"' in reused_step
    assert resumed_trade_step == ("trade-resume-success", "completed")


def test_repair_pipeline_schema_backfills_latest_completed_step_checkpoints(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted(workspace / "data" / "base" / "market_base.duckdb")

    run_data_to_system_pipeline()
    pipeline_path = workspace / "data" / "astock_lifespan_alpha" / "pipeline" / "pipeline.duckdb"
    with duckdb.connect(str(pipeline_path)) as connection:
        connection.execute("DELETE FROM pipeline_step_checkpoint")

    first_summary = repair_pipeline_schema()
    second_summary = repair_pipeline_schema()

    assert first_summary.checkpoint_rows_backfilled == 13
    assert second_summary.checkpoint_rows_backfilled == 0


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
