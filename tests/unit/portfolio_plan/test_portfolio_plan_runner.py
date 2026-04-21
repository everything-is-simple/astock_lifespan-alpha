from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from astock_lifespan_alpha.alpha.schema import initialize_alpha_signal_schema
from astock_lifespan_alpha.portfolio_plan import repair_portfolio_plan_schema, run_portfolio_plan_build
from astock_lifespan_alpha.portfolio_plan.schema import PORTFOLIO_PLAN_TABLES, initialize_portfolio_plan_schema
from astock_lifespan_alpha.portfolio_plan import runner as portfolio_plan_runner
from astock_lifespan_alpha.position import run_position_from_alpha_signal


def test_portfolio_plan_schema_initializes_formal_tables(tmp_path):
    database_path = tmp_path / "portfolio_plan.duckdb"

    initialize_portfolio_plan_schema(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}

    assert set(PORTFOLIO_PLAN_TABLES).issubset(table_names)


def test_portfolio_plan_runner_handles_missing_position_database(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)

    summary = run_portfolio_plan_build()

    assert summary.status == "completed"
    assert summary.materialization_counts["snapshot_rows"] == 0
    assert summary.checkpoint_summary.work_units_seen == 0
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"),
        read_only=True,
    ) as connection:
        run_rows = connection.execute("SELECT status, bounded_candidate_count FROM portfolio_plan_run").fetchall()
        default_cap = connection.execute(
            "SELECT portfolio_gross_cap_weight FROM portfolio_plan_run"
        ).fetchone()[0]
        checkpoint_count = connection.execute("SELECT COUNT(*) FROM portfolio_plan_checkpoint").fetchone()[0]

    assert run_rows == [("completed", 0)]
    assert default_cap == 0.50
    assert checkpoint_count == 0


def test_portfolio_plan_runner_records_reused_checkpoint_fast_path(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    first_summary = run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)
    second_summary = run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)

    assert first_summary.checkpoint_summary.work_units_updated == 1
    assert second_summary.checkpoint_summary.work_units_updated == 0
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"),
        read_only=True,
    ) as connection:
        queue_rows = connection.execute(
            "SELECT status, source_row_count FROM portfolio_plan_work_queue ORDER BY requested_at DESC LIMIT 1"
        ).fetchall()
        checkpoint_rows = connection.execute(
            "SELECT portfolio_id, last_run_id FROM portfolio_plan_checkpoint"
        ).fetchall()

    assert queue_rows == [("reused", 5)]
    assert len(checkpoint_rows) == 1


def test_portfolio_plan_releases_capacity_after_scheduled_exit(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    run_portfolio_plan_build(portfolio_gross_cap_weight=0.08)

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"),
        read_only=True,
    ) as connection:
        rows = connection.execute(
            """
            SELECT candidate_nk, plan_status, admitted_weight, blocking_reason_code,
                   current_portfolio_gross_weight, remaining_portfolio_capacity_weight
            FROM portfolio_plan_snapshot
            WHERE candidate_nk IN ('signal:bof:1', 'signal:tst:1', 'signal:cpb:1')
            ORDER BY candidate_nk
            """
        ).fetchall()

    assert rows == [
        ("signal:bof:1", "trimmed", 0.08, "portfolio_capacity_trimmed", 0.0, 0.0),
        ("signal:cpb:1", "admitted", 0.048, None, 0.0, 0.032),
        ("signal:tst:1", "blocked", 0.0, "portfolio_capacity_exhausted", 0.08, 0.0),
    ]


def test_portfolio_plan_runner_rolls_back_failed_rematerialization(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)
    portfolio_path = workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"
    with duckdb.connect(str(portfolio_path), read_only=True) as connection:
        before_rows = connection.execute(
            """
            SELECT candidate_nk, plan_status, admitted_weight, trimmed_weight
            FROM portfolio_plan_snapshot
            ORDER BY candidate_nk
            """
        ).fetchall()

    def fail_checkpoint(*, connection, run_id):
        raise RuntimeError("checkpoint write failed")

    monkeypatch.setattr(portfolio_plan_runner, "_upsert_portfolio_plan_checkpoint_sql", fail_checkpoint)
    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        run_portfolio_plan_build(portfolio_gross_cap_weight=0.08)

    with duckdb.connect(str(portfolio_path), read_only=True) as connection:
        after_rows = connection.execute(
            """
            SELECT candidate_nk, plan_status, admitted_weight, trimmed_weight
            FROM portfolio_plan_snapshot
            ORDER BY candidate_nk
            """
        ).fetchall()
        latest_run = connection.execute(
            "SELECT status FROM portfolio_plan_run ORDER BY started_at DESC LIMIT 1"
        ).fetchone()[0]

    assert after_rows == before_rows
    assert latest_run == "interrupted"


def test_repair_portfolio_plan_schema_is_idempotent(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)
    portfolio_path = workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"
    with duckdb.connect(str(portfolio_path)) as connection:
        connection.execute("DELETE FROM portfolio_plan_checkpoint")

    first_summary = repair_portfolio_plan_schema()
    second_summary = repair_portfolio_plan_schema()

    assert first_summary.status == "completed"
    assert first_summary.checkpoint_rows_backfilled == 1
    assert second_summary.checkpoint_rows_backfilled == 0
    with duckdb.connect(str(portfolio_path), read_only=True) as connection:
        checkpoint_rows = connection.execute(
            "SELECT portfolio_id, last_run_id FROM portfolio_plan_checkpoint"
        ).fetchall()

    assert len(checkpoint_rows) == 1


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_market_base_day(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        ("AAA", "2026-01-03T00:00:00", 10.8),
        ("AAA", "2026-01-04T00:00:00", 11.2),
        ("AAA", "2026-01-05T00:00:00", 12.3),
        ("AAA", "2026-01-06T00:00:00", 12.2),
        ("AAA", "2026-01-07T00:00:00", 11.5),
    ]
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS market_base_day")
        connection.execute(
            """
            CREATE TABLE market_base_day (
                symbol TEXT,
                bar_dt TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO market_base_day VALUES (?, ?, ?, ?, ?, ?)",
            [
                (symbol, datetime.fromisoformat(bar_dt), close_price, close_price, close_price, close_price)
                for symbol, bar_dt, close_price in rows
            ],
        )


def _write_alpha_signal(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_alpha_signal_schema(database_path)
    rows = [
        ("signal:bof:1", "run-1", "AAA", date(2026, 1, 3), "bof", "confirmed", "alpha_bof", "event:bof:1", "wave-1", "up", 1, 0, "alive", 70.0, 20.0, "early_progress"),
        ("signal:tst:1", "run-1", "AAA", date(2026, 1, 4), "tst", "confirmed", "alpha_tst", "event:tst:1", "wave-1", "up", 1, 1, "alive", 68.0, 25.0, "mature_progress"),
        ("signal:pb:1", "run-1", "AAA", date(2026, 1, 5), "pb", "candidate", "alpha_pb", "event:pb:1", "wave-1", "up", 1, 2, "alive", 55.0, 45.0, "mature_progress"),
        ("signal:cpb:1", "run-1", "AAA", date(2026, 1, 6), "cpb", "confirmed", "alpha_cpb", "event:cpb:1", "wave-1", "up", 1, 2, "alive", 52.0, 58.0, "mature_stagnation"),
        ("signal:bpb:1", "run-1", "AAA", date(2026, 1, 7), "bpb", "confirmed", "alpha_bpb", "event:bpb:1", "wave-2", "down", 1, 1, "alive", 40.0, 65.0, "weak_stagnation"),
    ]
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DELETE FROM alpha_signal")
        connection.executemany(
            """
            INSERT INTO alpha_signal (
                signal_nk, run_id, symbol, signal_date, trigger_type, formal_signal_status, source_trigger_db,
                source_trigger_event_nk, wave_id, direction, new_count, no_new_span, life_state,
                update_rank, stagnation_rank, wave_position_zone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
