from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.alpha.schema import initialize_alpha_signal_schema
from astock_lifespan_alpha.core.paths import default_settings
from astock_lifespan_alpha.portfolio_plan import run_portfolio_plan_build
from astock_lifespan_alpha.portfolio_plan.schema import PORTFOLIO_PLAN_TABLES
from astock_lifespan_alpha.position import run_position_from_alpha_signal
from astock_lifespan_alpha.position.source import load_position_source_rows
from astock_lifespan_alpha.position.schema import POSITION_TABLES


def test_position_runner_initializes_formal_schema(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    summary = run_position_from_alpha_signal()

    assert summary.status == "completed"
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "position" / "position.duckdb"), read_only=True
    ) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    assert set(POSITION_TABLES).issubset(table_names)


def test_position_runner_materializes_candidate_capacity_and_sizing(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    first_summary = run_position_from_alpha_signal()
    second_summary = run_position_from_alpha_signal()

    assert first_summary.materialization_counts["candidate_rows"] == 5
    assert first_summary.materialization_counts["capacity_rows"] == 5
    assert first_summary.materialization_counts["sizing_rows"] == 5
    assert first_summary.materialization_counts["exit_plan_rows"] == 3
    assert first_summary.materialization_counts["exit_leg_rows"] == 3
    assert second_summary.materialization_counts["candidate_rows"] == 0
    assert second_summary.checkpoint_summary.work_units_updated == 0

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "position" / "position.duckdb"), read_only=True
    ) as connection:
        status_rows = connection.execute(
            """
            SELECT candidate_status, COUNT(*)
            FROM position_candidate_audit
            GROUP BY candidate_status
            ORDER BY candidate_status
            """
        ).fetchall()
        max_weight = connection.execute(
            "SELECT MAX(final_allowed_position_weight) FROM position_sizing_snapshot"
        ).fetchone()[0]
        earliest_entry_trade_date = connection.execute(
            """
            SELECT MIN(planned_entry_trade_date)
            FROM position_sizing_snapshot
            WHERE final_allowed_position_weight > 0
            """
        ).fetchone()[0]

    assert dict(status_rows)["admitted"] >= 2
    assert dict(status_rows)["blocked"] >= 1
    assert max_weight <= 0.15
    assert earliest_entry_trade_date == date(2026, 1, 4)


def test_portfolio_plan_runner_bridges_from_position_outputs(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    first_summary = run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)
    second_summary = run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)

    assert first_summary.status == "completed"
    assert second_summary.status == "completed"

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"),
        read_only=True,
    ) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        plan_rows = connection.execute(
            """
            SELECT plan_status, COUNT(*)
            FROM portfolio_plan_snapshot
            GROUP BY plan_status
            ORDER BY plan_status
            """
        ).fetchall()
        actions = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT materialization_action FROM portfolio_plan_run_snapshot"
            ).fetchall()
        }

    assert set(PORTFOLIO_PLAN_TABLES).issubset(table_names)
    assert dict(plan_rows)["admitted"] >= 1
    assert dict(plan_rows)["trimmed"] >= 1
    assert dict(plan_rows)["blocked"] >= 1
    assert {"inserted", "reused"}.issubset(actions)


def test_portfolio_plan_runner_rematerializes_when_capacity_changes(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    run_position_from_alpha_signal()
    run_portfolio_plan_build(portfolio_gross_cap_weight=0.15)
    run_portfolio_plan_build(portfolio_gross_cap_weight=0.08)

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "portfolio_plan" / "portfolio_plan.duckdb"),
        read_only=True,
    ) as connection:
        rematerialized_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM portfolio_plan_run_snapshot
            WHERE materialization_action = 'rematerialized'
            """
        ).fetchone()[0]
        exhausted_rows = connection.execute(
            """
            SELECT candidate_nk, blocking_reason_code
            FROM portfolio_plan_snapshot
            WHERE blocking_reason_code = 'portfolio_capacity_exhausted'
            ORDER BY candidate_nk
            """
        ).fetchall()
        trimmed_rows = connection.execute(
            """
            SELECT candidate_nk, admitted_weight, trimmed_weight
            FROM portfolio_plan_snapshot
            WHERE plan_status = 'trimmed'
            ORDER BY candidate_nk
            """
        ).fetchall()

    assert rematerialized_rows >= 1
    assert exhausted_rows == [("signal:tst:1", "portfolio_capacity_exhausted")]
    assert trimmed_rows == [("signal:bof:1", 0.08, 0.04)]


def test_position_source_reads_stock_daily_adjusted_code_trade_date(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    dataset = load_position_source_rows(default_settings(repo_root=workspace / "repo"))

    assert dataset.market_source_path == workspace / "data" / "base" / "market_base.duckdb"
    assert dataset.row_count == 5
    assert dataset.rows_by_symbol["AAA"][0].symbol == "AAA"
    assert dataset.rows_by_symbol["AAA"][0].reference_trade_date == date(2026, 1, 3)


def test_position_runner_filters_backward_adjust_method(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_daily_adjusted_with_adjust_method(workspace / "data" / "base" / "market_base.duckdb")
    _write_alpha_signal(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb")

    summary = run_position_from_alpha_signal()

    assert summary.materialization_counts["candidate_rows"] == 5
    assert summary.checkpoint_summary.work_units_seen == 1
    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "position" / "position.duckdb"), read_only=True
    ) as connection:
        first_reference = connection.execute(
            """
            SELECT reference_price
            FROM position_candidate_audit
            ORDER BY signal_date, signal_nk
            LIMIT 1
            """
        ).fetchone()[0]
        first_entry_date = connection.execute(
            """
            SELECT planned_entry_trade_date
            FROM position_sizing_snapshot
            WHERE candidate_nk = 'signal:bof:1'
            """
        ).fetchone()[0]

    assert first_reference == 10.8
    assert first_entry_date == date(2026, 1, 4)


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


def _write_stock_daily_adjusted(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        ("AAA", date(2026, 1, 3), 10.8),
        ("AAA", date(2026, 1, 4), 11.2),
        ("AAA", date(2026, 1, 5), 12.3),
        ("AAA", date(2026, 1, 6), 12.2),
        ("AAA", date(2026, 1, 7), 11.5),
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
            [(symbol, trade_date, close_price, close_price, close_price, close_price) for symbol, trade_date, close_price in rows],
        )


def _write_stock_daily_adjusted_with_adjust_method(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    backward_rows = [
        ("AAA", date(2026, 1, 3), "backward", 10.8),
        ("AAA", date(2026, 1, 4), "backward", 11.2),
        ("AAA", date(2026, 1, 5), "backward", 12.3),
        ("AAA", date(2026, 1, 6), "backward", 12.2),
        ("AAA", date(2026, 1, 7), "backward", 11.5),
    ]
    forward_rows = [
        (symbol, trade_date, "forward", close_price + 100)
        for symbol, trade_date, _, close_price in backward_rows
    ]
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS stock_daily_adjusted")
        connection.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                code TEXT,
                trade_date DATE,
                adjust_method TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO stock_daily_adjusted VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (symbol, trade_date, adjust_method, close_price, close_price, close_price, close_price)
                for symbol, trade_date, adjust_method, close_price in backward_rows + forward_rows
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
