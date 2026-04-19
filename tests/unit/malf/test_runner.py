from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from astock_lifespan_alpha.malf import run_malf_day_build, run_malf_month_build, run_malf_week_build
from astock_lifespan_alpha.malf.source import load_source_bars
from astock_lifespan_alpha.core.paths import default_settings
from astock_lifespan_alpha.malf.contracts import Timeframe
from astock_lifespan_alpha.malf.schema import MALF_TABLES
from astock_lifespan_alpha.malf.source import DAY_ADJUST_METHOD, SourceContractViolationError


def test_malf_runner_initializes_formal_schema(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-05T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-06T00:00:00", 10.8, 12.0, 10.0, 11.9),
        ],
    )

    run_malf_day_build()
    run_malf_week_build()
    run_malf_month_build()

    for target_path in (
        workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_week.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_month.duckdb",
    ):
        with duckdb.connect(str(target_path), read_only=True) as connection:
            table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert set(MALF_TABLES).issubset(table_names)


def test_malf_day_runner_materializes_semantic_outputs(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ("AAA", "2026-01-06T00:00:00", 10.2, 10.3, 9.6, 9.8),
            ("AAA", "2026-01-07T00:00:00", 9.8, 9.9, 9.1, 9.2),
        ],
    )

    summary = run_malf_day_build()

    assert summary.runner_name == "run_malf_day_build"
    assert summary.status == "completed"
    assert summary.materialization_counts["wave_scale_snapshot_rows"] == 6
    assert summary.checkpoint_summary.symbols_updated == 1

    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path), read_only=True) as connection:
        snapshots = connection.execute(
            """
            SELECT direction, new_count, no_new_span, life_state, wave_position_zone
            FROM malf_wave_scale_snapshot
            ORDER BY bar_dt
            """
        ).fetchall()
        pivot_types = {row[0] for row in connection.execute("SELECT pivot_type FROM malf_pivot_ledger").fetchall()}
        wave_life_states = {row[0] for row in connection.execute("SELECT life_state FROM malf_wave_ledger").fetchall()}
        snapshot_life_states = {row[3] for row in snapshots}

    assert snapshots[1][1] >= 1
    assert snapshots[2][2] >= 1
    assert "break_down" in pivot_types
    assert "broken" in wave_life_states
    assert "reborn" in snapshot_life_states
    assert {row[4] for row in snapshots}.issubset(
        {"early_progress", "mature_progress", "mature_stagnation", "weak_stagnation"}
    )


def test_malf_runner_checkpoint_skips_unchanged_source(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    source_path = workspace / "data" / "base" / "market_base.duckdb"
    _write_day_source_bars(
        source_path,
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
        ],
    )

    first_summary = run_malf_day_build()
    second_summary = run_malf_day_build()

    assert first_summary.materialization_counts["wave_rows"] >= 1
    assert second_summary.materialization_counts["wave_rows"] == 0
    assert second_summary.checkpoint_summary.symbols_updated == 0

    _append_day_source_bars(
        source_path,
        [
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ("AAA", "2026-01-06T00:00:00", 10.2, 10.3, 9.8, 10.0),
        ],
    )

    third_summary = run_malf_day_build()
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path), read_only=True) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM malf_wave_scale_snapshot").fetchone()[0]

    assert third_summary.materialization_counts["wave_rows"] >= 1
    assert snapshot_count == 5


def test_malf_source_reads_real_stock_adjusted_timeframe_databases(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_adjusted_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        "stock_daily_adjusted",
        [("AAA", "2026-01-02", 10.0, 11.0, 9.5, 10.8)],
    )
    _write_stock_adjusted_bars(
        workspace / "data" / "base" / "market_base_week.duckdb",
        "stock_weekly_adjusted",
        [("AAA", "2026-01-09", 10.8, 12.0, 10.0, 11.5)],
    )
    _write_stock_adjusted_bars(
        workspace / "data" / "base" / "market_base_month.duckdb",
        "stock_monthly_adjusted",
        [("AAA", "2026-01-31", 11.5, 13.0, 10.8, 12.2)],
    )

    settings = default_settings(repo_root=workspace / "repo")
    day_source = load_source_bars(settings, Timeframe.DAY)
    week_source = load_source_bars(settings, Timeframe.WEEK)
    month_source = load_source_bars(settings, Timeframe.MONTH)

    assert day_source.source_path == workspace / "data" / "base" / "market_base.duckdb"
    assert week_source.source_path == workspace / "data" / "base" / "market_base_week.duckdb"
    assert month_source.source_path == workspace / "data" / "base" / "market_base_month.duckdb"
    assert day_source.bars_by_symbol["AAA"][0].bar_dt.date().isoformat() == "2026-01-02"
    assert week_source.bars_by_symbol["AAA"][0].bar_dt.date().isoformat() == "2026-01-09"
    assert month_source.bars_by_symbol["AAA"][0].bar_dt.date().isoformat() == "2026-01-31"


def test_malf_day_source_filters_stock_daily_adjusted_to_backward(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_adjusted_bars_with_adjust_method(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02", "none", 10.0, 10.5, 9.8, 10.1),
            ("AAA", "2026-01-02", "backward", 20.0, 21.0, 19.5, 20.8),
            ("AAA", "2026-01-02", "forward", 30.0, 31.0, 29.8, 30.6),
            ("AAA", "2026-01-03", "backward", 20.8, 22.0, 20.0, 21.6),
            ("BBB", "2026-01-03", "backward", 8.0, 8.5, 7.9, 8.1),
        ],
    )

    source = load_source_bars(default_settings(repo_root=workspace / "repo"), Timeframe.DAY)

    assert source.selected_adjust_method == DAY_ADJUST_METHOD
    assert source.duplicate_symbol_trade_date_groups == 0
    assert [bar.bar_dt.date().isoformat() for bar in source.bars_by_symbol["AAA"]] == ["2026-01-02", "2026-01-03"]
    assert source.bars_by_symbol["AAA"][0].open == 20.0
    assert source.bars_by_symbol["AAA"][0].close == 20.8


def test_malf_day_runner_fails_fast_on_duplicate_backward_rows(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_adjusted_bars_with_adjust_method(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02", "backward", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-02", "backward", 10.1, 11.1, 9.6, 10.9),
            ("AAA", "2026-01-03", "backward", 10.8, 12.2, 10.1, 12.0),
        ],
    )

    with pytest.raises(SourceContractViolationError, match="duplicate_symbol_trade_date_groups=1"):
        run_malf_day_build(settings=default_settings(repo_root=workspace / "repo"))


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_day_source_bars(database_path: Path, rows: list[tuple[str, str, float, float, float, float]]) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
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
            [(symbol, datetime.fromisoformat(bar_dt), open_price, high_price, low_price, close_price) for symbol, bar_dt, open_price, high_price, low_price, close_price in rows],
        )


def _append_day_source_bars(database_path: Path, rows: list[tuple[str, str, float, float, float, float]]) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.executemany(
            "INSERT INTO market_base_day VALUES (?, ?, ?, ?, ?, ?)",
            [(symbol, datetime.fromisoformat(bar_dt), open_price, high_price, low_price, close_price) for symbol, bar_dt, open_price, high_price, low_price, close_price in rows],
        )


def _write_stock_adjusted_bars(
    database_path: Path,
    table_name: str,
    rows: list[tuple[str, str, float, float, float, float]],
) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.execute(
            f"""
            CREATE TABLE {table_name} (
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
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )


def _write_stock_adjusted_bars_with_adjust_method(
    database_path: Path,
    rows: list[tuple[str, str, str, float, float, float, float]],
) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
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
            rows,
        )
