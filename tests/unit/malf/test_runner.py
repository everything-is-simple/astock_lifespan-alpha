from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

import astock_lifespan_alpha.malf.runner as malf_runner_module
from astock_lifespan_alpha.malf import (
    recover_malf_day_formal_target,
    run_malf_day_build,
    run_malf_month_build,
    run_malf_week_build,
)
from astock_lifespan_alpha.malf.source import load_source_bars
from astock_lifespan_alpha.core.paths import default_settings
from astock_lifespan_alpha.malf.contracts import Timeframe
from astock_lifespan_alpha.malf.repair import repair_malf_day_schema
from astock_lifespan_alpha.malf.schema import MALF_TABLES, initialize_malf_schema
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


def test_malf_schema_backfills_legacy_malf_run_progress_columns(tmp_path):
    target_path = tmp_path / "legacy_malf_day.duckdb"
    _write_legacy_malf_run(target_path)

    initialize_malf_schema(target_path)

    with duckdb.connect(str(target_path), read_only=True) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM malf_run WHERE run_id = 'day-legacy'").fetchone()[0]
        columns = {
            row[1]: {"type": row[2], "notnull": bool(row[3]), "default": row[4]}
            for row in connection.execute("PRAGMA table_info('malf_run')").fetchall()
        }
        backfilled_values = connection.execute(
            """
            SELECT
                symbols_total,
                symbols_completed,
                current_symbol,
                elapsed_seconds,
                estimated_remaining_symbols
            FROM malf_run
            WHERE run_id = 'day-legacy'
            """
        ).fetchone()

    assert row_count == 1
    assert columns["symbols_total"]["notnull"] is True
    assert columns["symbols_total"]["default"] == "0"
    assert columns["symbols_completed"]["notnull"] is True
    assert columns["elapsed_seconds"]["notnull"] is True
    assert columns["estimated_remaining_symbols"]["notnull"] is True
    assert backfilled_values == (0, 0, None, 0.0, 0)


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
    assert summary.segment_summary.full_universe is True
    assert summary.progress_summary.symbols_total == 1
    assert summary.progress_summary.symbols_completed == 1
    assert summary.progress_summary.progress_path is not None
    timing = summary.as_dict()["write_timing_summary"]
    assert timing["write_seconds"] >= 0.0
    assert timing["delete_old_rows_seconds"] >= 0.0
    assert timing["insert_ledgers_seconds"] >= 0.0
    assert timing["checkpoint_seconds"] >= 0.0
    assert timing["queue_update_seconds"] >= 0.0
    progress_payload = json.loads(Path(summary.progress_summary.progress_path).read_text(encoding="utf-8"))
    assert progress_payload["symbols_total"] == 1
    assert progress_payload["symbols_completed"] == 1
    assert progress_payload["ledger_rows_written"]["wave_scale_snapshot_rows"] == 6

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


def test_malf_day_runner_batches_multi_symbol_writes(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    rows = []
    for symbol in ("AAA", "BBB", "CCC"):
        rows.extend(
            [
                (symbol, "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
                (symbol, "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
                (symbol, "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
                (symbol, "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ]
        )
    _write_day_source_bars(workspace / "data" / "base" / "market_base.duckdb", rows)

    summary = run_malf_day_build()

    assert summary.checkpoint_summary.symbols_updated == 3
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path), read_only=True) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM malf_wave_scale_snapshot").fetchone()[0]
        checkpoint_count = connection.execute("SELECT COUNT(*) FROM malf_checkpoint").fetchone()[0]
        completed_queue_count = connection.execute(
            "SELECT COUNT(*) FROM malf_work_queue WHERE status = 'completed'"
        ).fetchone()[0]

    assert snapshot_count == 12
    assert checkpoint_count == 3
    assert completed_queue_count == 3


def test_malf_day_runner_filters_symbols_by_range_and_limit(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    rows = []
    for symbol in ("AAA", "BBB", "CCC", "DDD"):
        rows.extend(
            [
                (symbol, "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
                (symbol, "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
                (symbol, "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
                (symbol, "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ]
        )
    _write_day_source_bars(workspace / "data" / "base" / "market_base.duckdb", rows)

    summary = run_malf_day_build(start_symbol="BBB", end_symbol="DDD", symbol_limit=2)

    assert summary.segment_summary.start_symbol == "BBB"
    assert summary.segment_summary.end_symbol == "DDD"
    assert summary.segment_summary.symbol_limit == 2
    assert summary.segment_summary.full_universe is False
    assert summary.progress_summary.symbols_total == 2
    assert summary.progress_summary.symbols_seen == 2
    assert summary.progress_summary.symbols_completed == 2
    target_path = Path(summary.artifact_summary.active_build_path)
    with duckdb.connect(str(target_path), read_only=True) as connection:
        checkpoint_symbols = [
            row[0]
            for row in connection.execute("SELECT symbol FROM malf_checkpoint ORDER BY symbol").fetchall()
        ]

    assert checkpoint_symbols == ["BBB", "CCC"]


def test_malf_day_runner_full_universe_resumes_segmented_build_and_promotes(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    rows = []
    for symbol in ("AAA", "BBB", "CCC"):
        rows.extend(
            [
                (symbol, "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
                (symbol, "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
                (symbol, "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
                (symbol, "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ]
        )
    _write_day_source_bars(workspace / "data" / "base" / "market_base.duckdb", rows)

    first_summary = run_malf_day_build(symbol_limit=2)
    assert first_summary.artifact_summary.promoted_to_target is False
    assert first_summary.artifact_summary.active_build_path is not None

    second_summary = run_malf_day_build()

    assert second_summary.progress_summary.symbols_total == 3
    assert second_summary.progress_summary.symbols_completed == 3
    assert second_summary.checkpoint_summary.symbols_updated == 1
    assert second_summary.artifact_summary.promoted_to_target is True
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path), read_only=True) as connection:
        checkpoint_count = connection.execute("SELECT COUNT(*) FROM malf_checkpoint").fetchone()[0]

    assert checkpoint_count == 3


def test_malf_day_runner_resume_continues_after_partial_failure(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    source_path = workspace / "data" / "base" / "market_base.duckdb"
    _write_day_source_bars(
        source_path,
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ("BBB", "2026-01-02T00:00:00", 8.0, 8.8, 7.8, 8.4),
            ("BBB", "2026-01-03T00:00:00", 8.4, 9.2, 8.2, 9.0),
            ("BBB", "2026-01-04T00:00:00", 9.0, 9.1, 8.4, 8.6),
            ("BBB", "2026-01-05T00:00:00", 8.6, 8.7, 8.0, 8.1),
        ],
    )

    monkeypatch.setattr(malf_runner_module, "_WRITE_BATCH_SYMBOL_LIMIT", 1)
    original_run_malf_engine = malf_runner_module.run_malf_engine
    call_count = {"BBB": 0}

    def failing_engine(*, symbol, timeframe, bars):
        if symbol == "BBB" and call_count["BBB"] == 0:
            call_count["BBB"] += 1
            raise RuntimeError("synthetic interruption")
        return original_run_malf_engine(symbol=symbol, timeframe=timeframe, bars=bars)

    monkeypatch.setattr(malf_runner_module, "run_malf_engine", failing_engine)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        run_malf_day_build(symbol_limit=2)

    monkeypatch.setattr(malf_runner_module, "run_malf_engine", original_run_malf_engine)
    summary = run_malf_day_build(symbol_limit=2)

    assert summary.progress_summary.symbols_total == 2
    assert summary.progress_summary.symbols_completed == 2
    assert summary.checkpoint_summary.symbols_updated == 1
    target_path = Path(summary.artifact_summary.active_build_path)
    with duckdb.connect(str(target_path), read_only=True) as connection:
        checkpoint_symbols = [
            row[0]
            for row in connection.execute("SELECT symbol FROM malf_checkpoint ORDER BY symbol").fetchall()
        ]

    assert checkpoint_symbols == ["AAA", "BBB"]


def test_malf_day_runner_reports_progress_and_abandoned_build_artifacts(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
        ],
    )
    malf_root = workspace / "data" / "astock_lifespan_alpha" / "malf"
    malf_root.mkdir(parents=True, exist_ok=True)
    abandoned_path = malf_root / "malf_day.day-abandoned.building.duckdb"
    active_path = malf_root / "malf_day.day-active.building.duckdb"
    abandoned_path.touch()
    active_path.touch()
    os.utime(abandoned_path, (1, 1))
    os.utime(active_path, (2, 2))

    summary = run_malf_day_build(symbol_limit=1)

    assert summary.artifact_summary.active_build_path == str(active_path)
    assert str(abandoned_path) in summary.artifact_summary.abandoned_build_artifacts
    assert abandoned_path.exists()
    progress_path = Path(summary.progress_summary.progress_path)
    progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert progress_payload["symbols_seen"] == 1
    assert progress_payload["symbols_completed"] == 1
    assert progress_payload["estimated_remaining_symbols"] == 0
    assert progress_payload["ledger_rows_written"]["wave_rows"] >= 1


def test_malf_day_full_universe_prefers_complete_target_over_stale_building(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
        ],
    )
    first_summary = run_malf_day_build()
    assert first_summary.artifact_summary.active_build_path is None

    malf_root = workspace / "data" / "astock_lifespan_alpha" / "malf"
    stale_building_path = malf_root / "malf_day.day-stale.building.duckdb"
    stale_building_path.touch()
    os.utime(stale_building_path, (3, 3))

    second_summary = run_malf_day_build()

    assert second_summary.artifact_summary.active_build_path is None
    assert second_summary.artifact_summary.promoted_to_target is False
    assert str(stale_building_path) in second_summary.artifact_summary.abandoned_build_artifacts
    assert second_summary.checkpoint_summary.symbols_updated == 0
    assert stale_building_path.exists()


def test_malf_day_full_universe_ignores_checkpointed_stale_running_queue(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
        ],
    )
    run_malf_day_build()

    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO malf_run (
                run_id, timeframe, status, source_path, message,
                symbols_seen, symbols_completed
            )
            VALUES ('day-stale', 'day', 'running', 'stale-source.duckdb', 'stale run', 0, 0)
            """
        )
        connection.execute(
            """
            INSERT INTO malf_work_queue (
                queue_id, symbol, timeframe, status, source_bar_count, claimed_at, last_bar_dt
            )
            VALUES ('day-stale:AAA', 'AAA', 'day', 'running', 4, CURRENT_TIMESTAMP, TIMESTAMP '2026-01-05 00:00:00')
            """
        )

    summary = run_malf_day_build()

    assert summary.artifact_summary.active_build_path is None
    assert summary.artifact_summary.promoted_to_target is False
    assert summary.checkpoint_summary.symbols_updated == 0


def test_malf_day_full_universe_no_resume_forces_isolated_build_and_preserves_target_until_promotion(
    monkeypatch, tmp_path
):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
        ],
    )
    first_summary = run_malf_day_build()
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    baseline_run_id = first_summary.run_id
    promotion_calls: list[tuple[str, str, str]] = []

    def spy_promote(*, build_path: Path, target_path: Path, run_id: str) -> None:
        promotion_calls.append((str(build_path), str(target_path), run_id))
        with duckdb.connect(str(target_path), read_only=True) as target_connection:
            assert target_connection.execute(
                "SELECT COUNT(*) FROM malf_run WHERE run_id = ?",
                [run_id],
            ).fetchone()[0] == 0
            assert target_connection.execute(
                "SELECT COUNT(*) FROM malf_run WHERE run_id = ?",
                [baseline_run_id],
            ).fetchone()[0] == 1
        with duckdb.connect(str(build_path), read_only=True) as build_connection:
            assert build_connection.execute(
                "SELECT COUNT(*) FROM malf_run WHERE run_id = ?",
                [run_id],
            ).fetchone()[0] == 1

    monkeypatch.setattr(malf_runner_module, "_promote_rebuilt_database", spy_promote)
    summary = run_malf_day_build(resume=False)

    assert summary.artifact_summary.active_build_path is not None
    assert summary.artifact_summary.active_build_path.endswith(".building.duckdb")
    assert summary.artifact_summary.promoted_to_target is True
    assert promotion_calls
    with duckdb.connect(str(target_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM malf_run WHERE run_id = ?", [summary.run_id]).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM malf_run WHERE run_id = ?", [baseline_run_id]).fetchone()[0] == 1


def test_malf_day_runner_backfills_legacy_building_schema(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
        ],
    )
    building_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.day-legacy.building.duckdb"
    _write_legacy_malf_run(building_path)

    summary = run_malf_day_build(symbol_limit=1)

    assert summary.artifact_summary.active_build_path == str(building_path)
    assert summary.progress_summary.symbols_total == 1
    assert summary.progress_summary.symbols_completed == 1
    assert summary.progress_summary.progress_path is not None


def test_repair_malf_day_schema_repairs_target_and_building_artifacts(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    malf_root = workspace / "data" / "astock_lifespan_alpha" / "malf"
    target_path = malf_root / "malf_day.duckdb"
    active_path = malf_root / "malf_day.day-active.building.duckdb"
    abandoned_path = malf_root / "malf_day.day-abandoned.building.duckdb"
    for database_path in (target_path, active_path, abandoned_path):
        _write_legacy_malf_run(database_path)
    original_mtimes = {database_path: database_path.stat().st_mtime for database_path in (target_path, active_path, abandoned_path)}

    summary = repair_malf_day_schema(settings=default_settings(repo_root=workspace / "repo"))
    rerun_summary = repair_malf_day_schema(settings=default_settings(repo_root=workspace / "repo"))

    assert summary.runner_name == "repair_malf_day_schema"
    assert summary.status == "completed"
    assert summary.scanned_database_count == 3
    assert summary.repaired_database_count == 3
    assert all(database.after.compatible for database in summary.databases)
    assert all("symbols_total" in database.before.missing_columns for database in summary.databases)
    assert rerun_summary.status == "completed"
    assert rerun_summary.repaired_database_count == 0
    assert all(database.actions == () for database in rerun_summary.databases)
    assert {database_path: database_path.stat().st_mtime for database_path in original_mtimes} == original_mtimes


def test_recover_malf_day_formal_target_restores_baseline_rows_and_clears_running_bookkeeping(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    baseline_run_id = "day-fc56ff5e5441"
    polluted_run_id = "day-107059a919fc"
    _seed_polluted_malf_day_target(
        target_path=target_path,
        baseline_run_id=baseline_run_id,
        zero_row_completed_run_id="day-a1c965e1f7a9",
        polluted_run_id=polluted_run_id,
        stale_running_run_id="day-d696fdcd4774",
    )

    summary = recover_malf_day_formal_target(
        baseline_run_id=baseline_run_id,
        settings=default_settings(repo_root=workspace / "repo"),
    )

    assert summary.status == "completed"
    assert summary.resolved_baseline.run_id == baseline_run_id
    assert summary.recovered_running_run_count == 0
    assert summary.recovered_running_queue_count == 0
    assert Path(summary.quarantine_path).exists()
    with duckdb.connect(str(target_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM malf_run WHERE run_id = ?", [baseline_run_id]).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM malf_run").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM malf_state_snapshot WHERE run_id = ?", [baseline_run_id]).fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM malf_state_snapshot WHERE run_id = ?", [polluted_run_id]).fetchone()[0] == 0
        checkpoint_rows = connection.execute(
            "SELECT symbol, last_run_id FROM malf_checkpoint ORDER BY symbol"
        ).fetchall()
        assert checkpoint_rows == [("AAA", baseline_run_id), ("BBB", baseline_run_id)]
        assert connection.execute(
            "SELECT COUNT(*) FROM malf_work_queue WHERE status = 'running'"
        ).fetchone()[0] == 0


def test_recover_malf_day_formal_target_ignores_zero_row_completed_runs_when_resolving_baseline(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    baseline_run_id = "day-fc56ff5e5441"
    _seed_polluted_malf_day_target(
        target_path=target_path,
        baseline_run_id=baseline_run_id,
        zero_row_completed_run_id="day-a1c965e1f7a9",
        polluted_run_id="day-107059a919fc",
        stale_running_run_id="day-d696fdcd4774",
    )

    summary = recover_malf_day_formal_target(settings=default_settings(repo_root=workspace / "repo"))

    assert summary.status == "completed"
    assert summary.requested_baseline_run_id is None
    assert summary.resolved_baseline.run_id == baseline_run_id
    assert summary.resolved_baseline.state_snapshot_rows == 2
    with duckdb.connect(str(target_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM malf_run WHERE run_id = 'day-a1c965e1f7a9'").fetchone()[0] == 0


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


def _write_legacy_malf_run(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS malf_run")
        connection.execute(
            """
            CREATE TABLE malf_run (
                run_id TEXT PRIMARY KEY,
                timeframe TEXT NOT NULL,
                status TEXT NOT NULL,
                source_path TEXT,
                input_rows BIGINT NOT NULL DEFAULT 0,
                symbols_seen BIGINT NOT NULL DEFAULT 0,
                symbols_updated BIGINT NOT NULL DEFAULT 0,
                inserted_pivots BIGINT NOT NULL DEFAULT 0,
                inserted_waves BIGINT NOT NULL DEFAULT 0,
                inserted_state_snapshots BIGINT NOT NULL DEFAULT 0,
                inserted_wave_scale_snapshots BIGINT NOT NULL DEFAULT 0,
                inserted_wave_scale_profiles BIGINT NOT NULL DEFAULT 0,
                latest_bar_dt TIMESTAMP,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            INSERT INTO malf_run (
                run_id,
                timeframe,
                status,
                source_path,
                input_rows,
                symbols_seen,
                symbols_updated,
                message
            )
            VALUES ('day-legacy', 'day', 'running', 'legacy-source.duckdb', 0, 0, 0, 'legacy run')
            """
        )


def _append_day_source_bars(database_path: Path, rows: list[tuple[str, str, float, float, float, float]]) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.executemany(
            "INSERT INTO market_base_day VALUES (?, ?, ?, ?, ?, ?)",
            [(symbol, datetime.fromisoformat(bar_dt), open_price, high_price, low_price, close_price) for symbol, bar_dt, open_price, high_price, low_price, close_price in rows],
        )


def _seed_polluted_malf_day_target(
    *,
    target_path: Path,
    baseline_run_id: str,
    zero_row_completed_run_id: str,
    polluted_run_id: str,
    stale_running_run_id: str,
) -> None:
    initialize_malf_schema(target_path)
    with duckdb.connect(str(target_path)) as connection:
        connection.execute("DELETE FROM malf_wave_scale_profile")
        connection.execute("DELETE FROM malf_wave_scale_snapshot")
        connection.execute("DELETE FROM malf_state_snapshot")
        connection.execute("DELETE FROM malf_wave_ledger")
        connection.execute("DELETE FROM malf_pivot_ledger")
        connection.execute("DELETE FROM malf_checkpoint")
        connection.execute("DELETE FROM malf_work_queue")
        connection.execute("DELETE FROM malf_run")
        connection.executemany(
            """
            INSERT INTO malf_run (
                run_id,
                timeframe,
                status,
                source_path,
                input_rows,
                symbols_total,
                symbols_seen,
                symbols_completed,
                symbols_updated,
                inserted_pivots,
                inserted_waves,
                inserted_state_snapshots,
                inserted_wave_scale_snapshots,
                inserted_wave_scale_profiles,
                current_symbol,
                elapsed_seconds,
                estimated_remaining_symbols,
                latest_bar_dt,
                message,
                started_at,
                finished_at
            )
            VALUES (?, 'day', ?, 'seed-source.duckdb', 4, 2, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1.0, 0, TIMESTAMP '2026-01-05 00:00:00', ?, TIMESTAMP '2026-01-05 00:00:00', ?)
            """,
            [
                (baseline_run_id, "completed", 2, 2, 2, 2, 2, 2, 2, 2, "baseline run", datetime(2026, 1, 5, 0, 10)),
                (zero_row_completed_run_id, "completed", 2, 2, 0, 0, 0, 0, 0, 0, "zero row completed", datetime(2026, 1, 6, 0, 10)),
                (polluted_run_id, "interrupted", 2, 1, 1, 1, 1, 1, 1, 1, "polluted interrupted", datetime(2026, 1, 7, 0, 10)),
                (stale_running_run_id, "running", 1, 0, 0, 0, 0, 0, 0, 0, "stale running", None),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_work_queue (
                queue_id,
                symbol,
                timeframe,
                status,
                source_bar_count,
                requested_at,
                claimed_at,
                finished_at,
                last_bar_dt
            )
            VALUES (?, ?, 'day', ?, 2, TIMESTAMP '2026-01-05 00:00:00', TIMESTAMP '2026-01-05 00:00:00', ?, TIMESTAMP '2026-01-05 00:00:00')
            """,
            [
                (f"{baseline_run_id}:AAA", "AAA", "completed", datetime(2026, 1, 5, 0, 10)),
                (f"{baseline_run_id}:BBB", "BBB", "completed", datetime(2026, 1, 5, 0, 10)),
                (f"{polluted_run_id}:AAA", "AAA", "interrupted", datetime(2026, 1, 7, 0, 10)),
                (f"{stale_running_run_id}:BBB", "BBB", "running", None),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_pivot_ledger (
                pivot_nk,
                run_id,
                symbol,
                timeframe,
                wave_id,
                bar_dt,
                pivot_type,
                price
            )
            VALUES (?, ?, ?, 'day', ?, ?, ?, ?)
            """,
            [
                ("pivot-baseline-aaa", baseline_run_id, "AAA", "wave-baseline-aaa", datetime(2026, 1, 4), "hh", 12.0),
                ("pivot-baseline-bbb", baseline_run_id, "BBB", "wave-baseline-bbb", datetime(2026, 1, 4), "ll", 8.0),
                ("pivot-polluted-aaa", polluted_run_id, "AAA", "wave-polluted-aaa", datetime(2026, 1, 5), "break_down", 9.0),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_wave_ledger (
                wave_id,
                run_id,
                symbol,
                timeframe,
                direction,
                start_bar_dt,
                end_bar_dt,
                guard_bar_dt,
                guard_price,
                extreme_price,
                new_count,
                no_new_span,
                life_state
            )
            VALUES (?, ?, ?, 'day', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("wave-baseline-aaa", baseline_run_id, "AAA", "up", datetime(2026, 1, 2), datetime(2026, 1, 5), datetime(2026, 1, 4), 10.5, 12.0, 2, 1, "alive"),
                ("wave-baseline-bbb", baseline_run_id, "BBB", "down", datetime(2026, 1, 2), datetime(2026, 1, 5), datetime(2026, 1, 4), 8.5, 7.8, 2, 1, "alive"),
                ("wave-polluted-aaa", polluted_run_id, "AAA", "down", datetime(2026, 1, 2), datetime(2026, 1, 5), datetime(2026, 1, 5), 9.2, 8.9, 1, 2, "broken"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_state_snapshot (
                snapshot_nk,
                run_id,
                symbol,
                timeframe,
                bar_dt,
                wave_id,
                direction,
                guard_price,
                extreme_price,
                new_count,
                no_new_span,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone
            )
            VALUES (?, ?, ?, 'day', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("snapshot-baseline-aaa", baseline_run_id, "AAA", datetime(2026, 1, 5), "wave-baseline-aaa", "up", 10.5, 12.0, 2, 1, "alive", 0.8, 0.2, "mature_progress"),
                ("snapshot-baseline-bbb", baseline_run_id, "BBB", datetime(2026, 1, 5), "wave-baseline-bbb", "down", 8.5, 7.8, 2, 1, "alive", 0.7, 0.3, "mature_progress"),
                ("snapshot-polluted-aaa", polluted_run_id, "AAA", datetime(2026, 1, 5), "wave-polluted-aaa", "down", 9.2, 8.9, 1, 2, "broken", 0.4, 0.8, "weak_stagnation"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_wave_scale_snapshot (
                snapshot_nk,
                run_id,
                symbol,
                timeframe,
                bar_dt,
                direction,
                wave_id,
                new_count,
                no_new_span,
                life_state,
                update_rank,
                stagnation_rank,
                wave_position_zone
            )
            VALUES (?, ?, ?, 'day', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("scale-snapshot-baseline-aaa", baseline_run_id, "AAA", datetime(2026, 1, 5), "up", "wave-baseline-aaa", 2, 1, "alive", 0.8, 0.2, "mature_progress"),
                ("scale-snapshot-baseline-bbb", baseline_run_id, "BBB", datetime(2026, 1, 5), "down", "wave-baseline-bbb", 2, 1, "alive", 0.7, 0.3, "mature_progress"),
                ("scale-snapshot-polluted-aaa", polluted_run_id, "AAA", datetime(2026, 1, 5), "down", "wave-polluted-aaa", 1, 2, "broken", 0.4, 0.8, "weak_stagnation"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_wave_scale_profile (
                profile_nk,
                run_id,
                symbol,
                timeframe,
                direction,
                wave_id,
                sample_size,
                new_count,
                no_new_span,
                update_rank,
                stagnation_rank,
                wave_position_zone
            )
            VALUES (?, ?, ?, 'day', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("profile-baseline-aaa", baseline_run_id, "AAA", "up", "wave-baseline-aaa", 10, 2, 1, 0.8, 0.2, "mature_progress"),
                ("profile-baseline-bbb", baseline_run_id, "BBB", "down", "wave-baseline-bbb", 10, 2, 1, 0.7, 0.3, "mature_progress"),
                ("profile-polluted-aaa", polluted_run_id, "AAA", "down", "wave-polluted-aaa", 5, 1, 2, 0.4, 0.8, "weak_stagnation"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO malf_checkpoint (symbol, timeframe, last_bar_dt, last_run_id, updated_at)
            VALUES (?, 'day', ?, ?, TIMESTAMP '2026-01-07 00:00:00')
            """,
            [
                ("AAA", datetime(2026, 1, 5), polluted_run_id),
                ("BBB", datetime(2026, 1, 5), baseline_run_id),
            ],
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
