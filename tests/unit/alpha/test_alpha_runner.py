from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.alpha import (
    run_alpha_bof_build,
    run_alpha_bpb_build,
    run_alpha_cpb_build,
    run_alpha_pb_build,
    run_alpha_signal_build,
    run_alpha_tst_build,
)
from astock_lifespan_alpha.alpha.schema import SIGNAL_TABLES, TRIGGER_TABLES


def test_alpha_runners_initialize_trigger_and_signal_schema(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_malf_day_snapshot(workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb")

    trigger_summaries = [
        run_alpha_bof_build(),
        run_alpha_tst_build(),
        run_alpha_pb_build(),
        run_alpha_cpb_build(),
        run_alpha_bpb_build(),
    ]
    signal_summary = run_alpha_signal_build()

    assert all(summary.status == "completed" for summary in trigger_summaries)
    assert signal_summary.status == "completed"

    for target_path in (
        workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_bof.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_tst.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_pb.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_cpb.duckdb",
        workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_bpb.duckdb",
    ):
        with duckdb.connect(str(target_path), read_only=True) as connection:
            table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert set(TRIGGER_TABLES).issubset(table_names)

    with duckdb.connect(
        str(workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb"), read_only=True
    ) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    assert set(SIGNAL_TABLES).issubset(table_names)


def test_alpha_trigger_runners_and_alpha_signal_materialize_outputs(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_malf_day_snapshot(workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb")

    bof_summary = run_alpha_bof_build()
    tst_summary = run_alpha_tst_build()
    pb_summary = run_alpha_pb_build()
    cpb_summary = run_alpha_cpb_build()
    bpb_summary = run_alpha_bpb_build()
    signal_summary = run_alpha_signal_build()

    assert bof_summary.materialization_counts["event_rows"] >= 1
    assert tst_summary.materialization_counts["event_rows"] >= 1
    assert pb_summary.materialization_counts["event_rows"] >= 1
    assert cpb_summary.materialization_counts["event_rows"] >= 1
    assert bpb_summary.materialization_counts["event_rows"] >= 1
    assert signal_summary.materialization_counts["signal_rows"] >= 5

    signal_path = workspace / "data" / "astock_lifespan_alpha" / "alpha" / "alpha_signal.duckdb"
    with duckdb.connect(str(signal_path), read_only=True) as connection:
        signal_types = {
            row[0]
            for row in connection.execute("SELECT DISTINCT trigger_type FROM alpha_signal").fetchall()
        }
        field_row = connection.execute(
            """
            SELECT signal_nk, source_trigger_db, source_trigger_event_nk, wave_id, direction, formal_signal_status
            FROM alpha_signal
            ORDER BY signal_date, trigger_type
            LIMIT 1
            """
        ).fetchone()

    assert signal_types == {"bof", "tst", "pb", "cpb", "bpb"}
    assert all(field_row)


def test_alpha_runner_checkpoint_skips_unchanged_source(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_market_base_day(workspace / "data" / "base" / "market_base.duckdb")
    _write_malf_day_snapshot(workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb")

    first_bof = run_alpha_bof_build()
    second_bof = run_alpha_bof_build()
    first_signal = run_alpha_signal_build()
    second_signal = run_alpha_signal_build()

    assert first_bof.materialization_counts["event_rows"] >= 1
    assert second_bof.materialization_counts["event_rows"] == 0
    assert second_bof.checkpoint_summary.work_units_updated == 0
    assert first_signal.materialization_counts["signal_rows"] >= 1
    assert second_signal.materialization_counts["signal_rows"] == 0
    assert second_signal.checkpoint_summary.work_units_updated == 0


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
        ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
        ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 11.2),
        ("AAA", "2026-01-04T00:00:00", 12.0, 12.4, 11.8, 12.3),
        ("AAA", "2026-01-05T00:00:00", 12.4, 12.5, 11.9, 12.0),
        ("AAA", "2026-01-06T00:00:00", 12.0, 12.3, 11.95, 12.2),
        ("AAA", "2026-01-07T00:00:00", 12.0, 12.1, 11.4, 11.5),
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
                (symbol, datetime.fromisoformat(bar_dt), open_price, high_price, low_price, close_price)
                for symbol, bar_dt, open_price, high_price, low_price, close_price in rows
            ],
        )


def _write_malf_day_snapshot(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        ("AAA", date(2026, 1, 2), "up", "wave-1", 0, 0, "alive", 45.0, 20.0, "early_progress"),
        ("AAA", date(2026, 1, 3), "up", "wave-1", 1, 0, "alive", 65.0, 20.0, "early_progress"),
        ("AAA", date(2026, 1, 4), "up", "wave-1", 1, 1, "alive", 70.0, 35.0, "mature_progress"),
        ("AAA", date(2026, 1, 5), "up", "wave-1", 1, 2, "alive", 60.0, 55.0, "mature_stagnation"),
        ("AAA", date(2026, 1, 6), "up", "wave-1", 1, 2, "alive", 62.0, 58.0, "mature_progress"),
        ("AAA", date(2026, 1, 7), "down", "wave-2", 1, 1, "alive", 40.0, 65.0, "weak_stagnation"),
    ]
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS malf_wave_scale_snapshot")
        connection.execute(
            """
            CREATE TABLE malf_wave_scale_snapshot (
                symbol TEXT,
                bar_dt TIMESTAMP,
                direction TEXT,
                wave_id TEXT,
                new_count BIGINT,
                no_new_span BIGINT,
                life_state TEXT,
                update_rank DOUBLE,
                stagnation_rank DOUBLE,
                wave_position_zone TEXT
            )
            """
        )
        connection.executemany(
            "INSERT INTO malf_wave_scale_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    symbol,
                    datetime.combine(signal_date, datetime.min.time()),
                    direction,
                    wave_id,
                    new_count,
                    no_new_span,
                    life_state,
                    update_rank,
                    stagnation_rank,
                    wave_position_zone,
                )
                for symbol, signal_date, direction, wave_id, new_count, no_new_span, life_state, update_rank, stagnation_rank, wave_position_zone in rows
            ],
        )
