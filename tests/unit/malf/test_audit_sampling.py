from __future__ import annotations

from datetime import datetime

import duckdb

from astock_lifespan_alpha.malf.audit import _select_sample_windows


def test_select_sample_windows_prioritizes_wave_position_zone_coverage():
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE SCHEMA live")
        connection.execute(
            """
            CREATE TABLE wave_summary (
                symbol TEXT,
                timeframe TEXT,
                wave_id TEXT,
                direction TEXT,
                start_bar_dt TIMESTAMP,
                end_bar_dt TIMESTAMP,
                bar_count INTEGER,
                new_count INTEGER,
                no_new_span INTEGER,
                reborn_bar_count INTEGER
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE live.malf_state_snapshot (
                symbol TEXT,
                timeframe TEXT,
                wave_id TEXT,
                wave_position_zone TEXT,
                update_rank DOUBLE,
                stagnation_rank DOUBLE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE break_events (
                symbol TEXT,
                wave_id TEXT,
                direction TEXT,
                start_bar_dt TIMESTAMP,
                end_bar_dt TIMESTAMP,
                bar_count INTEGER,
                new_count INTEGER,
                no_new_span INTEGER,
                reborn_bar_count INTEGER
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE reborn_windows (
                symbol TEXT,
                wave_id TEXT,
                direction TEXT,
                start_bar_dt TIMESTAMP,
                end_bar_dt TIMESTAMP,
                bar_count INTEGER,
                new_count INTEGER,
                no_new_span INTEGER,
                reborn_bar_count INTEGER
            )
            """
        )
        zones = [
            ("AAA", "wave-early", "up", "early_progress", 10.0, 10.0),
            ("BBB", "wave-mature", "up", "mature_progress", 90.0, 10.0),
            ("CCC", "wave-mature-stag", "down", "mature_stagnation", 90.0, 90.0),
            ("DDD", "wave-weak-stag", "down", "weak_stagnation", 10.0, 90.0),
        ]
        for index, (symbol, wave_id, direction, zone, update_rank, stagnation_rank) in enumerate(zones):
            start_dt = datetime(2026, 1, index + 1)
            end_dt = datetime(2026, 1, index + 2)
            connection.execute(
                "INSERT INTO wave_summary VALUES (?, 'day', ?, ?, ?, ?, 8, 3, 2, 1)",
                [symbol, wave_id, direction, start_dt, end_dt],
            )
            connection.execute(
                "INSERT INTO live.malf_state_snapshot VALUES (?, 'day', ?, ?, ?, ?)",
                [symbol, wave_id, zone, update_rank, stagnation_rank],
            )

        windows = _select_sample_windows(audit_connection=connection, sample_count=4)

    assert [window.category for window in windows] == [
        "zone_coverage_early_progress",
        "zone_coverage_mature_progress",
        "zone_coverage_mature_stagnation",
        "zone_coverage_weak_stagnation",
    ]
