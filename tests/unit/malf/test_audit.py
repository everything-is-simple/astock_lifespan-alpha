from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import default_settings
import astock_lifespan_alpha.malf.audit as audit_module
from astock_lifespan_alpha.malf import run_malf_day_build
from astock_lifespan_alpha.malf.audit import audit_malf_day_semantics


def test_malf_day_semantic_audit_writes_reports_and_artifacts(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    monkeypatch.setattr(audit_module, "_MATERIALIZE_SYMBOL_CHUNK_SIZE", 1)
    _write_day_source_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02T00:00:00", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-03T00:00:00", 10.8, 12.2, 10.1, 12.0),
            ("AAA", "2026-01-04T00:00:00", 12.0, 12.1, 11.2, 11.5),
            ("AAA", "2026-01-05T00:00:00", 11.5, 11.6, 10.0, 10.2),
            ("AAA", "2026-01-06T00:00:00", 10.2, 10.3, 9.6, 9.8),
            ("AAA", "2026-01-07T00:00:00", 9.8, 10.4, 9.7, 10.3),
            ("AAA", "2026-01-08T00:00:00", 10.3, 10.9, 10.2, 10.8),
            ("BBB", "2026-01-02T00:00:00", 8.0, 8.6, 7.8, 8.4),
            ("BBB", "2026-01-03T00:00:00", 8.4, 8.5, 7.9, 8.0),
            ("BBB", "2026-01-04T00:00:00", 8.0, 8.1, 7.4, 7.5),
            ("BBB", "2026-01-05T00:00:00", 7.5, 7.9, 7.3, 7.8),
            ("BBB", "2026-01-06T00:00:00", 7.8, 8.4, 7.7, 8.3),
            ("BBB", "2026-01-07T00:00:00", 8.3, 8.8, 8.2, 8.6),
        ],
    )
    run_summary = run_malf_day_build(settings=default_settings(repo_root=workspace / "repo"))

    report = audit_malf_day_semantics(
        settings=default_settings(repo_root=workspace / "repo"),
        run_id=run_summary.run_id,
        sample_count=4,
    )

    assert report.runner_name == "audit_malf_day_semantics"
    assert report.target_run_id == run_summary.run_id
    assert report.target_snapshot_rows > 0
    assert report.target_wave_rows > 0
    assert Path(report.summary_json_path).exists()
    assert Path(report.summary_markdown_path).exists()
    assert Path(report.artifact_database_path).exists()
    assert len(report.table_artifacts) == 4
    assert len(report.sample_windows) >= 1
    assert all(window.plot_path is not None for window in report.sample_windows)
    payload = json.loads(Path(report.summary_json_path).read_text(encoding="utf-8"))
    assert payload["target_run_id"] == run_summary.run_id
    assert payload["table_artifacts"][0]["table_name"] == "wave_summary"
    zone_coverage = next(
        item for item in payload["soft_observations"] if item["observation_name"] == "zone_coverage"
    )
    markdown = Path(report.summary_markdown_path).read_text(encoding="utf-8")
    assert zone_coverage["threshold"] == "expected 4 distinct zones in state_snapshot_sample"
    assert zone_coverage["note"] == "按 state_snapshot_sample 统计的 sample coverage，不代表全量 malf_state_snapshot。"
    assert "state_snapshot_sample" in markdown
    assert "sample coverage" in markdown
    with duckdb.connect(str(report.artifact_database_path), read_only=True) as connection:
        table_names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {
            "wave_summary",
            "state_snapshot_sample",
            "break_events",
            "reborn_windows",
            "sample_windows",
        }.issubset(table_names)


def test_malf_day_semantic_audit_tracks_stale_running_rows_without_scoring_them(monkeypatch, tmp_path):
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
    run_summary = run_malf_day_build(settings=default_settings(repo_root=workspace / "repo"))
    target_path = workspace / "data" / "astock_lifespan_alpha" / "malf" / "malf_day.duckdb"
    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO malf_run (
                run_id,
                timeframe,
                status,
                source_path,
                symbols_total,
                symbols_seen,
                symbols_completed,
                message
            )
            VALUES ('day-stale-running', 'day', 'running', 'stale-source.duckdb', 100, 10, 5, 'stale running row')
            """
        )
        connection.execute(
            """
            INSERT INTO malf_work_queue (
                queue_id,
                symbol,
                timeframe,
                status,
                source_bar_count,
                claimed_at,
                last_bar_dt
            )
            VALUES ('day-stale-running:AAA', 'AAA', 'day', 'running', 4, CURRENT_TIMESTAMP, TIMESTAMP '2026-01-05 00:00:00')
            """
        )

    report = audit_malf_day_semantics(
        settings=default_settings(repo_root=workspace / "repo"),
        run_id=run_summary.run_id,
        sample_count=2,
    )

    stale_run_ids = {item.run_id for item in report.stale_run_summaries}
    assert "day-stale-running" in stale_run_ids
    assert report.target_run_id == run_summary.run_id
    assert report.running_queue_count >= 1


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
            [
                (symbol, datetime.fromisoformat(bar_dt), open_price, high_price, low_price, close_price)
                for symbol, bar_dt, open_price, high_price, low_price, close_price in rows
            ],
        )
