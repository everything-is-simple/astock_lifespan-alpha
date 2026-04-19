from __future__ import annotations

import json
from pathlib import Path

from astock_lifespan_alpha.core.paths import default_settings
from astock_lifespan_alpha.malf.diagnostics import profile_malf_day_real_data


def test_malf_day_real_data_diagnostics_writes_report(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_adjusted_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02", "none", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-02", "backward", 20.0, 21.0, 19.5, 20.8),
            ("AAA", "2026-01-02", "forward", 30.0, 31.0, 29.8, 30.6),
            ("AAA", "2026-01-03", "backward", 20.8, 22.2, 20.1, 22.0),
            ("BBB", "2026-01-02", "backward", 20.0, 20.5, 19.8, 20.1),
            ("BBB", "2026-01-03", "backward", 20.1, 21.0, 19.9, 20.8),
        ],
    )

    report = profile_malf_day_real_data(settings=default_settings(repo_root=workspace / "repo"), top_n=2, symbol_limit=2)

    assert report.runner_name == "profile_malf_day_real_data"
    assert report.source is not None
    assert report.source.table_name == "stock_daily_adjusted"
    assert report.source.selected_adjust_method == "backward"
    assert report.source.row_count == 6
    assert report.source.symbol_count == 2
    assert report.source.duplicate_symbol_trade_date_groups_before_filter == 1
    assert report.source.duplicate_symbol_trade_date_groups_after_filter == 0
    assert report.profiled_symbol_count == 2
    assert report.bottleneck_stage in {"source_load_timing", "engine_timing", "write_timing"}
    assert len(report.top_slow_symbols) == 2
    assert Path(report.report_json_path).exists()
    assert Path(report.report_markdown_path).exists()

    payload = json.loads(Path(report.report_json_path).read_text(encoding="utf-8"))
    assert payload["source"]["table_name"] == "stock_daily_adjusted"
    assert payload["source"]["selected_adjust_method"] == "backward"
    assert payload["timings"]["source_load_seconds"] >= 0.0
    assert payload["timings"]["write_timing_summary"]["write_seconds"] >= 0.0
    assert payload["timings"]["write_timing_summary"]["delete_old_rows_seconds"] >= 0.0
    assert payload["timings"]["write_timing_summary"]["insert_ledgers_seconds"] >= 0.0
    assert payload["timings"]["write_timing_summary"]["checkpoint_seconds"] >= 0.0
    assert payload["timings"]["write_timing_summary"]["queue_update_seconds"] >= 0.0
    assert payload["top_slow_symbols"][0]["write_timing_summary"]["write_seconds"] >= 0.0
    markdown = Path(report.report_markdown_path).read_text(encoding="utf-8")
    assert "delete old rows timing" in markdown
    assert "insert ledgers timing" in markdown
    assert "checkpoint timing" in markdown
    assert "queue update timing" in markdown


def test_malf_day_real_data_diagnostics_records_duplicate_backward_contract_violation(monkeypatch, tmp_path):
    workspace = _configure_workspace(monkeypatch=monkeypatch, tmp_path=tmp_path)
    _write_stock_adjusted_bars(
        workspace / "data" / "base" / "market_base.duckdb",
        [
            ("AAA", "2026-01-02", "backward", 10.0, 11.0, 9.5, 10.8),
            ("AAA", "2026-01-02", "backward", 10.2, 11.2, 9.7, 10.9),
            ("AAA", "2026-01-03", "backward", 10.8, 12.2, 10.1, 12.0),
        ],
    )

    report = profile_malf_day_real_data(settings=default_settings(repo_root=workspace / "repo"), top_n=1, symbol_limit=1)

    assert report.source is not None
    assert report.source.duplicate_symbol_trade_date_groups_after_filter == 1
    assert "Source contract violations remain after adjust_method=backward formalization" in report.message
    assert report.top_slow_symbols[0].error is not None


def _configure_workspace(*, monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)
    return workspace


def _write_stock_adjusted_bars(database_path: Path, rows: list[tuple[str, str, str, float, float, float, float]]) -> None:
    import duckdb

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
