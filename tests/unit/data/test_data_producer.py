from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.data import (
    DataProducerSafetyError,
    audit_data_source_fact_freeze,
    market_base_timeframe_ledger_path,
    raw_market_timeframe_ledger_path,
    run_market_base_build,
    run_tdx_stock_raw_ingest,
)


def test_timeframe_ledger_paths_route_to_isolated_raw_and_base_roots(tmp_path: Path) -> None:
    target_root = tmp_path / "isolated-data"

    assert raw_market_timeframe_ledger_path(target_root, timeframe="day") == target_root / "raw" / "raw_market.duckdb"
    assert raw_market_timeframe_ledger_path(target_root, timeframe="week") == target_root / "raw" / "raw_market_week.duckdb"
    assert raw_market_timeframe_ledger_path(target_root, timeframe="month") == target_root / "raw" / "raw_market_month.duckdb"
    assert market_base_timeframe_ledger_path(target_root, timeframe="day") == target_root / "base" / "market_base.duckdb"
    assert market_base_timeframe_ledger_path(target_root, timeframe="week") == target_root / "base" / "market_base_week.duckdb"
    assert market_base_timeframe_ledger_path(target_root, timeframe="month") == target_root / "base" / "market_base_month.duckdb"


def test_producer_rejects_existing_source_fact_root_by_default(tmp_path: Path) -> None:
    settings = _settings(tmp_path / "official")
    source_root = _write_tdx_fixture(tmp_path / "tdx")

    with pytest.raises(DataProducerSafetyError, match="Refusing to write to source fact root"):
        run_tdx_stock_raw_ingest(settings=settings, source_root=source_root, target_data_root=settings.data_root)

    with pytest.raises(DataProducerSafetyError, match="Refusing to write to source fact root"):
        run_market_base_build(settings=settings, target_data_root=settings.data_root)


def test_tdx_raw_ingest_writes_isolated_raw_ledger_and_skips_unchanged_files(tmp_path: Path) -> None:
    settings = _settings(tmp_path / "official")
    source_root = _write_tdx_fixture(tmp_path / "tdx")
    target_root = tmp_path / "isolated"

    first = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        target_data_root=target_root,
        run_id="raw-test-1",
    )
    second = run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        target_data_root=target_root,
        run_id="raw-test-2",
    )

    assert first.candidate_file_count == 1
    assert first.ingested_file_count == 1
    assert first.bar_inserted_count == 2
    assert second.skipped_unchanged_file_count == 1
    raw_path = raw_market_timeframe_ledger_path(target_root, timeframe="day")
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM stock_daily_bar").fetchone()[0]
        dirty_rows = connection.execute(
            "SELECT code, dirty_status FROM base_dirty_instrument ORDER BY code"
        ).fetchall()

    assert row_count == 2
    assert dirty_rows == [("600000.SH", "pending")]
    assert not settings.source_databases.raw_market.exists()


def test_market_base_build_materializes_isolated_base_and_consumes_dirty_queue(tmp_path: Path) -> None:
    settings = _settings(tmp_path / "official")
    source_root = _write_tdx_fixture(tmp_path / "tdx")
    target_root = tmp_path / "isolated"
    run_tdx_stock_raw_ingest(
        settings=settings,
        source_root=source_root,
        target_data_root=target_root,
        run_id="raw-test",
    )
    base_path = market_base_timeframe_ledger_path(target_root, timeframe="day")
    base_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(base_path)) as connection:
        connection.execute(
            """
            CREATE TABLE stock_daily_adjusted (
                daily_bar_nk TEXT,
                code TEXT,
                name TEXT,
                timeframe TEXT,
                trade_date DATE,
                adjust_method TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                source_bar_nk TEXT,
                first_seen_run_id TEXT,
                last_materialized_run_id TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            INSERT INTO stock_daily_adjusted
            VALUES (
                'legacy|1990-01-01|backward', 'LEGACY', 'legacy', 'day', DATE '1990-01-01',
                'backward', 1, 1, 1, 1, 1, 1, 'legacy-raw', 'legacy-run', 'legacy-run',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        )

    summary = run_market_base_build(
        settings=settings,
        target_data_root=target_root,
        instruments=("600000.SH",),
        run_id="base-test",
    )

    assert summary.source_row_count == 2
    assert summary.inserted_count == 2
    assert summary.consumed_dirty_count == 1
    with duckdb.connect(str(base_path), read_only=True) as connection:
        rows = connection.execute(
            "SELECT code, trade_date, close FROM stock_daily_adjusted ORDER BY code, trade_date"
        ).fetchall()
        dirty_rows = connection.execute(
            "SELECT code, dirty_status, last_consumed_run_id FROM base_dirty_instrument ORDER BY code"
        ).fetchall()

    assert ("LEGACY", "1990-01-01", 1.0) in [(code, str(trade_date), close) for code, trade_date, close in rows]
    assert ("600000.SH", "2026-04-10", 10.7) in [(code, str(trade_date), close) for code, trade_date, close in rows]
    assert dirty_rows == [("600000.SH", "consumed", "base-test")]
    assert not settings.source_databases.market_base.exists()


def test_audit_data_source_fact_freeze_uses_read_only_connections(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path / "official")
    _write_source_fact_fixture(settings)
    import astock_lifespan_alpha.data.audit as audit_module

    real_connect = audit_module.duckdb.connect
    read_only_values: list[bool | None] = []

    def tracking_connect(*args, **kwargs):
        read_only_values.append(kwargs.get("read_only"))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(audit_module.duckdb, "connect", tracking_connect)

    summary = audit_data_source_fact_freeze(settings=settings)

    assert read_only_values
    assert set(read_only_values) == {True}
    assert summary.tables["market_base"].row_count == 1
    assert summary.tables["raw_market"].symbol_count == 2
    assert summary.raw_base_code_delta["raw_only_codes"] == ("000002.SZ",)
    assert summary.tables["market_base"].backward_duplicate_groups == 0


def _settings(data_root: Path) -> WorkspaceRoots:
    root = data_root.parent
    return WorkspaceRoots(
        repo_root=root / "repo",
        data_root=data_root,
        report_root=root / "report",
        temp_root=root / "temp",
        validated_root=root / "validated",
    )


def _write_tdx_fixture(root: Path) -> Path:
    source_dir = root / "stock-day" / "Backward-Adjusted"
    source_dir.mkdir(parents=True)
    (source_dir / "SH#600000.txt").write_text(
        "\n".join(
            [
                "600000 浦发银行 日线 前复权",
                "日期\t开盘\t最高\t最低\t收盘\t成交量\t成交额",
                "2026/04/09\t10.00\t10.50\t9.80\t10.20\t1000\t2000",
                "2026/04/10\t10.20\t10.80\t10.10\t10.70\t1100\t2200",
                "数据来源: 通达信",
            ]
        ),
        encoding="gbk",
    )
    return root


def _write_source_fact_fixture(settings: WorkspaceRoots) -> None:
    table_specs = {
        settings.source_databases.market_base: ("stock_daily_adjusted", [("000001.SZ", "2026-04-10")]),
        settings.source_databases.market_base_week: ("stock_weekly_adjusted", [("000001.SZ", "2026-04-10")]),
        settings.source_databases.market_base_month: ("stock_monthly_adjusted", [("000001.SZ", "2026-04-10")]),
        settings.source_databases.raw_market: (
            "stock_daily_bar",
            [("000001.SZ", "2026-04-10"), ("000002.SZ", "2026-04-10")],
        ),
        settings.source_databases.raw_market_week: ("stock_weekly_bar", [("000001.SZ", "2026-04-10")]),
        settings.source_databases.raw_market_month: ("stock_monthly_bar", [("000001.SZ", "2026-04-10")]),
    }
    for database_path, (table_name, rows) in table_specs.items():
        database_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(
                f"""
                CREATE TABLE {table_name} (
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
                f"INSERT INTO {table_name} VALUES (?, ?, 'backward', 1, 1, 1, 1)",
                rows,
            )
