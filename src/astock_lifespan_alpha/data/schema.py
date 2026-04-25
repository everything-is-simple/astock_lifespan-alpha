"""DuckDB schema bootstrap for isolated stock data producer ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb

from astock_lifespan_alpha.data.ledger_timeframe import (
    market_base_timeframe_ledger_path,
    normalize_timeframe,
    raw_market_timeframe_ledger_path,
)

RAW_STOCK_TABLE_BY_TIMEFRAME = {
    "day": "stock_daily_bar",
    "week": "stock_weekly_bar",
    "month": "stock_monthly_bar",
}
MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME = {
    "day": "stock_daily_adjusted",
    "week": "stock_weekly_adjusted",
    "month": "stock_monthly_adjusted",
}


def initialize_raw_market_schema(target_data_root: Path | str, *, timeframe: str = "day") -> Path:
    normalized = normalize_timeframe(timeframe)
    path = raw_market_timeframe_ledger_path(target_data_root, timeframe=normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as connection:
        _create_raw_schema(connection, raw_table=RAW_STOCK_TABLE_BY_TIMEFRAME[normalized])
    return path


def initialize_market_base_schema(target_data_root: Path | str, *, timeframe: str = "day") -> Path:
    normalized = normalize_timeframe(timeframe)
    path = market_base_timeframe_ledger_path(target_data_root, timeframe=normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as connection:
        _create_market_base_schema(connection, market_table=MARKET_BASE_STOCK_TABLE_BY_TIMEFRAME[normalized])
    return path


def _create_raw_schema(connection: duckdb.DuckDBPyConnection, *, raw_table: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {raw_table} (
            bar_nk TEXT PRIMARY KEY,
            source_file_nk TEXT,
            asset_type TEXT,
            code TEXT,
            name TEXT,
            trade_date DATE,
            adjust_method TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            source_path TEXT,
            source_mtime_utc TIMESTAMP,
            first_seen_run_id TEXT,
            last_ingested_run_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            timeframe TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_file_registry (
            file_nk TEXT PRIMARY KEY,
            timeframe TEXT,
            adjust_method TEXT,
            code TEXT,
            name TEXT,
            source_path TEXT,
            source_size_bytes BIGINT,
            source_mtime_utc TIMESTAMP,
            source_content_hash TEXT,
            source_line_count BIGINT,
            last_ingested_run_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_ingest_run (
            run_id TEXT PRIMARY KEY,
            status TEXT,
            timeframe TEXT,
            adjust_method TEXT,
            source_root TEXT,
            candidate_file_count BIGINT,
            processed_file_count BIGINT,
            message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_ingest_file (
            run_id TEXT,
            file_nk TEXT,
            code TEXT,
            source_path TEXT,
            action TEXT,
            row_count BIGINT,
            error_message TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _create_dirty_table(connection)


def _create_market_base_schema(connection: duckdb.DuckDBPyConnection, *, market_table: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {market_table} (
            daily_bar_nk TEXT PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS base_build_run (
            run_id TEXT PRIMARY KEY,
            status TEXT,
            timeframe TEXT,
            adjust_method TEXT,
            source_row_count BIGINT,
            inserted_count BIGINT,
            reused_count BIGINT,
            rematerialized_count BIGINT,
            consumed_dirty_count BIGINT,
            message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        )
        """
    )
    _create_dirty_table(connection)


def _create_dirty_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS base_dirty_instrument (
            dirty_nk TEXT PRIMARY KEY,
            asset_type TEXT,
            timeframe TEXT,
            code TEXT,
            adjust_method TEXT,
            dirty_reason TEXT,
            source_run_id TEXT,
            source_file_nk TEXT,
            dirty_status TEXT,
            last_consumed_run_id TEXT,
            last_marked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            consumed_at TIMESTAMP
        )
        """
    )
