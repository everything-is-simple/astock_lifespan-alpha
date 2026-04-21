"""DuckDB schema initialization for stage-six system readout ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


SYSTEM_TABLES = (
    "system_run",
    "system_work_queue",
    "system_checkpoint",
    "system_trade_readout",
    "system_portfolio_trade_summary",
)


def initialize_system_schema(database_path: Path) -> None:
    """Create the formal system schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                source_trade_path TEXT,
                readout_rows BIGINT NOT NULL DEFAULT 0,
                summary_rows BIGINT NOT NULL DEFAULT 0,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_work_queue (
                queue_id TEXT PRIMARY KEY,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                source_row_count BIGINT NOT NULL DEFAULT 0,
                latest_execution_trade_date DATE,
                source_fingerprint TEXT,
                requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_checkpoint (
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                latest_execution_trade_date DATE,
                last_source_fingerprint TEXT,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (portfolio_id, symbol)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_trade_readout (
                system_readout_nk TEXT PRIMARY KEY,
                order_intent_nk TEXT NOT NULL,
                order_execution_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                reference_trade_date DATE,
                planned_trade_date DATE,
                execution_trade_date DATE,
                position_action_decision TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                execution_status TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                admitted_weight DOUBLE NOT NULL,
                execution_weight DOUBLE NOT NULL,
                executed_weight DOUBLE NOT NULL,
                execution_price DOUBLE,
                blocking_reason_code TEXT,
                source_price_line TEXT NOT NULL,
                system_contract_version TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_portfolio_trade_summary (
                portfolio_id TEXT PRIMARY KEY,
                execution_count BIGINT NOT NULL DEFAULT 0,
                filled_count BIGINT NOT NULL DEFAULT 0,
                rejected_count BIGINT NOT NULL DEFAULT 0,
                symbol_count BIGINT NOT NULL DEFAULT 0,
                gross_executed_weight DOUBLE NOT NULL DEFAULT 0,
                latest_execution_trade_date DATE,
                system_contract_version TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_system_trade_readout_portfolio ON system_trade_readout(portfolio_id, execution_trade_date)"
        )
