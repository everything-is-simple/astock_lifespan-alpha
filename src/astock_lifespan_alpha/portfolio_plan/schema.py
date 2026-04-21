"""DuckDB schema initialization for stage-four portfolio plan ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


PORTFOLIO_PLAN_TABLES = (
    "portfolio_plan_run",
    "portfolio_plan_work_queue",
    "portfolio_plan_checkpoint",
    "portfolio_plan_snapshot",
    "portfolio_plan_run_snapshot",
)


def initialize_portfolio_plan_schema(database_path: Path) -> None:
    """Create the formal portfolio_plan schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_plan_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                source_position_path TEXT,
                bounded_candidate_count BIGINT NOT NULL DEFAULT 0,
                admitted_count BIGINT NOT NULL DEFAULT 0,
                blocked_count BIGINT NOT NULL DEFAULT 0,
                trimmed_count BIGINT NOT NULL DEFAULT 0,
                portfolio_gross_cap_weight DOUBLE NOT NULL,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_plan_work_queue (
                queue_id TEXT PRIMARY KEY,
                portfolio_id TEXT NOT NULL,
                status TEXT NOT NULL,
                source_row_count BIGINT NOT NULL DEFAULT 0,
                last_reference_trade_date DATE,
                source_fingerprint TEXT,
                requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_plan_checkpoint (
                portfolio_id TEXT PRIMARY KEY,
                last_reference_trade_date DATE,
                last_source_fingerprint TEXT,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_plan_snapshot (
                plan_snapshot_nk TEXT PRIMARY KEY,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                reference_trade_date DATE,
                planned_entry_trade_date DATE,
                scheduled_exit_trade_date DATE,
                position_action_decision TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                admitted_weight DOUBLE NOT NULL,
                trimmed_weight DOUBLE NOT NULL,
                plan_status TEXT NOT NULL,
                blocking_reason_code TEXT,
                planned_exit_reason_code TEXT,
                portfolio_gross_cap_weight DOUBLE NOT NULL,
                current_portfolio_gross_weight DOUBLE NOT NULL DEFAULT 0,
                remaining_portfolio_capacity_weight DOUBLE NOT NULL DEFAULT 0,
                portfolio_gross_used_weight DOUBLE NOT NULL,
                portfolio_gross_remaining_weight DOUBLE NOT NULL,
                portfolio_plan_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_plan_run_snapshot (
                run_id TEXT NOT NULL,
                plan_snapshot_nk TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                plan_status TEXT NOT NULL,
                materialization_action TEXT NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, plan_snapshot_nk)
            )
            """
        )
        connection.execute(
            "ALTER TABLE portfolio_plan_snapshot ADD COLUMN IF NOT EXISTS planned_entry_trade_date DATE"
        )
        connection.execute(
            "ALTER TABLE portfolio_plan_snapshot ADD COLUMN IF NOT EXISTS scheduled_exit_trade_date DATE"
        )
        connection.execute(
            "ALTER TABLE portfolio_plan_snapshot ADD COLUMN IF NOT EXISTS planned_exit_reason_code TEXT"
        )
        connection.execute(
            "ALTER TABLE portfolio_plan_snapshot ADD COLUMN IF NOT EXISTS current_portfolio_gross_weight DOUBLE DEFAULT 0"
        )
        connection.execute(
            "ALTER TABLE portfolio_plan_snapshot ADD COLUMN IF NOT EXISTS remaining_portfolio_capacity_weight DOUBLE DEFAULT 0"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_portfolio_plan_snapshot_portfolio ON portfolio_plan_snapshot(portfolio_id, reference_trade_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_portfolio_plan_run_snapshot_run ON portfolio_plan_run_snapshot(run_id, plan_status)"
        )
