"""DuckDB schema initialization for stage-four portfolio plan ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


PORTFOLIO_PLAN_TABLES = (
    "portfolio_plan_run",
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
            CREATE TABLE IF NOT EXISTS portfolio_plan_snapshot (
                plan_snapshot_nk TEXT PRIMARY KEY,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                reference_trade_date DATE,
                position_action_decision TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                admitted_weight DOUBLE NOT NULL,
                trimmed_weight DOUBLE NOT NULL,
                plan_status TEXT NOT NULL,
                blocking_reason_code TEXT,
                portfolio_gross_cap_weight DOUBLE NOT NULL,
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
            "CREATE INDEX IF NOT EXISTS idx_portfolio_plan_snapshot_portfolio ON portfolio_plan_snapshot(portfolio_id, reference_trade_date)"
        )
