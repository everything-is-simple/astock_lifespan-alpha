"""DuckDB schema initialization for stage-four position ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


POSITION_TABLES = (
    "position_run",
    "position_work_queue",
    "position_checkpoint",
    "position_candidate_audit",
    "position_capacity_snapshot",
    "position_sizing_snapshot",
)


def initialize_position_schema(database_path: Path) -> None:
    """Create the formal position schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                alpha_source_path TEXT,
                market_source_path TEXT,
                input_rows BIGINT NOT NULL DEFAULT 0,
                symbols_seen BIGINT NOT NULL DEFAULT 0,
                symbols_updated BIGINT NOT NULL DEFAULT 0,
                inserted_candidates BIGINT NOT NULL DEFAULT 0,
                inserted_capacity_rows BIGINT NOT NULL DEFAULT 0,
                inserted_sizing_rows BIGINT NOT NULL DEFAULT 0,
                latest_signal_date DATE,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_work_queue (
                queue_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                source_row_count BIGINT NOT NULL DEFAULT 0,
                requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TIMESTAMP,
                finished_at TIMESTAMP,
                last_signal_date DATE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_checkpoint (
                symbol TEXT PRIMARY KEY,
                last_signal_date DATE,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_candidate_audit (
                candidate_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                signal_nk TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_date DATE NOT NULL,
                trigger_type TEXT NOT NULL,
                formal_signal_status TEXT NOT NULL,
                candidate_status TEXT NOT NULL,
                blocked_reason_code TEXT,
                source_trigger_event_nk TEXT NOT NULL,
                wave_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL,
                reference_trade_date DATE,
                reference_price DOUBLE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_capacity_snapshot (
                capacity_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_date DATE NOT NULL,
                policy_id TEXT NOT NULL,
                capacity_status TEXT NOT NULL,
                capacity_ceiling_weight DOUBLE NOT NULL,
                reference_trade_date DATE,
                reference_price DOUBLE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS position_sizing_snapshot (
                sizing_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_date DATE NOT NULL,
                policy_id TEXT NOT NULL,
                position_action_decision TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                final_allowed_position_weight DOUBLE NOT NULL,
                required_reduction_weight DOUBLE NOT NULL,
                candidate_status TEXT NOT NULL,
                reference_trade_date DATE,
                reference_price DOUBLE
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_candidate_symbol ON position_candidate_audit(symbol, signal_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_position_sizing_symbol ON position_sizing_snapshot(symbol, signal_date)"
        )
