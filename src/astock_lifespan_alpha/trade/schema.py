"""DuckDB schema initialization for stage-five trade ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


TRADE_TABLES = (
    "trade_run",
    "trade_work_queue",
    "trade_checkpoint",
    "trade_order_intent",
    "trade_order_execution",
    "trade_position_leg",
    "trade_carry_snapshot",
    "trade_exit_execution",
    "trade_run_order_intent",
)


def initialize_trade_schema(database_path: Path) -> None:
    """Create the formal trade schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                source_portfolio_plan_path TEXT,
                source_execution_price_path TEXT,
                input_rows BIGINT NOT NULL DEFAULT 0,
                work_units_seen BIGINT NOT NULL DEFAULT 0,
                work_units_updated BIGINT NOT NULL DEFAULT 0,
                inserted_order_intents BIGINT NOT NULL DEFAULT 0,
                reused_order_intents BIGINT NOT NULL DEFAULT 0,
                rematerialized_order_intents BIGINT NOT NULL DEFAULT 0,
                inserted_order_executions BIGINT NOT NULL DEFAULT 0,
                reused_order_executions BIGINT NOT NULL DEFAULT 0,
                rematerialized_order_executions BIGINT NOT NULL DEFAULT 0,
                inserted_position_legs BIGINT NOT NULL DEFAULT 0,
                reused_position_legs BIGINT NOT NULL DEFAULT 0,
                rematerialized_position_legs BIGINT NOT NULL DEFAULT 0,
                inserted_carry_rows BIGINT NOT NULL DEFAULT 0,
                reused_carry_rows BIGINT NOT NULL DEFAULT 0,
                rematerialized_carry_rows BIGINT NOT NULL DEFAULT 0,
                inserted_exit_rows BIGINT NOT NULL DEFAULT 0,
                reused_exit_rows BIGINT NOT NULL DEFAULT 0,
                rematerialized_exit_rows BIGINT NOT NULL DEFAULT 0,
                latest_reference_trade_date DATE,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_work_queue (
                queue_id TEXT PRIMARY KEY,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS trade_checkpoint (
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                last_reference_trade_date DATE,
                last_source_fingerprint TEXT,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (portfolio_id, symbol)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_order_intent (
                order_intent_nk TEXT PRIMARY KEY,
                plan_snapshot_nk TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                reference_trade_date DATE,
                planned_trade_date DATE,
                position_action_decision TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                requested_weight DOUBLE NOT NULL,
                admitted_weight DOUBLE NOT NULL,
                execution_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_order_execution (
                order_execution_nk TEXT PRIMARY KEY,
                order_intent_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                execution_status TEXT NOT NULL,
                execution_trade_date DATE,
                execution_price DOUBLE,
                executed_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                source_price_line TEXT NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_position_leg (
                position_leg_nk TEXT PRIMARY KEY,
                candidate_nk TEXT NOT NULL,
                order_intent_nk TEXT,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                entry_reference_trade_date DATE,
                entry_trade_date DATE,
                entry_execution_price DOUBLE,
                position_weight DOUBLE NOT NULL,
                scheduled_exit_trade_date DATE,
                position_state TEXT NOT NULL,
                exit_execution_nk TEXT,
                exit_trade_date DATE,
                exit_execution_price DOUBLE,
                active_weight DOUBLE NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_carry_snapshot (
                carry_snapshot_nk TEXT PRIMARY KEY,
                position_leg_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                as_of_trade_date DATE,
                carry_status TEXT NOT NULL,
                carried_weight DOUBLE NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_exit_execution (
                exit_execution_nk TEXT PRIMARY KEY,
                position_leg_nk TEXT NOT NULL,
                candidate_nk TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exit_trade_date DATE,
                execution_status TEXT NOT NULL,
                execution_price DOUBLE,
                exited_weight DOUBLE NOT NULL,
                blocking_reason_code TEXT,
                exit_reason_code TEXT,
                source_price_line TEXT NOT NULL,
                trade_contract_version TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_materialized_run_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_run_order_intent (
                run_id TEXT NOT NULL,
                order_intent_nk TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                materialization_action TEXT NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, order_intent_nk)
            )
            """
        )
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS inserted_position_legs BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS reused_position_legs BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS rematerialized_position_legs BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS inserted_carry_rows BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS reused_carry_rows BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS rematerialized_carry_rows BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS inserted_exit_rows BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS reused_exit_rows BIGINT DEFAULT 0")
        connection.execute("ALTER TABLE trade_run ADD COLUMN IF NOT EXISTS rematerialized_exit_rows BIGINT DEFAULT 0")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_intent_work_unit ON trade_order_intent(portfolio_id, symbol, reference_trade_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_execution_work_unit ON trade_order_execution(portfolio_id, symbol, execution_trade_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_position_leg_work_unit ON trade_position_leg(portfolio_id, symbol, entry_trade_date)"
        )
