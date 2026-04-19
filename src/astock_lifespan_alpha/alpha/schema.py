"""DuckDB schema initialization for Alpha ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


TRIGGER_TABLES = (
    "alpha_run",
    "alpha_work_queue",
    "alpha_checkpoint",
    "alpha_trigger_event",
    "alpha_trigger_profile",
)

SIGNAL_TABLES = (
    "alpha_signal_run",
    "alpha_signal_work_queue",
    "alpha_signal_checkpoint",
    "alpha_signal",
)


def initialize_alpha_trigger_schema(database_path: Path) -> None:
    """Create the formal alpha trigger schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_run (
                run_id TEXT PRIMARY KEY,
                trigger_type TEXT NOT NULL,
                status TEXT NOT NULL,
                market_source_path TEXT,
                malf_source_path TEXT,
                input_rows BIGINT NOT NULL DEFAULT 0,
                symbols_seen BIGINT NOT NULL DEFAULT 0,
                symbols_updated BIGINT NOT NULL DEFAULT 0,
                inserted_trigger_events BIGINT NOT NULL DEFAULT 0,
                inserted_trigger_profiles BIGINT NOT NULL DEFAULT 0,
                latest_signal_date DATE,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_work_queue (
                queue_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS alpha_checkpoint (
                symbol TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                last_signal_date DATE,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trigger_type)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_trigger_event (
                event_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_date DATE NOT NULL,
                trigger_type TEXT NOT NULL,
                formal_signal_status TEXT NOT NULL,
                source_bar_dt DATE NOT NULL,
                wave_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_trigger_profile (
                profile_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                formal_signal_status TEXT NOT NULL,
                event_count BIGINT NOT NULL,
                latest_signal_date DATE NOT NULL,
                avg_update_rank DOUBLE NOT NULL,
                avg_stagnation_rank DOUBLE NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_alpha_trigger_event_symbol ON alpha_trigger_event(symbol, trigger_type, signal_date)"
        )


def initialize_alpha_signal_schema(database_path: Path) -> None:
    """Create the formal alpha_signal schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_signal_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                source_trigger_count BIGINT NOT NULL DEFAULT 0,
                sources_updated BIGINT NOT NULL DEFAULT 0,
                inserted_signals BIGINT NOT NULL DEFAULT 0,
                latest_signal_date DATE,
                message TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_signal_work_queue (
                queue_id TEXT PRIMARY KEY,
                source_trigger_db TEXT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS alpha_signal_checkpoint (
                source_trigger_db TEXT PRIMARY KEY,
                last_signal_date DATE,
                last_run_id TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_signal (
                signal_nk TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_date DATE NOT NULL,
                trigger_type TEXT NOT NULL,
                formal_signal_status TEXT NOT NULL,
                source_trigger_db TEXT NOT NULL,
                source_trigger_event_nk TEXT NOT NULL,
                wave_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                new_count BIGINT NOT NULL,
                no_new_span BIGINT NOT NULL,
                life_state TEXT NOT NULL,
                update_rank DOUBLE NOT NULL,
                stagnation_rank DOUBLE NOT NULL,
                wave_position_zone TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_alpha_signal_symbol ON alpha_signal(symbol, signal_date, trigger_type)"
        )
