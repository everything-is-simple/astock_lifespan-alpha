"""DuckDB schema initialization for stage-eight pipeline ledgers."""

from __future__ import annotations

from pathlib import Path

import duckdb


PIPELINE_TABLES = (
    "pipeline_run",
    "pipeline_step_run",
)


def initialize_pipeline_schema(database_path: Path) -> None:
    """Create the formal pipeline schema if it does not already exist."""

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_run (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                portfolio_id TEXT NOT NULL,
                step_count BIGINT NOT NULL DEFAULT 0,
                message TEXT,
                pipeline_contract_version TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_step_run (
                pipeline_run_id TEXT NOT NULL,
                step_order BIGINT NOT NULL,
                runner_name TEXT NOT NULL,
                runner_run_id TEXT NOT NULL,
                runner_status TEXT NOT NULL,
                target_path TEXT,
                message TEXT,
                summary_json TEXT NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (pipeline_run_id, step_order)
            )
            """
        )

