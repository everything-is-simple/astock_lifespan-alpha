"""Schema repair entrypoints for pipeline ledgers."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.pipeline.schema import initialize_pipeline_schema


@dataclass(frozen=True)
class PipelineSchemaRepairSummary:
    runner_name: str
    status: str
    target_path: str
    checkpoint_rows_backfilled: int

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "status": self.status,
            "target_path": self.target_path,
            "checkpoint_rows_backfilled": self.checkpoint_rows_backfilled,
        }


def repair_pipeline_schema(*, settings: WorkspaceRoots | None = None) -> PipelineSchemaRepairSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.pipeline
    initialize_pipeline_schema(target_path)
    with duckdb.connect(str(target_path)) as connection:
        checkpoint_rows_backfilled = int(
            connection.execute(
                """
                WITH latest_completed_runs AS (
                    SELECT
                        run_id,
                        portfolio_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY portfolio_id
                            ORDER BY started_at DESC, run_id DESC
                        ) AS row_number
                    FROM pipeline_run
                    WHERE status = 'completed'
                )
                INSERT INTO pipeline_step_checkpoint (
                    portfolio_id,
                    step_order,
                    runner_name,
                    runner_run_id,
                    runner_status,
                    target_path,
                    message,
                    summary_json,
                    last_pipeline_run_id,
                    updated_at
                )
                SELECT
                    latest.portfolio_id,
                    step.step_order,
                    step.runner_name,
                    step.runner_run_id,
                    step.runner_status,
                    step.target_path,
                    step.message,
                    step.summary_json,
                    latest.run_id,
                    CURRENT_TIMESTAMP
                FROM latest_completed_runs AS latest
                INNER JOIN pipeline_step_run AS step
                    ON step.pipeline_run_id = latest.run_id
                WHERE latest.row_number = 1
                ON CONFLICT(portfolio_id, step_order) DO NOTHING
                RETURNING portfolio_id, step_order
                """
            ).fetchall().__len__()
        )
    return PipelineSchemaRepairSummary(
        runner_name="repair_pipeline_schema",
        status="completed",
        target_path=str(target_path),
        checkpoint_rows_backfilled=checkpoint_rows_backfilled,
    )
