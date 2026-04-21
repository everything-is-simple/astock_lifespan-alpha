"""Schema repair entrypoints for portfolio plan ledgers."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.portfolio_plan.schema import initialize_portfolio_plan_schema


@dataclass(frozen=True)
class PortfolioPlanSchemaRepairSummary:
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


def repair_portfolio_plan_schema(*, settings: WorkspaceRoots | None = None) -> PortfolioPlanSchemaRepairSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.portfolio_plan
    initialize_portfolio_plan_schema(target_path)
    checkpoint_rows_backfilled = 0
    with duckdb.connect(str(target_path)) as connection:
        checkpoint_rows_backfilled = int(
            connection.execute(
                """
                INSERT INTO portfolio_plan_checkpoint (
                    portfolio_id,
                    last_reference_trade_date,
                    last_source_fingerprint,
                    last_run_id,
                    updated_at
                )
                SELECT
                    snapshot.portfolio_id,
                    MAX(snapshot.reference_trade_date),
                    md5(
                        string_agg(
                            CONCAT(
                                snapshot.plan_snapshot_nk,
                                '|',
                                snapshot.candidate_nk,
                                '|',
                                snapshot.symbol,
                                '|',
                                COALESCE(CAST(snapshot.reference_trade_date AS VARCHAR), ''),
                                '|',
                                COALESCE(CAST(snapshot.planned_entry_trade_date AS VARCHAR), ''),
                                '|',
                                COALESCE(CAST(snapshot.scheduled_exit_trade_date AS VARCHAR), ''),
                                '|',
                                snapshot.position_action_decision,
                                '|',
                                CAST(snapshot.requested_weight AS VARCHAR),
                                '|',
                                CAST(snapshot.admitted_weight AS VARCHAR),
                                '|',
                                CAST(snapshot.trimmed_weight AS VARCHAR),
                                '|',
                                snapshot.plan_status,
                                '|',
                                COALESCE(snapshot.blocking_reason_code, ''),
                                '|',
                                COALESCE(snapshot.planned_exit_reason_code, ''),
                                '|',
                                CAST(snapshot.portfolio_gross_cap_weight AS VARCHAR),
                                '|',
                                CAST(snapshot.current_portfolio_gross_weight AS VARCHAR),
                                '|',
                                CAST(snapshot.remaining_portfolio_capacity_weight AS VARCHAR)
                            )
                            ORDER BY snapshot.planned_entry_trade_date, snapshot.reference_trade_date, snapshot.candidate_nk, snapshot.plan_snapshot_nk
                        )
                    ),
                    MAX(snapshot.last_materialized_run_id),
                    CURRENT_TIMESTAMP
                FROM portfolio_plan_snapshot AS snapshot
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM portfolio_plan_checkpoint AS checkpoint
                    WHERE checkpoint.portfolio_id = snapshot.portfolio_id
                )
                GROUP BY snapshot.portfolio_id
                ON CONFLICT(portfolio_id) DO NOTHING
                RETURNING portfolio_id
                """
            ).fetchall().__len__()
        )
    return PortfolioPlanSchemaRepairSummary(
        runner_name="repair_portfolio_plan_schema",
        status="completed",
        target_path=str(target_path),
        checkpoint_rows_backfilled=checkpoint_rows_backfilled,
    )
