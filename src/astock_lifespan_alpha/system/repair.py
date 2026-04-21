"""Schema repair entrypoints for system ledgers."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.system.schema import initialize_system_schema


@dataclass(frozen=True)
class SystemSchemaRepairSummary:
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


def repair_system_schema(*, settings: WorkspaceRoots | None = None) -> SystemSchemaRepairSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.system
    initialize_system_schema(target_path)
    checkpoint_rows_backfilled = 0
    with duckdb.connect(str(target_path)) as connection:
        checkpoint_rows_backfilled = int(
            connection.execute(
                """
                INSERT INTO system_checkpoint (
                    portfolio_id,
                    symbol,
                    latest_execution_trade_date,
                    last_source_fingerprint,
                    last_run_id,
                    updated_at
                )
                SELECT
                    readout.portfolio_id,
                    readout.symbol,
                    MAX(readout.execution_trade_date),
                    md5(
                        string_agg(
                            CONCAT(
                                readout.order_intent_nk,
                                '|',
                                readout.order_execution_nk,
                                '|',
                                readout.portfolio_id,
                                '|',
                                readout.symbol,
                                '|',
                                COALESCE(CAST(readout.reference_trade_date AS VARCHAR), ''),
                                '|',
                                COALESCE(CAST(readout.planned_trade_date AS VARCHAR), ''),
                                '|',
                                COALESCE(CAST(readout.execution_trade_date AS VARCHAR), ''),
                                '|',
                                readout.trade_action,
                                '|',
                                COALESCE(readout.position_leg_nk, ''),
                                '|',
                                readout.position_action_decision,
                                '|',
                                readout.intent_status,
                                '|',
                                readout.execution_status,
                                '|',
                                CAST(readout.requested_weight AS VARCHAR),
                                '|',
                                CAST(readout.admitted_weight AS VARCHAR),
                                '|',
                                CAST(readout.execution_weight AS VARCHAR),
                                '|',
                                CAST(readout.executed_weight AS VARCHAR),
                                '|',
                                COALESCE(CAST(readout.execution_price AS VARCHAR), ''),
                                '|',
                                COALESCE(readout.blocking_reason_code, ''),
                                '|',
                                readout.source_price_line
                            )
                            ORDER BY readout.execution_trade_date, readout.order_execution_nk
                        )
                    ),
                    MAX(readout.last_materialized_run_id),
                    CURRENT_TIMESTAMP
                FROM system_trade_readout AS readout
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM system_checkpoint AS checkpoint
                    WHERE checkpoint.portfolio_id = readout.portfolio_id
                        AND checkpoint.symbol = readout.symbol
                )
                GROUP BY readout.portfolio_id, readout.symbol
                ON CONFLICT(portfolio_id, symbol) DO NOTHING
                RETURNING portfolio_id, symbol
                """
            ).fetchall().__len__()
        )
    return SystemSchemaRepairSummary(
        runner_name="repair_system_schema",
        status="completed",
        target_path=str(target_path),
        checkpoint_rows_backfilled=checkpoint_rows_backfilled,
    )
