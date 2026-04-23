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


def _system_readout_row_signature_sql(*, row_alias: str) -> str:
    return f"""
        hash(
            {row_alias}.system_readout_nk,
            {row_alias}.order_intent_nk,
            {row_alias}.order_execution_nk,
            {row_alias}.portfolio_id,
            {row_alias}.symbol,
            {row_alias}.reference_trade_date,
            {row_alias}.planned_trade_date,
            {row_alias}.execution_trade_date,
            {row_alias}.trade_action,
            {row_alias}.position_leg_nk,
            {row_alias}.position_action_decision,
            {row_alias}.intent_status,
            {row_alias}.execution_status,
            {row_alias}.requested_weight,
            {row_alias}.admitted_weight,
            {row_alias}.execution_weight,
            {row_alias}.executed_weight,
            {row_alias}.execution_price,
            {row_alias}.blocking_reason_code,
            {row_alias}.source_price_line
        )
    """


def repair_system_schema(*, settings: WorkspaceRoots | None = None) -> SystemSchemaRepairSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.system
    initialize_system_schema(target_path)
    checkpoint_rows_backfilled = 0
    with duckdb.connect(str(target_path)) as connection:
        row_signature = _system_readout_row_signature_sql(row_alias="readout")
        checkpoint_rows_backfilled = int(
            connection.execute(
                f"""
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
                    CAST(hash(
                        COUNT(*),
                        MAX(readout.execution_trade_date),
                        bit_xor({row_signature}),
                        SUM({row_signature})
                    ) AS VARCHAR),
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
