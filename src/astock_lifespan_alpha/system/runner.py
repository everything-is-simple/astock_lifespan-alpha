"""Stage-six system readout runner."""

from __future__ import annotations

from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.system.contracts import (
    SYSTEM_CONTRACT_VERSION,
    SystemRunStatus,
    SystemRunSummary,
    SystemTradeReadoutRecord,
)
from astock_lifespan_alpha.system.schema import initialize_system_schema
from astock_lifespan_alpha.system.source import load_system_trade_readout_rows


def run_system_from_trade(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> SystemRunSummary:
    """Build the minimal trade -> system readout ledger."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.system
    initialize_system_schema(target_path)

    run_id = f"system-{uuid4().hex[:12]}"
    source = load_system_trade_readout_rows(settings=workspace, portfolio_id=portfolio_id)
    message = "system run completed."
    summary_rows = 0

    with duckdb.connect(str(target_path)) as connection:
        connection.execute(
            """
            INSERT INTO system_run (
                run_id, status, portfolio_id, source_trade_path, readout_rows, summary_rows, message
            ) VALUES (?, 'running', ?, ?, 0, 0, 'system run started.')
            """,
            [
                run_id,
                portfolio_id,
                str(source.trade_source_path) if source.trade_source_path is not None else None,
            ],
        )
        _replace_system_portfolio_rows(connection=connection, portfolio_id=portfolio_id)
        if source.readout_rows:
            _insert_system_readout_rows(connection=connection, run_id=run_id, rows=source.readout_rows)
            _insert_system_summary(connection=connection, run_id=run_id, portfolio_id=portfolio_id)
            summary_rows = 1
        elif not source.source_available:
            message = "system schema initialized without trade rows."
        else:
            message = "system run completed without trade rows for portfolio."

        connection.execute(
            """
            UPDATE system_run
            SET
                status = ?,
                readout_rows = ?,
                summary_rows = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                SystemRunStatus.COMPLETED.value,
                len(source.readout_rows),
                summary_rows,
                message,
                run_id,
            ],
        )

    return SystemRunSummary(
        runner_name="run_system_from_trade",
        run_id=run_id,
        status=SystemRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={"trade": str(source.trade_source_path) if source.trade_source_path else None},
        message=message,
        readout_rows=len(source.readout_rows),
        summary_rows=summary_rows,
    )


def _replace_system_portfolio_rows(*, connection: duckdb.DuckDBPyConnection, portfolio_id: str) -> None:
    connection.execute("DELETE FROM system_trade_readout WHERE portfolio_id = ?", [portfolio_id])
    connection.execute("DELETE FROM system_portfolio_trade_summary WHERE portfolio_id = ?", [portfolio_id])


def _insert_system_readout_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    rows: list[SystemTradeReadoutRecord],
) -> None:
    connection.executemany(
        """
        INSERT INTO system_trade_readout (
            system_readout_nk, order_intent_nk, order_execution_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, execution_trade_date,
            position_action_decision, intent_status, execution_status,
            requested_weight, admitted_weight, execution_weight, executed_weight,
            execution_price, blocking_reason_code, source_price_line,
            system_contract_version, last_materialized_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.system_readout_nk,
                row.order_intent_nk,
                row.order_execution_nk,
                row.portfolio_id,
                row.symbol,
                row.reference_trade_date,
                row.planned_trade_date,
                row.execution_trade_date,
                row.position_action_decision,
                row.intent_status,
                row.execution_status,
                row.requested_weight,
                row.admitted_weight,
                row.execution_weight,
                row.executed_weight,
                row.execution_price,
                row.blocking_reason_code,
                row.source_price_line,
                SYSTEM_CONTRACT_VERSION,
                run_id,
            )
            for row in rows
        ],
    )


def _insert_system_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
) -> None:
    connection.execute(
        """
        INSERT INTO system_portfolio_trade_summary (
            portfolio_id,
            execution_count,
            filled_count,
            rejected_count,
            symbol_count,
            gross_executed_weight,
            latest_execution_trade_date,
            system_contract_version,
            last_materialized_run_id
        )
        SELECT
            portfolio_id,
            COUNT(*) AS execution_count,
            SUM(CASE WHEN execution_status = 'filled' THEN 1 ELSE 0 END) AS filled_count,
            SUM(CASE WHEN execution_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count,
            COUNT(DISTINCT symbol) AS symbol_count,
            SUM(ABS(executed_weight)) AS gross_executed_weight,
            MAX(execution_trade_date) AS latest_execution_trade_date,
            ? AS system_contract_version,
            ? AS last_materialized_run_id
        FROM system_trade_readout
        WHERE portfolio_id = ?
        GROUP BY portfolio_id
        """,
        [SYSTEM_CONTRACT_VERSION, run_id, portfolio_id],
    )

