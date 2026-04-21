"""Stage-six system readout runner."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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


REQUIRED_TRADE_TABLES = ("trade_order_intent", "trade_order_execution")


@dataclass(frozen=True)
class _SystemTradeSourceMetadata:
    trade_source_path: Path | None
    row_count: int
    source_available: bool


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
    message = "system run completed."
    summary_rows = 0

    with duckdb.connect(str(target_path)) as connection:
        source = _attach_system_trade_source_view(connection=connection, workspace=workspace, portfolio_id=portfolio_id)
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
        if source.row_count:
            _insert_system_readout_rows_sql(connection=connection, run_id=run_id)
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
                source.row_count,
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
        readout_rows=source.row_count,
        summary_rows=summary_rows,
    )


def _attach_system_trade_source_view(
    *,
    connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
    portfolio_id: str,
) -> _SystemTradeSourceMetadata:
    trade_path = workspace.databases.trade
    if not trade_path.exists():
        return _SystemTradeSourceMetadata(trade_source_path=None, row_count=0, source_available=False)

    connection.execute(f"ATTACH {_duckdb_string_literal(trade_path)} AS system_trade_source (READ_ONLY)")
    available_tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'system_trade_source'"
        ).fetchall()
    }
    if not set(REQUIRED_TRADE_TABLES).issubset(available_tables):
        return _SystemTradeSourceMetadata(trade_source_path=trade_path, row_count=0, source_available=False)

    portfolio_id_literal = _duckdb_string_literal(portfolio_id)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW system_trade_source_rows AS
        SELECT
            intent.order_intent_nk,
            execution.order_execution_nk,
            intent.portfolio_id,
            intent.symbol,
            CAST(intent.reference_trade_date AS DATE) AS reference_trade_date,
            CAST(intent.planned_trade_date AS DATE) AS planned_trade_date,
            CAST(execution.execution_trade_date AS DATE) AS execution_trade_date,
            intent.position_action_decision,
            intent.intent_status,
            execution.execution_status,
            intent.requested_weight,
            intent.admitted_weight,
            intent.execution_weight,
            execution.executed_weight,
            execution.execution_price,
            COALESCE(execution.blocking_reason_code, intent.blocking_reason_code) AS blocking_reason_code,
            execution.source_price_line
        FROM system_trade_source.trade_order_execution AS execution
        INNER JOIN system_trade_source.trade_order_intent AS intent
            ON intent.order_intent_nk = execution.order_intent_nk
        WHERE intent.portfolio_id = {portfolio_id_literal}
        """
    )
    row_count = connection.execute("SELECT COUNT(*) FROM system_trade_source_rows").fetchone()[0]
    return _SystemTradeSourceMetadata(trade_source_path=trade_path, row_count=int(row_count), source_available=True)


def _replace_system_portfolio_rows(*, connection: duckdb.DuckDBPyConnection, portfolio_id: str) -> None:
    connection.execute("DELETE FROM system_trade_readout WHERE portfolio_id = ?", [portfolio_id])
    connection.execute("DELETE FROM system_portfolio_trade_summary WHERE portfolio_id = ?", [portfolio_id])


def _insert_system_readout_rows_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> None:
    connection.execute(
        """
        INSERT INTO system_trade_readout (
            system_readout_nk, order_intent_nk, order_execution_nk, portfolio_id, symbol,
            reference_trade_date, planned_trade_date, execution_trade_date,
            position_action_decision, intent_status, execution_status,
            requested_weight, admitted_weight, execution_weight, executed_weight,
            execution_price, blocking_reason_code, source_price_line,
            system_contract_version, last_materialized_run_id
        )
        SELECT
            'system:' || order_execution_nk AS system_readout_nk,
            order_intent_nk,
            order_execution_nk,
            portfolio_id,
            symbol,
            reference_trade_date,
            planned_trade_date,
            execution_trade_date,
            position_action_decision,
            intent_status,
            execution_status,
            requested_weight,
            admitted_weight,
            execution_weight,
            executed_weight,
            execution_price,
            blocking_reason_code,
            source_price_line,
            ? AS system_contract_version,
            ? AS last_materialized_run_id
        FROM system_trade_source_rows
        ORDER BY portfolio_id, symbol, execution_trade_date, order_execution_nk
        """,
        [SYSTEM_CONTRACT_VERSION, run_id],
    )


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


def _duckdb_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
