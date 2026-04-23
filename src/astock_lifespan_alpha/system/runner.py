"""Stage-six system readout runner."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.system.contracts import (
    SYSTEM_CONTRACT_VERSION,
    SystemCheckpointSummary,
    SystemRunStatus,
    SystemRunSummary,
)
from astock_lifespan_alpha.system.schema import initialize_system_schema


REQUIRED_TRADE_TABLES = ("trade_order_intent", "trade_order_execution", "trade_exit_execution", "trade_position_leg")


@dataclass(frozen=True)
class _SystemTradeSourceMetadata:
    trade_source_path: Path | None
    row_count: int
    work_unit_count: int
    source_available: bool


def _table_row_count(*, connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)


def _record_system_phase(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    phase: str,
    started_at: float,
    table_name: str | None = None,
    detail: str | None = None,
) -> str:
    parts = [phase, f"elapsed_seconds={perf_counter() - started_at:.6f}"]
    if table_name is not None:
        parts.append(f"rows={_table_row_count(connection=connection, table_name=table_name)}")
    if detail:
        parts.append(detail)
    message = "system phase " + " ".join(parts)
    connection.execute("UPDATE system_run SET message = ? WHERE run_id = ?", [message, run_id])
    print(message, file=sys.stderr, flush=True)
    return message


def run_system_from_trade(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> SystemRunSummary:
    """Build the rolling trade -> system readout ledger."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.system
    initialize_system_schema(target_path)

    run_id = f"system-{uuid4().hex[:12]}"
    message = "system run completed."
    summary_rows = 0
    work_units_updated = 0
    latest_execution_trade_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        phase_started = perf_counter()
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
        message = _record_system_phase(
            connection=connection,
            run_id=run_id,
            phase="source_attached",
            started_at=phase_started,
            table_name="system_trade_source_rows" if source.row_count else None,
            detail=f"work_units={source.work_unit_count}",
        )

        try:
            connection.execute("DELETE FROM system_work_queue")
            if source.row_count:
                phase_started = perf_counter()
                _create_system_source_work_unit_summary(connection=connection)
                message = _record_system_phase(
                    connection=connection,
                    run_id=run_id,
                    phase="work_unit_summary_ready",
                    started_at=phase_started,
                    table_name="system_source_work_unit_summary",
                    detail=f"source_rows={source.row_count}",
                )
                if _system_checkpoint_fast_path_available(connection=connection):
                    phase_started = perf_counter()
                    work_units_updated, latest_execution_trade_date = _record_reused_system_sql(
                        connection=connection,
                        run_id=run_id,
                    )
                    message = _record_system_phase(
                        connection=connection,
                        run_id=run_id,
                        phase="write_reused_tracking_committed",
                        started_at=phase_started,
                        table_name="system_work_queue",
                        detail="work_units_updated=0",
                    )
                else:
                    phase_started = perf_counter()
                    work_units_updated, latest_execution_trade_date = _materialize_system_sql(
                        connection=connection,
                        run_id=run_id,
                        portfolio_id=portfolio_id,
                    )
                    message = _record_system_phase(
                        connection=connection,
                        run_id=run_id,
                        phase="write_materialized_committed",
                        started_at=phase_started,
                        table_name="system_trade_readout",
                        detail=f"work_units_updated={work_units_updated}",
                    )
                summary_rows = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM system_portfolio_trade_summary WHERE portfolio_id = ?",
                        [portfolio_id],
                    ).fetchone()[0]
                )
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
            message = _record_system_phase(
                connection=connection,
                run_id=run_id,
                phase="system_run_completed",
                started_at=phase_started,
                table_name="system_portfolio_trade_summary" if summary_rows else None,
                detail=f"readout_rows={source.row_count}",
            )
        except Exception as exc:
            connection.execute(
                """
                UPDATE system_run
                SET status = 'interrupted', message = ?, finished_at = CURRENT_TIMESTAMP
                WHERE run_id = ? AND finished_at IS NULL
                """,
                [f"system run interrupted: {exc}", run_id],
            )
            raise

    return SystemRunSummary(
        runner_name="run_system_from_trade",
        run_id=run_id,
        status=SystemRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={"trade": str(source.trade_source_path) if source.trade_source_path else None},
        message=message,
        readout_rows=source.row_count,
        summary_rows=summary_rows,
        checkpoint_summary=SystemCheckpointSummary(
            work_units_seen=source.work_unit_count,
            work_units_updated=work_units_updated,
            latest_execution_trade_date=latest_execution_trade_date.isoformat()
            if latest_execution_trade_date is not None
            else None,
        ),
    )


def _attach_system_trade_source_view(
    *,
    connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
    portfolio_id: str,
) -> _SystemTradeSourceMetadata:
    trade_path = workspace.databases.trade
    if not trade_path.exists():
        return _SystemTradeSourceMetadata(trade_source_path=None, row_count=0, work_unit_count=0, source_available=False)

    connection.execute(f"ATTACH {_duckdb_string_literal(trade_path)} AS system_trade_source (READ_ONLY)")
    available_tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'system_trade_source'"
        ).fetchall()
    }
    if not set(REQUIRED_TRADE_TABLES).issubset(available_tables):
        return _SystemTradeSourceMetadata(
            trade_source_path=trade_path,
            row_count=0,
            work_unit_count=0,
            source_available=False,
        )

    portfolio_id_literal = _duckdb_string_literal(portfolio_id)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW system_trade_source_rows AS
        SELECT
            CONCAT('system:', execution.order_execution_nk) AS system_readout_nk,
            intent.order_intent_nk,
            execution.order_execution_nk,
            intent.portfolio_id,
            intent.symbol,
            CAST(intent.reference_trade_date AS DATE) AS reference_trade_date,
            CAST(intent.planned_trade_date AS DATE) AS planned_trade_date,
            CAST(execution.execution_trade_date AS DATE) AS execution_trade_date,
            'open_entry' AS trade_action,
            CAST(NULL AS VARCHAR) AS position_leg_nk,
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

        UNION ALL

        SELECT
            CONCAT('system:', exit_execution.exit_execution_nk) AS system_readout_nk,
            CONCAT(exit_execution.position_leg_nk, ':exit') AS order_intent_nk,
            exit_execution.exit_execution_nk AS order_execution_nk,
            exit_execution.portfolio_id,
            exit_execution.symbol,
            leg.entry_reference_trade_date AS reference_trade_date,
            leg.scheduled_exit_trade_date AS planned_trade_date,
            exit_execution.exit_trade_date AS execution_trade_date,
            'full_exit' AS trade_action,
            exit_execution.position_leg_nk,
            'full_exit' AS position_action_decision,
            CASE WHEN exit_execution.execution_status = 'filled' THEN 'planned' ELSE 'blocked' END AS intent_status,
            exit_execution.execution_status,
            leg.position_weight AS requested_weight,
            leg.position_weight AS admitted_weight,
            leg.position_weight AS execution_weight,
            exit_execution.exited_weight AS executed_weight,
            exit_execution.execution_price,
            exit_execution.blocking_reason_code,
            exit_execution.source_price_line
        FROM system_trade_source.trade_exit_execution AS exit_execution
        INNER JOIN system_trade_source.trade_position_leg AS leg
            ON leg.position_leg_nk = exit_execution.position_leg_nk
        WHERE exit_execution.portfolio_id = {portfolio_id_literal}
        """
    )
    row_count, work_unit_count = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT portfolio_id || ':' || symbol) FROM system_trade_source_rows"
    ).fetchone()
    return _SystemTradeSourceMetadata(
        trade_source_path=trade_path,
        row_count=int(row_count),
        work_unit_count=int(work_unit_count),
        source_available=True,
    )


def _system_source_row_signature_sql(*, row_alias: str) -> str:
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


def _create_system_source_work_unit_summary(*, connection: duckdb.DuckDBPyConnection) -> None:
    row_signature = _system_source_row_signature_sql(row_alias="source")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE system_source_work_unit_summary AS
        SELECT
            portfolio_id,
            symbol,
            COUNT(*) AS source_row_count,
            MAX(execution_trade_date) AS latest_execution_trade_date,
            CAST(hash(
                COUNT(*),
                MAX(execution_trade_date),
                bit_xor({row_signature}),
                SUM({row_signature})
            ) AS VARCHAR) AS source_fingerprint
        FROM system_trade_source_rows AS source
        GROUP BY portfolio_id, symbol
        """
    )


def _system_checkpoint_fast_path_available(*, connection: duckdb.DuckDBPyConnection) -> bool:
    row = connection.execute(
        """
        WITH existing_counts AS (
            SELECT portfolio_id, symbol, COUNT(*) AS existing_row_count
            FROM system_trade_readout
            GROUP BY portfolio_id, symbol
        )
        SELECT
            COUNT(*) AS work_unit_count,
            SUM(
                CASE
                    WHEN checkpoint.portfolio_id IS NOT NULL
                        AND checkpoint.latest_execution_trade_date IS NOT DISTINCT FROM source.latest_execution_trade_date
                        AND checkpoint.last_source_fingerprint = source.source_fingerprint
                        AND COALESCE(existing.existing_row_count, 0) = source.source_row_count
                    THEN 1
                    ELSE 0
                END
            ) AS matching_work_units
        FROM system_source_work_unit_summary AS source
        LEFT JOIN system_checkpoint AS checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
            AND checkpoint.symbol = source.symbol
        LEFT JOIN existing_counts AS existing
            ON existing.portfolio_id = source.portfolio_id
            AND existing.symbol = source.symbol
        """
    ).fetchone()
    return int(row[0] or 0) > 0 and int(row[0] or 0) == int(row[1] or 0)


def _record_reused_system_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> tuple[int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_source_work_unit_reused AS
        SELECT
            portfolio_id,
            symbol,
            source_row_count,
            latest_execution_trade_date,
            source_fingerprint,
            'reused' AS status
        FROM system_source_work_unit_summary
        """
    )
    try:
        connection.execute("BEGIN TRANSACTION")
        _insert_system_work_queue_sql(connection=connection, run_id=run_id, status_table_name="system_source_work_unit_reused")
        _upsert_system_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    latest_execution_trade_date = connection.execute(
        "SELECT MAX(latest_execution_trade_date) FROM system_source_work_unit_summary"
    ).fetchone()[0]
    return 0, latest_execution_trade_date


def _materialize_system_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
) -> tuple[int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_existing_readout_counts AS
        SELECT portfolio_id, symbol, COUNT(*) AS existing_row_count
        FROM system_trade_readout
        WHERE portfolio_id = ?
        GROUP BY portfolio_id, symbol
        """,
        [portfolio_id],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_work_units_to_update AS
        SELECT source.portfolio_id, source.symbol
        FROM system_source_work_unit_summary AS source
        LEFT JOIN system_checkpoint AS checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
            AND checkpoint.symbol = source.symbol
        LEFT JOIN system_existing_readout_counts AS existing
            ON existing.portfolio_id = source.portfolio_id
            AND existing.symbol = source.symbol
        WHERE checkpoint.portfolio_id IS NULL
            OR checkpoint.latest_execution_trade_date IS DISTINCT FROM source.latest_execution_trade_date
            OR checkpoint.last_source_fingerprint IS DISTINCT FROM source.source_fingerprint
            OR COALESCE(existing.existing_row_count, 0) != source.source_row_count
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_source_work_unit_reused AS
        SELECT
            source.portfolio_id,
            source.symbol,
            source.source_row_count,
            source.latest_execution_trade_date,
            source.source_fingerprint,
            'reused' AS status
        FROM system_source_work_unit_summary AS source
        LEFT JOIN system_work_units_to_update AS updates
            ON updates.portfolio_id = source.portfolio_id
            AND updates.symbol = source.symbol
        WHERE updates.symbol IS NULL
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_source_work_unit_completed AS
        SELECT
            source.portfolio_id,
            source.symbol,
            source.source_row_count,
            source.latest_execution_trade_date,
            source.source_fingerprint,
            'completed' AS status
        FROM system_source_work_unit_summary AS source
        INNER JOIN system_work_units_to_update AS updates
            ON updates.portfolio_id = source.portfolio_id
            AND updates.symbol = source.symbol
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE system_materialized_readout AS
        SELECT
            source.system_readout_nk,
            source.order_intent_nk,
            source.order_execution_nk,
            source.portfolio_id,
            source.symbol,
            source.reference_trade_date,
            source.planned_trade_date,
            source.execution_trade_date,
            source.trade_action,
            source.position_leg_nk,
            source.position_action_decision,
            source.intent_status,
            source.execution_status,
            source.requested_weight,
            source.admitted_weight,
            source.execution_weight,
            source.executed_weight,
            source.execution_price,
            source.blocking_reason_code,
            source.source_price_line,
            ? AS system_contract_version,
            ? AS last_materialized_run_id
        FROM system_trade_source_rows AS source
        INNER JOIN system_work_units_to_update AS updates
            ON updates.portfolio_id = source.portfolio_id
            AND updates.symbol = source.symbol
        ORDER BY source.portfolio_id, source.symbol, source.execution_trade_date, source.order_execution_nk
        """,
        [SYSTEM_CONTRACT_VERSION, run_id],
    )
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            DELETE FROM system_trade_readout
            WHERE portfolio_id = ?
                AND symbol IN (SELECT symbol FROM system_work_units_to_update)
            """,
            [portfolio_id],
        )
        connection.execute(
            """
            INSERT INTO system_trade_readout (
                system_readout_nk, order_intent_nk, order_execution_nk, portfolio_id, symbol,
                reference_trade_date, planned_trade_date, execution_trade_date, trade_action, position_leg_nk,
                position_action_decision, intent_status, execution_status,
                requested_weight, admitted_weight, execution_weight, executed_weight,
                execution_price, blocking_reason_code, source_price_line,
                system_contract_version, last_materialized_run_id
            )
            SELECT
                system_readout_nk,
                order_intent_nk,
                order_execution_nk,
                portfolio_id,
                symbol,
                reference_trade_date,
                planned_trade_date,
                execution_trade_date,
                trade_action,
                position_leg_nk,
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
                system_contract_version,
                last_materialized_run_id
            FROM system_materialized_readout
            """
        )
        _insert_system_work_queue_sql(connection=connection, run_id=run_id, status_table_name="system_source_work_unit_reused")
        _insert_system_work_queue_sql(connection=connection, run_id=run_id, status_table_name="system_source_work_unit_completed")
        _upsert_system_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("DELETE FROM system_portfolio_trade_summary WHERE portfolio_id = ?", [portfolio_id])
        connection.execute(
            """
            INSERT INTO system_portfolio_trade_summary (
                portfolio_id,
                open_entry_count,
                full_exit_count,
                active_symbol_count,
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
                readout.portfolio_id,
                SUM(CASE WHEN readout.trade_action = 'open_entry' AND readout.execution_status = 'filled' THEN 1 ELSE 0 END) AS open_entry_count,
                SUM(CASE WHEN readout.trade_action = 'full_exit' AND readout.execution_status = 'filled' THEN 1 ELSE 0 END) AS full_exit_count,
                COALESCE(active_positions.active_symbol_count, 0) AS active_symbol_count,
                COUNT(*) AS execution_count,
                SUM(CASE WHEN readout.execution_status = 'filled' THEN 1 ELSE 0 END) AS filled_count,
                SUM(CASE WHEN readout.execution_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count,
                COUNT(DISTINCT readout.symbol) AS symbol_count,
                SUM(ABS(readout.executed_weight)) AS gross_executed_weight,
                MAX(readout.execution_trade_date) AS latest_execution_trade_date,
                ? AS system_contract_version,
                ? AS last_materialized_run_id
            FROM system_trade_readout AS readout
            LEFT JOIN (
                SELECT portfolio_id, COUNT(DISTINCT symbol) AS active_symbol_count
                FROM system_trade_source.trade_position_leg
                WHERE portfolio_id = ?
                    AND position_state = 'open'
                    AND active_weight > 0
                GROUP BY portfolio_id
            ) AS active_positions
                ON active_positions.portfolio_id = readout.portfolio_id
            WHERE readout.portfolio_id = ?
            GROUP BY readout.portfolio_id, active_positions.active_symbol_count
            """,
            [SYSTEM_CONTRACT_VERSION, run_id, portfolio_id, portfolio_id],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    work_units_updated = int(connection.execute("SELECT COUNT(*) FROM system_work_units_to_update").fetchone()[0])
    latest_execution_trade_date = connection.execute(
        "SELECT MAX(latest_execution_trade_date) FROM system_source_work_unit_summary"
    ).fetchone()[0]
    return work_units_updated, latest_execution_trade_date


def _insert_system_work_queue_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    status_table_name: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO system_work_queue (
            queue_id, portfolio_id, symbol, status, source_row_count,
            latest_execution_trade_date, source_fingerprint, claimed_at, finished_at
        )
        SELECT
            CONCAT(?, ':', portfolio_id, ':', symbol),
            portfolio_id,
            symbol,
            status,
            source_row_count,
            latest_execution_trade_date,
            source_fingerprint,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM {status_table_name}
        """,
        [run_id],
    )


def _upsert_system_checkpoint_sql(*, connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    connection.execute(
        """
        INSERT INTO system_checkpoint (
            portfolio_id, symbol, latest_execution_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT
            portfolio_id,
            symbol,
            latest_execution_trade_date,
            source_fingerprint,
            ?,
            CURRENT_TIMESTAMP
        FROM system_source_work_unit_summary
        ON CONFLICT(portfolio_id, symbol) DO UPDATE
        SET
            latest_execution_trade_date = excluded.latest_execution_trade_date,
            last_source_fingerprint = excluded.last_source_fingerprint,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [run_id],
    )


def _duckdb_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
