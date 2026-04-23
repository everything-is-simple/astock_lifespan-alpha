"""Stage-five trade runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.trade.contracts import (
    EXECUTION_PRICE_LINE,
    TRADE_CONTRACT_VERSION,
    TradeCheckpointSummary,
    TradeRunStatus,
    TradeRunSummary,
)
from astock_lifespan_alpha.trade.schema import initialize_trade_schema


DAY_TABLE_CANDIDATES = ("stock_daily_adjusted", "market_base_day", "bars_day", "price_bar_day", "market_day")


@dataclass(frozen=True)
class _TradeSourceMetadata:
    portfolio_plan_source_path: Path | None
    execution_price_source_path: Path | None
    row_count: int
    work_unit_count: int


def run_trade_from_portfolio_plan(
    *,
    portfolio_id: str = "core",
    settings: WorkspaceRoots | None = None,
) -> TradeRunSummary:
    """Build the minimal rolling portfolio_plan -> trade execution ledger."""

    workspace = settings or default_settings()
    workspace.ensure_directories()
    target_path = workspace.databases.trade
    initialize_trade_schema(target_path)

    run_id = f"trade-{uuid4().hex[:12]}"
    message = "trade run completed."
    counts = {
        "intents_inserted": 0,
        "intents_reused": 0,
        "intents_rematerialized": 0,
        "executions_inserted": 0,
        "executions_reused": 0,
        "executions_rematerialized": 0,
        "position_legs_inserted": 0,
        "position_legs_reused": 0,
        "position_legs_rematerialized": 0,
        "carry_rows_inserted": 0,
        "carry_rows_reused": 0,
        "carry_rows_rematerialized": 0,
        "exit_rows_inserted": 0,
        "exit_rows_reused": 0,
        "exit_rows_rematerialized": 0,
    }
    work_units_updated = 0
    latest_reference_trade_date: date | None = None

    with duckdb.connect(str(target_path)) as connection:
        source = _attach_trade_source_views(connection=connection, workspace=workspace, portfolio_id=portfolio_id)
        connection.execute(
            """
            INSERT INTO trade_run (
                run_id, status, portfolio_id, source_portfolio_plan_path, source_execution_price_path,
                input_rows, work_units_seen, message
            ) VALUES (?, 'running', ?, ?, ?, ?, ?, 'trade run started.')
            """,
            [
                run_id,
                portfolio_id,
                str(source.portfolio_plan_source_path) if source.portfolio_plan_source_path is not None else None,
                str(source.execution_price_source_path) if source.execution_price_source_path is not None else None,
                source.row_count,
                source.work_unit_count,
            ],
        )
        connection.execute("DELETE FROM trade_work_queue")

        if source.row_count == 0:
            message = "trade schema initialized without portfolio_plan rows."
        else:
            counts, work_units_updated, latest_reference_trade_date = _materialize_trade_sql(
                connection=connection,
                run_id=run_id,
            )

        connection.execute(
            """
            UPDATE trade_run
            SET
                status = ?,
                work_units_updated = ?,
                inserted_order_intents = ?,
                reused_order_intents = ?,
                rematerialized_order_intents = ?,
                inserted_order_executions = ?,
                reused_order_executions = ?,
                rematerialized_order_executions = ?,
                inserted_position_legs = ?,
                reused_position_legs = ?,
                rematerialized_position_legs = ?,
                inserted_carry_rows = ?,
                reused_carry_rows = ?,
                rematerialized_carry_rows = ?,
                inserted_exit_rows = ?,
                reused_exit_rows = ?,
                rematerialized_exit_rows = ?,
                latest_reference_trade_date = ?,
                message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            """,
            [
                TradeRunStatus.COMPLETED.value,
                work_units_updated,
                counts["intents_inserted"],
                counts["intents_reused"],
                counts["intents_rematerialized"],
                counts["executions_inserted"],
                counts["executions_reused"],
                counts["executions_rematerialized"],
                counts["position_legs_inserted"],
                counts["position_legs_reused"],
                counts["position_legs_rematerialized"],
                counts["carry_rows_inserted"],
                counts["carry_rows_reused"],
                counts["carry_rows_rematerialized"],
                counts["exit_rows_inserted"],
                counts["exit_rows_reused"],
                counts["exit_rows_rematerialized"],
                latest_reference_trade_date,
                message,
                run_id,
            ],
        )

    return TradeRunSummary(
        runner_name="run_trade_from_portfolio_plan",
        run_id=run_id,
        status=TradeRunStatus.COMPLETED.value,
        target_path=str(target_path),
        source_paths={
            "portfolio_plan": str(source.portfolio_plan_source_path) if source.portfolio_plan_source_path else None,
            "execution_price_line": str(source.execution_price_source_path) if source.execution_price_source_path else None,
        },
        message=message,
        materialization_counts=counts,
        checkpoint_summary=TradeCheckpointSummary(
            work_units_seen=source.work_unit_count,
            work_units_updated=work_units_updated,
            latest_reference_trade_date=latest_reference_trade_date.isoformat()
            if latest_reference_trade_date is not None
            else None,
        ),
    )


def _attach_trade_source_views(
    *,
    connection: duckdb.DuckDBPyConnection,
    workspace: WorkspaceRoots,
    portfolio_id: str,
) -> _TradeSourceMetadata:
    portfolio_plan_path = workspace.databases.portfolio_plan if workspace.databases.portfolio_plan.exists() else None
    execution_price_path = workspace.source_databases.market_base if workspace.source_databases.market_base.exists() else None
    if portfolio_plan_path is None:
        return _TradeSourceMetadata(
            portfolio_plan_source_path=portfolio_plan_path,
            execution_price_source_path=execution_price_path,
            row_count=0,
            work_unit_count=0,
        )

    connection.execute(f"ATTACH {_duckdb_string_literal(portfolio_plan_path)} AS trade_plan_source (READ_ONLY)")
    if not _attached_table_exists(connection=connection, catalog="trade_plan_source", table_name="portfolio_plan_snapshot"):
        return _TradeSourceMetadata(
            portfolio_plan_source_path=portfolio_plan_path,
            execution_price_source_path=execution_price_path,
            row_count=0,
            work_unit_count=0,
        )
    portfolio_id_literal = _duckdb_string_literal(portfolio_id)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW trade_plan_source_rows AS
        SELECT
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            CAST(reference_trade_date AS DATE) AS reference_trade_date,
            CAST(planned_entry_trade_date AS DATE) AS planned_entry_trade_date,
            CAST(scheduled_exit_trade_date AS DATE) AS scheduled_exit_trade_date,
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code,
            planned_exit_reason_code
        FROM trade_plan_source.portfolio_plan_snapshot
        WHERE portfolio_id = {portfolio_id_literal}
        """
    )
    if execution_price_path is not None:
        connection.execute(f"ATTACH {_duckdb_string_literal(execution_price_path)} AS trade_price_source (READ_ONLY)")
        market_source = _resolve_market_source(connection=connection, catalog="trade_price_source")
        if market_source is not None:
            connection.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW trade_execution_price_source AS
                {_market_select_sql(market_source, catalog="trade_price_source")}
                """
            )
    if not _temp_view_exists(connection=connection, view_name="trade_execution_price_source"):
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW trade_execution_price_source AS
            SELECT
                CAST(NULL AS VARCHAR) AS symbol,
                CAST(NULL AS DATE) AS trade_date,
                CAST(NULL AS DOUBLE) AS open_price
            WHERE FALSE
            """
        )
    row_count, work_unit_count = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT portfolio_id || ':' || symbol) FROM trade_plan_source_rows"
    ).fetchone()
    return _TradeSourceMetadata(
        portfolio_plan_source_path=portfolio_plan_path,
        execution_price_source_path=execution_price_path,
        row_count=int(row_count),
        work_unit_count=int(work_unit_count),
    )


@dataclass(frozen=True)
class _MarketSource:
    table_name: str
    symbol_column: str
    date_column: str
    has_adjust_method: bool


def _resolve_market_source(*, connection: duckdb.DuckDBPyConnection, catalog: str) -> _MarketSource | None:
    available_tables = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?",
            [catalog],
        ).fetchall()
    }
    for table_name in DAY_TABLE_CANDIDATES:
        if table_name not in available_tables:
            continue
        column_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_catalog = ? AND table_name = ?
                """,
                [catalog, table_name],
            ).fetchall()
        }
        if "open" not in column_names:
            return None
        return _MarketSource(
            table_name=table_name,
            symbol_column=_pick_required_column(column_names, ("symbol", "code")),
            date_column=_pick_required_column(column_names, ("bar_dt", "trade_date", "date")),
            has_adjust_method="adjust_method" in column_names,
        )
    return None


def _market_select_sql(source: _MarketSource, *, catalog: str) -> str:
    adjust_filter = "WHERE adjust_method = 'backward'" if source.has_adjust_method else ""
    return f"""
        SELECT
            {source.symbol_column} AS symbol,
            CAST({source.date_column} AS DATE) AS trade_date,
            CAST(open AS DOUBLE) AS open_price
        FROM {catalog}.{source.table_name}
        {adjust_filter}
    """


def _materialize_trade_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> tuple[dict[str, int], int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_source_work_unit_rows AS
        SELECT
            portfolio_id,
            symbol,
            reference_trade_date,
            planned_entry_trade_date,
            candidate_nk,
            plan_snapshot_nk,
            md5(
                CONCAT(
                    plan_snapshot_nk, '|', candidate_nk, '|',
                    COALESCE(CAST(reference_trade_date AS VARCHAR), 'None'), '|',
                    COALESCE(CAST(planned_entry_trade_date AS VARCHAR), 'None'), '|',
                    COALESCE(CAST(scheduled_exit_trade_date AS VARCHAR), 'None'), '|',
                    position_action_decision, '|', requested_weight, '|', admitted_weight, '|', trimmed_weight,
                    '|', plan_status, '|', COALESCE(blocking_reason_code, ''), '|', COALESCE(planned_exit_reason_code, '')
                )
            ) AS row_fingerprint
        FROM trade_plan_source_rows
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_source_work_unit_summary AS
        SELECT
            portfolio_id,
            symbol,
            COUNT(*) AS source_row_count,
            MAX(reference_trade_date) AS last_reference_trade_date,
            md5(
                string_agg(
                    row_fingerprint,
                    '||'
                    ORDER BY planned_entry_trade_date, candidate_nk, plan_snapshot_nk
                )
            ) AS source_fingerprint
        FROM trade_source_work_unit_rows
        GROUP BY portfolio_id, symbol
        """
    )
    if _trade_checkpoint_fast_path_available(connection=connection):
        return _record_reused_trade_sql(connection=connection, run_id=run_id)

    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_intent AS
        WITH planned AS (
            SELECT
                plan.*,
                COALESCE(plan.planned_entry_trade_date, fallback_price.trade_date) AS effective_planned_trade_date,
                CASE
                    WHEN plan.planned_entry_trade_date IS NOT NULL THEN explicit_price.open_price
                    ELSE fallback_price.open_price
                END AS execution_price,
                CASE
                    WHEN plan.planned_entry_trade_date IS NOT NULL THEN explicit_price.trade_date
                    ELSE fallback_price.trade_date
                END AS execution_trade_date
            FROM trade_plan_source_rows AS plan
            LEFT JOIN trade_execution_price_source AS explicit_price
                ON explicit_price.symbol = plan.symbol
                AND explicit_price.trade_date = plan.planned_entry_trade_date
            ASOF LEFT JOIN trade_execution_price_source AS fallback_price
                ON fallback_price.symbol = plan.symbol
                AND plan.reference_trade_date < fallback_price.trade_date
        ),
        reasoned AS (
            SELECT
                *,
                CASE
                    WHEN plan_status = 'blocked' THEN COALESCE(blocking_reason_code, 'plan_blocked')
                    WHEN plan_status NOT IN ('admitted', 'trimmed') THEN COALESCE(blocking_reason_code, 'unsupported_plan_status')
                    WHEN position_action_decision != 'open' THEN 'unsupported_position_action'
                    WHEN admitted_weight <= 0 THEN 'invalid_admitted_weight'
                    WHEN effective_planned_trade_date IS NULL THEN 'missing_next_execution_trade_date'
                    WHEN execution_trade_date IS NULL THEN 'missing_next_execution_trade_date'
                    WHEN execution_price IS NULL THEN 'missing_execution_open_price'
                    ELSE NULL
                END AS trade_blocking_reason_code
            FROM planned
        )
        SELECT
            CONCAT(
                portfolio_id,
                ':',
                candidate_nk,
                ':entry:',
                COALESCE(CAST(effective_planned_trade_date AS VARCHAR), 'no_execution_date'),
                ':',
                ?
            ) AS order_intent_nk,
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            reference_trade_date,
            effective_planned_trade_date AS planned_trade_date,
            position_action_decision,
            CASE WHEN trade_blocking_reason_code IS NULL THEN 'planned' ELSE 'blocked' END AS intent_status,
            requested_weight,
            admitted_weight,
            CASE WHEN trade_blocking_reason_code IS NULL THEN ROUND(admitted_weight, 8) ELSE 0.0 END AS execution_weight,
            trade_blocking_reason_code AS blocking_reason_code,
            scheduled_exit_trade_date,
            planned_exit_reason_code
        FROM reasoned
        """,
        [TRADE_CONTRACT_VERSION],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_execution AS
        SELECT
            CONCAT(
                order_intent_nk,
                ':',
                COALESCE(CAST(planned_trade_date AS VARCHAR), 'no_execution_date'),
                ':',
                CASE WHEN blocking_reason_code IS NULL THEN 'filled' ELSE 'rejected' END
            ) AS order_execution_nk,
            intent.order_intent_nk,
            intent.candidate_nk,
            intent.portfolio_id,
            intent.symbol,
            CASE WHEN intent.blocking_reason_code IS NULL THEN 'filled' ELSE 'rejected' END AS execution_status,
            intent.planned_trade_date AS execution_trade_date,
            price.open_price AS execution_price,
            CASE WHEN intent.blocking_reason_code IS NULL THEN ROUND(intent.execution_weight, 8) ELSE 0.0 END AS executed_weight,
            intent.blocking_reason_code,
            ? AS source_price_line
        FROM trade_materialized_intent AS intent
        LEFT JOIN trade_execution_price_source AS price
            ON price.symbol = intent.symbol
            AND price.trade_date = intent.planned_trade_date
        """,
        [EXECUTION_PRICE_LINE],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_exit_execution AS
        SELECT
            CONCAT(
                intent.portfolio_id,
                ':',
                intent.candidate_nk,
                ':exit:',
                COALESCE(CAST(intent.scheduled_exit_trade_date AS VARCHAR), 'no_exit_date'),
                ':',
                ?
            ) AS exit_execution_nk,
            CONCAT(intent.portfolio_id, ':', intent.candidate_nk, ':leg') AS position_leg_nk,
            intent.candidate_nk,
            intent.portfolio_id,
            intent.symbol,
            intent.scheduled_exit_trade_date AS exit_trade_date,
            CASE
                WHEN intent.blocking_reason_code IS NOT NULL THEN 'rejected'
                WHEN exit_trade_date IS NULL THEN 'rejected'
                WHEN price.open_price IS NULL THEN 'rejected'
                ELSE 'filled'
            END AS execution_status,
            price.open_price AS execution_price,
            CASE
                WHEN intent.blocking_reason_code IS NULL AND exit_trade_date IS NOT NULL AND price.open_price IS NOT NULL
                    THEN ROUND(intent.admitted_weight, 8)
                ELSE 0.0
            END AS exited_weight,
            CASE
                WHEN intent.blocking_reason_code IS NOT NULL THEN 'entry_not_filled'
                WHEN exit_trade_date IS NULL THEN 'missing_exit_execution_trade_date'
                WHEN price.open_price IS NULL THEN 'missing_execution_open_price'
                ELSE NULL
            END AS blocking_reason_code,
            intent.planned_exit_reason_code AS exit_reason_code,
            ? AS source_price_line
        FROM trade_materialized_intent AS intent
        LEFT JOIN trade_execution_price_source AS price
            ON price.symbol = intent.symbol
            AND price.trade_date = intent.scheduled_exit_trade_date
        WHERE intent.admitted_weight > 0
            AND intent.scheduled_exit_trade_date IS NOT NULL
        """,
        [TRADE_CONTRACT_VERSION, EXECUTION_PRICE_LINE],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_position_leg AS
        SELECT
            CONCAT(intent.portfolio_id, ':', intent.candidate_nk, ':leg') AS position_leg_nk,
            intent.candidate_nk,
            intent.order_intent_nk,
            intent.portfolio_id,
            intent.symbol,
            intent.reference_trade_date AS entry_reference_trade_date,
            execution.execution_trade_date AS entry_trade_date,
            execution.execution_price AS entry_execution_price,
            intent.execution_weight AS position_weight,
            intent.scheduled_exit_trade_date,
            CASE
                WHEN execution.execution_status != 'filled' THEN 'entry_rejected'
                WHEN exit_execution.execution_status = 'filled' THEN 'closed'
                ELSE 'open'
            END AS position_state,
            exit_execution.exit_execution_nk,
            exit_execution.exit_trade_date,
            exit_execution.execution_price AS exit_execution_price,
            CASE
                WHEN execution.execution_status = 'filled'
                    AND COALESCE(exit_execution.execution_status, 'open') != 'filled'
                    THEN ROUND(intent.execution_weight, 8)
                ELSE 0.0
            END AS active_weight
        FROM trade_materialized_intent AS intent
        INNER JOIN trade_materialized_execution AS execution
            ON execution.order_intent_nk = intent.order_intent_nk
        LEFT JOIN trade_materialized_exit_execution AS exit_execution
            ON exit_execution.portfolio_id = intent.portfolio_id
            AND exit_execution.candidate_nk = intent.candidate_nk
        WHERE intent.admitted_weight > 0
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_carry_snapshot AS
        SELECT
            CONCAT(position_leg_nk, ':open') AS carry_snapshot_nk,
            position_leg_nk,
            portfolio_id,
            symbol,
            entry_trade_date AS as_of_trade_date,
            'open' AS carry_status,
            CASE WHEN position_state != 'entry_rejected' THEN position_weight ELSE 0.0 END AS carried_weight
        FROM trade_materialized_position_leg
        WHERE entry_trade_date IS NOT NULL

        UNION ALL

        SELECT
            CONCAT(position_leg_nk, ':close') AS carry_snapshot_nk,
            position_leg_nk,
            portfolio_id,
            symbol,
            COALESCE(exit_trade_date, scheduled_exit_trade_date) AS as_of_trade_date,
            CASE WHEN position_state = 'closed' THEN 'closed' ELSE 'open' END AS carry_status,
            CASE WHEN position_state = 'closed' THEN 0.0 ELSE active_weight END AS carried_weight
        FROM trade_materialized_position_leg
        WHERE scheduled_exit_trade_date IS NOT NULL
        """
    )
    _classify_trade_actions(connection=connection, run_id=run_id)

    intent_counts = _action_counts(connection=connection, table_name="trade_materialized_intent_with_action")
    execution_counts = _action_counts(connection=connection, table_name="trade_materialized_execution_with_action")
    position_leg_counts = _action_counts(connection=connection, table_name="trade_materialized_position_leg_with_action")
    carry_counts = _action_counts(connection=connection, table_name="trade_materialized_carry_snapshot_with_action")
    exit_counts = _action_counts(connection=connection, table_name="trade_materialized_exit_execution_with_action")
    work_units_updated = int(
        connection.execute("SELECT COUNT(*) FROM trade_work_unit_actions WHERE status != 'reused'").fetchone()[0]
    )
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            DELETE FROM trade_order_execution
            WHERE portfolio_id IN (SELECT portfolio_id FROM trade_source_work_unit_summary)
                AND symbol IN (SELECT symbol FROM trade_source_work_unit_summary)
            """
        )
        connection.execute(
            """
            DELETE FROM trade_order_intent
            WHERE portfolio_id IN (SELECT portfolio_id FROM trade_source_work_unit_summary)
                AND symbol IN (SELECT symbol FROM trade_source_work_unit_summary)
            """
        )
        connection.execute(
            """
            DELETE FROM trade_position_leg
            WHERE portfolio_id IN (SELECT portfolio_id FROM trade_source_work_unit_summary)
                AND symbol IN (SELECT symbol FROM trade_source_work_unit_summary)
            """
        )
        connection.execute(
            """
            DELETE FROM trade_carry_snapshot
            WHERE portfolio_id IN (SELECT portfolio_id FROM trade_source_work_unit_summary)
                AND symbol IN (SELECT symbol FROM trade_source_work_unit_summary)
            """
        )
        connection.execute(
            """
            DELETE FROM trade_exit_execution
            WHERE portfolio_id IN (SELECT portfolio_id FROM trade_source_work_unit_summary)
                AND symbol IN (SELECT symbol FROM trade_source_work_unit_summary)
            """
        )
        connection.execute(
            """
            INSERT INTO trade_order_intent (
                order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
                reference_trade_date, planned_trade_date, position_action_decision, intent_status,
                requested_weight, admitted_weight, execution_weight, blocking_reason_code,
                trade_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                order_intent_nk,
                plan_snapshot_nk,
                candidate_nk,
                portfolio_id,
                symbol,
                reference_trade_date,
                planned_trade_date,
                position_action_decision,
                intent_status,
                requested_weight,
                admitted_weight,
                execution_weight,
                blocking_reason_code,
                ?,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM trade_materialized_intent_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, run_id],
        )
        connection.execute(
            """
            INSERT INTO trade_order_execution (
                order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
                execution_trade_date, execution_price, executed_weight, blocking_reason_code,
                source_price_line, trade_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                order_execution_nk,
                order_intent_nk,
                portfolio_id,
                symbol,
                execution_status,
                execution_trade_date,
                execution_price,
                executed_weight,
                blocking_reason_code,
                source_price_line,
                ?,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM trade_materialized_execution_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, run_id],
        )
        connection.execute(
            """
            INSERT INTO trade_position_leg (
                position_leg_nk, candidate_nk, order_intent_nk, portfolio_id, symbol,
                entry_reference_trade_date, entry_trade_date, entry_execution_price, position_weight,
                scheduled_exit_trade_date, position_state, exit_execution_nk, exit_trade_date,
                exit_execution_price, active_weight, trade_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                position_leg_nk,
                candidate_nk,
                order_intent_nk,
                portfolio_id,
                symbol,
                entry_reference_trade_date,
                entry_trade_date,
                entry_execution_price,
                position_weight,
                scheduled_exit_trade_date,
                position_state,
                exit_execution_nk,
                exit_trade_date,
                exit_execution_price,
                active_weight,
                ?,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM trade_materialized_position_leg_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, run_id],
        )
        connection.execute(
            """
            INSERT INTO trade_carry_snapshot (
                carry_snapshot_nk, position_leg_nk, portfolio_id, symbol, as_of_trade_date,
                carry_status, carried_weight, trade_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                carry_snapshot_nk,
                position_leg_nk,
                portfolio_id,
                symbol,
                as_of_trade_date,
                carry_status,
                carried_weight,
                ?,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM trade_materialized_carry_snapshot_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, run_id],
        )
        connection.execute(
            """
            INSERT INTO trade_exit_execution (
                exit_execution_nk, position_leg_nk, candidate_nk, portfolio_id, symbol,
                exit_trade_date, execution_status, execution_price, exited_weight,
                blocking_reason_code, exit_reason_code, source_price_line,
                trade_contract_version, first_seen_run_id, last_materialized_run_id
            )
            SELECT
                exit_execution_nk,
                position_leg_nk,
                candidate_nk,
                portfolio_id,
                symbol,
                exit_trade_date,
                execution_status,
                execution_price,
                exited_weight,
                blocking_reason_code,
                exit_reason_code,
                source_price_line,
                ?,
                COALESCE(existing_first_seen_run_id, ?),
                ?
            FROM trade_materialized_exit_execution_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, run_id],
        )
        connection.execute(
            """
            INSERT INTO trade_run_order_intent (
                run_id, order_intent_nk, intent_status, materialization_action
            )
            SELECT ?, order_intent_nk, intent_status, materialization_action
            FROM trade_materialized_intent_with_action
            """,
            [run_id],
        )
        _insert_trade_work_queue_sql(connection=connection, run_id=run_id, action_table_name="trade_work_unit_actions")
        _upsert_trade_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return (
        {
            "intents_inserted": int(intent_counts.get("inserted", 0)),
            "intents_reused": int(intent_counts.get("reused", 0)),
            "intents_rematerialized": int(intent_counts.get("rematerialized", 0)),
            "executions_inserted": int(execution_counts.get("inserted", 0)),
            "executions_reused": int(execution_counts.get("reused", 0)),
            "executions_rematerialized": int(execution_counts.get("rematerialized", 0)),
            "position_legs_inserted": int(position_leg_counts.get("inserted", 0)),
            "position_legs_reused": int(position_leg_counts.get("reused", 0)),
            "position_legs_rematerialized": int(position_leg_counts.get("rematerialized", 0)),
            "carry_rows_inserted": int(carry_counts.get("inserted", 0)),
            "carry_rows_reused": int(carry_counts.get("reused", 0)),
            "carry_rows_rematerialized": int(carry_counts.get("rematerialized", 0)),
            "exit_rows_inserted": int(exit_counts.get("inserted", 0)),
            "exit_rows_reused": int(exit_counts.get("reused", 0)),
            "exit_rows_rematerialized": int(exit_counts.get("rematerialized", 0)),
        },
        work_units_updated,
        latest_reference_trade_date,
    )


def _classify_trade_actions(*, connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_intent",
        existing_table="trade_order_intent",
        key_column="order_intent_nk",
        compare_columns=[
            "plan_snapshot_nk",
            "candidate_nk",
            "portfolio_id",
            "symbol",
            "reference_trade_date",
            "planned_trade_date",
            "position_action_decision",
            "intent_status",
            "requested_weight",
            "admitted_weight",
            "execution_weight",
            "blocking_reason_code",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_execution",
        existing_table="trade_order_execution",
        key_column="order_execution_nk",
        compare_columns=[
            "order_intent_nk",
            "portfolio_id",
            "symbol",
            "execution_status",
            "execution_trade_date",
            "execution_price",
            "executed_weight",
            "blocking_reason_code",
            "source_price_line",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_position_leg",
        existing_table="trade_position_leg",
        key_column="position_leg_nk",
        compare_columns=[
            "candidate_nk",
            "order_intent_nk",
            "portfolio_id",
            "symbol",
            "entry_reference_trade_date",
            "entry_trade_date",
            "entry_execution_price",
            "position_weight",
            "scheduled_exit_trade_date",
            "position_state",
            "exit_execution_nk",
            "exit_trade_date",
            "exit_execution_price",
            "active_weight",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_carry_snapshot",
        existing_table="trade_carry_snapshot",
        key_column="carry_snapshot_nk",
        compare_columns=[
            "position_leg_nk",
            "portfolio_id",
            "symbol",
            "as_of_trade_date",
            "carry_status",
            "carried_weight",
        ],
    )
    _create_action_table(
        connection=connection,
        materialized_table="trade_materialized_exit_execution",
        existing_table="trade_exit_execution",
        key_column="exit_execution_nk",
        compare_columns=[
            "position_leg_nk",
            "candidate_nk",
            "portfolio_id",
            "symbol",
            "exit_trade_date",
            "execution_status",
            "execution_price",
            "exited_weight",
            "blocking_reason_code",
            "exit_reason_code",
            "source_price_line",
        ],
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_intent_with_action",
        summary_table_name="trade_intent_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_execution_with_action",
        summary_table_name="trade_execution_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_position_leg_with_action",
        summary_table_name="trade_position_leg_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_carry_snapshot_with_action",
        summary_table_name="trade_carry_work_unit_change_summary",
    )
    _create_work_unit_change_summary(
        connection=connection,
        action_table_name="trade_materialized_exit_execution_with_action",
        summary_table_name="trade_exit_work_unit_change_summary",
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_work_unit_actions AS
        SELECT
            summary.portfolio_id,
            summary.symbol,
            summary.source_row_count,
            summary.last_reference_trade_date,
            summary.source_fingerprint,
            CASE
                WHEN COALESCE(intent.changed_row_count, 0) = 0
                    AND COALESCE(execution.changed_row_count, 0) = 0
                    AND COALESCE(leg.changed_row_count, 0) = 0
                    AND COALESCE(carry.changed_row_count, 0) = 0
                    AND COALESCE(exit_execution.changed_row_count, 0) = 0
                    THEN 'reused'
                ELSE 'completed'
            END AS status
        FROM trade_source_work_unit_summary AS summary
        LEFT JOIN trade_intent_work_unit_change_summary AS intent
            ON intent.portfolio_id = summary.portfolio_id
            AND intent.symbol = summary.symbol
        LEFT JOIN trade_execution_work_unit_change_summary AS execution
            ON execution.portfolio_id = summary.portfolio_id
            AND execution.symbol = summary.symbol
        LEFT JOIN trade_position_leg_work_unit_change_summary AS leg
            ON leg.portfolio_id = summary.portfolio_id
            AND leg.symbol = summary.symbol
        LEFT JOIN trade_carry_work_unit_change_summary AS carry
            ON carry.portfolio_id = summary.portfolio_id
            AND carry.symbol = summary.symbol
        LEFT JOIN trade_exit_work_unit_change_summary AS exit_execution
            ON exit_execution.portfolio_id = summary.portfolio_id
            AND exit_execution.symbol = summary.symbol
        """
    )


def _create_action_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    materialized_table: str,
    existing_table: str,
    key_column: str,
    compare_columns: list[str],
) -> None:
    compare_sql = " AND ".join(
        [f"existing.{column} IS NOT DISTINCT FROM materialized.{column}" for column in compare_columns]
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {materialized_table}_with_action AS
        SELECT
            materialized.*,
            existing.first_seen_run_id AS existing_first_seen_run_id,
            CASE
                WHEN existing.{key_column} IS NULL THEN 'inserted'
                WHEN {compare_sql} THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM {materialized_table} AS materialized
        LEFT JOIN {existing_table} AS existing
            ON existing.{key_column} = materialized.{key_column}
        """
    )


def _action_counts(*, connection: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, int]:
    return {
        key: int(value)
        for key, value in connection.execute(
            f"""
            SELECT materialization_action, COUNT(*)
            FROM {table_name}
            GROUP BY materialization_action
            """
        ).fetchall()
    }


def _create_work_unit_change_summary(
    *,
    connection: duckdb.DuckDBPyConnection,
    action_table_name: str,
    summary_table_name: str,
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {summary_table_name} AS
        SELECT
            portfolio_id,
            symbol,
            SUM(CASE WHEN materialization_action != 'reused' THEN 1 ELSE 0 END) AS changed_row_count
        FROM {action_table_name}
        GROUP BY portfolio_id, symbol
        """
    )


def _trade_checkpoint_fast_path_available(*, connection: duckdb.DuckDBPyConnection) -> bool:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS work_unit_count,
            SUM(
                CASE
                    WHEN checkpoint.portfolio_id IS NOT NULL
                        AND checkpoint.last_reference_trade_date IS NOT DISTINCT FROM source.last_reference_trade_date
                        AND checkpoint.last_source_fingerprint = source.source_fingerprint
                        THEN 1
                    ELSE 0
                END
            ) AS matching_checkpoint_count
        FROM trade_source_work_unit_summary AS source
        LEFT JOIN trade_checkpoint AS checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
            AND checkpoint.symbol = source.symbol
        """
    ).fetchone()
    work_unit_count = int(row[0] or 0)
    matching_checkpoint_count = int(row[1] or 0)
    if work_unit_count == 0 or matching_checkpoint_count != work_unit_count:
        return False
    intent_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_intent AS intent
        INNER JOIN trade_source_work_unit_summary AS source
            ON source.portfolio_id = intent.portfolio_id
            AND source.symbol = intent.symbol
        """
    ).fetchone()[0]
    execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_execution AS execution
        INNER JOIN trade_source_work_unit_summary AS source
            ON source.portfolio_id = execution.portfolio_id
            AND source.symbol = execution.symbol
        """
    ).fetchone()[0]
    expected_intent_count = int(connection.execute("SELECT SUM(source_row_count) FROM trade_source_work_unit_summary").fetchone()[0] or 0)
    if int(intent_count) != int(execution_count) or int(intent_count) != expected_intent_count:
        return False
    actionable_row_count, exit_row_count = connection.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE admitted_weight > 0),
            COUNT(*) FILTER (WHERE admitted_weight > 0 AND scheduled_exit_trade_date IS NOT NULL)
        FROM trade_plan_source_rows
        """
    ).fetchone()
    position_leg_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_position_leg AS leg
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = leg.portfolio_id
            AND source.candidate_nk = leg.candidate_nk
        WHERE source.admitted_weight > 0
        """
    ).fetchone()[0]
    exit_execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_exit_execution AS exit_execution
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = exit_execution.portfolio_id
            AND source.candidate_nk = exit_execution.candidate_nk
        WHERE source.admitted_weight > 0
            AND source.scheduled_exit_trade_date IS NOT NULL
        """
    ).fetchone()[0]
    carry_snapshot_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_carry_snapshot AS carry
        INNER JOIN trade_position_leg AS leg
            ON leg.position_leg_nk = carry.position_leg_nk
        INNER JOIN trade_plan_source_rows AS source
            ON source.portfolio_id = leg.portfolio_id
            AND source.candidate_nk = leg.candidate_nk
        WHERE source.admitted_weight > 0
        """
    ).fetchone()[0]
    return (
        int(position_leg_count) == int(actionable_row_count or 0)
        and int(exit_execution_count) == int(exit_row_count or 0)
        and int(carry_snapshot_count) == int((actionable_row_count or 0) + (exit_row_count or 0))
    )


def _record_reused_trade_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> tuple[dict[str, int], int, date | None]:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_work_unit_actions AS
        SELECT
            portfolio_id,
            symbol,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            'reused' AS status
        FROM trade_source_work_unit_summary
        """
    )
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            INSERT INTO trade_run_order_intent (
                run_id, order_intent_nk, intent_status, materialization_action
            )
            SELECT ?, intent.order_intent_nk, intent.intent_status, 'reused'
            FROM trade_order_intent AS intent
            INNER JOIN trade_source_work_unit_summary AS source
                ON source.portfolio_id = intent.portfolio_id
                AND source.symbol = intent.symbol
            """,
            [run_id],
        )
        _insert_trade_work_queue_sql(connection=connection, run_id=run_id, action_table_name="trade_work_unit_actions")
        _upsert_trade_checkpoint_sql(connection=connection, run_id=run_id)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    return (
        {
            "intents_inserted": 0,
            "intents_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_order_intent AS intent
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = intent.portfolio_id
                        AND source.symbol = intent.symbol
                    """
                ).fetchone()[0]
            ),
            "intents_rematerialized": 0,
            "executions_inserted": 0,
            "executions_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_order_execution AS execution
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = execution.portfolio_id
                        AND source.symbol = execution.symbol
                    """
                ).fetchone()[0]
            ),
            "executions_rematerialized": 0,
            "position_legs_inserted": 0,
            "position_legs_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_position_leg AS leg
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = leg.portfolio_id
                        AND source.symbol = leg.symbol
                    """
                ).fetchone()[0]
            ),
            "position_legs_rematerialized": 0,
            "carry_rows_inserted": 0,
            "carry_rows_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_carry_snapshot AS carry
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = carry.portfolio_id
                        AND source.symbol = carry.symbol
                    """
                ).fetchone()[0]
            ),
            "carry_rows_rematerialized": 0,
            "exit_rows_inserted": 0,
            "exit_rows_reused": int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM trade_exit_execution AS exit_execution
                    INNER JOIN trade_source_work_unit_summary AS source
                        ON source.portfolio_id = exit_execution.portfolio_id
                        AND source.symbol = exit_execution.symbol
                    """
                ).fetchone()[0]
            ),
            "exit_rows_rematerialized": 0,
        },
        0,
        latest_reference_trade_date,
    )


def _insert_trade_work_queue_sql(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    action_table_name: str,
) -> None:
    connection.execute(
        f"""
        INSERT INTO trade_work_queue (
            queue_id, portfolio_id, symbol, status, source_row_count,
            last_reference_trade_date, source_fingerprint, claimed_at, finished_at
        )
        SELECT
            CONCAT(?, ':', portfolio_id, ':', symbol),
            portfolio_id,
            symbol,
            status,
            source_row_count,
            last_reference_trade_date,
            source_fingerprint,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM {action_table_name}
        """,
        [run_id],
    )


def _upsert_trade_checkpoint_sql(*, connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    connection.execute(
        """
        INSERT INTO trade_checkpoint (
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        )
        SELECT portfolio_id, symbol, last_reference_trade_date, source_fingerprint, ?, CURRENT_TIMESTAMP
        FROM trade_source_work_unit_summary
        ON CONFLICT(portfolio_id, symbol) DO UPDATE
        SET
            last_reference_trade_date = excluded.last_reference_trade_date,
            last_source_fingerprint = excluded.last_source_fingerprint,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [run_id],
    )


def _attached_table_exists(*, connection: duckdb.DuckDBPyConnection, catalog: str, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = ? AND table_name = ?
        """,
        [catalog, table_name],
    ).fetchone()
    return bool(row[0])


def _temp_view_exists(*, connection: duckdb.DuckDBPyConnection, view_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_views()
        WHERE view_name = ?
        """,
        [view_name],
    ).fetchone()
    return bool(row[0])


def _pick_required_column(column_names: set[str], candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in column_names:
            return candidate
    raise ValueError(f"Could not resolve required source columns from candidates: {candidates}")


def _duckdb_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
