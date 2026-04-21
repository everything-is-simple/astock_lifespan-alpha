"""Stage-five trade runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.trade.contracts import (
    EXECUTION_PRICE_LINE,
    TRADE_CONTRACT_VERSION,
    TradeCheckpointSummary,
    TradeExecutionRecord,
    TradeMaterializationAction,
    TradeRunStatus,
    TradeRunSummary,
)
from astock_lifespan_alpha.trade.engine import materialize_trade_work_unit
from astock_lifespan_alpha.trade.schema import initialize_trade_schema
from astock_lifespan_alpha.trade.source import load_trade_source_rows


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
    """Build the minimal portfolio_plan -> trade execution ledger."""

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
            position_action_decision,
            requested_weight,
            admitted_weight,
            trimmed_weight,
            plan_status,
            blocking_reason_code
        FROM trade_plan_source.portfolio_plan_snapshot
        WHERE portfolio_id = {portfolio_id_literal}
        """,
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
        CREATE OR REPLACE TEMP TABLE trade_source_work_unit_summary AS
        SELECT
            portfolio_id,
            symbol,
            COUNT(*) AS source_row_count,
            MAX(reference_trade_date) AS last_reference_trade_date,
            md5(
                string_agg(
                    CONCAT(
                        plan_snapshot_nk, '|', candidate_nk, '|', COALESCE(CAST(reference_trade_date AS VARCHAR), 'None'),
                        '|', position_action_decision, '|', requested_weight, '|', admitted_weight, '|', trimmed_weight,
                        '|', plan_status, '|', COALESCE(blocking_reason_code, '')
                    ),
                    '||'
                    ORDER BY reference_trade_date, candidate_nk, plan_snapshot_nk
                )
            ) AS source_fingerprint
        FROM trade_plan_source_rows
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
                price.trade_date AS planned_trade_date,
                price.open_price AS execution_price
            FROM trade_plan_source_rows plan
            ASOF LEFT JOIN trade_execution_price_source price
                ON price.symbol = plan.symbol
                AND plan.reference_trade_date < price.trade_date
        ),
        reasoned AS (
            SELECT
                *,
                CASE
                    WHEN plan_status = 'blocked' THEN COALESCE(blocking_reason_code, 'plan_blocked')
                    WHEN plan_status NOT IN ('admitted', 'trimmed') THEN COALESCE(blocking_reason_code, 'unsupported_plan_status')
                    WHEN position_action_decision != 'open' THEN 'unsupported_position_action'
                    WHEN admitted_weight <= 0 THEN 'invalid_admitted_weight'
                    WHEN reference_trade_date IS NULL THEN 'missing_reference_trade_date'
                    WHEN planned_trade_date IS NULL THEN 'missing_next_execution_trade_date'
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
                ':',
                COALESCE(CAST(planned_trade_date AS VARCHAR), 'no_execution_date'),
                ':',
                ?
            ) AS order_intent_nk,
            plan_snapshot_nk,
            candidate_nk,
            portfolio_id,
            symbol,
            reference_trade_date,
            planned_trade_date,
            position_action_decision,
            CASE WHEN trade_blocking_reason_code IS NULL THEN 'planned' ELSE 'blocked' END AS intent_status,
            requested_weight,
            admitted_weight,
            CASE WHEN trade_blocking_reason_code IS NULL THEN ROUND(admitted_weight, 8) ELSE 0.0 END AS execution_weight,
            trade_blocking_reason_code AS blocking_reason_code,
            execution_price
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
            order_intent_nk,
            portfolio_id,
            symbol,
            CASE WHEN blocking_reason_code IS NULL THEN 'filled' ELSE 'rejected' END AS execution_status,
            planned_trade_date AS execution_trade_date,
            execution_price,
            CASE WHEN blocking_reason_code IS NULL THEN ROUND(admitted_weight, 8) ELSE 0.0 END AS executed_weight,
            blocking_reason_code,
            ? AS source_price_line
        FROM trade_materialized_intent
        """,
        [EXECUTION_PRICE_LINE],
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_intent_with_action AS
        SELECT
            materialized.*,
            CASE
                WHEN existing.order_intent_nk IS NULL THEN 'inserted'
                WHEN existing.plan_snapshot_nk = materialized.plan_snapshot_nk
                    AND existing.candidate_nk = materialized.candidate_nk
                    AND existing.portfolio_id = materialized.portfolio_id
                    AND existing.symbol = materialized.symbol
                    AND existing.reference_trade_date IS NOT DISTINCT FROM materialized.reference_trade_date
                    AND existing.planned_trade_date IS NOT DISTINCT FROM materialized.planned_trade_date
                    AND existing.position_action_decision = materialized.position_action_decision
                    AND existing.intent_status = materialized.intent_status
                    AND existing.requested_weight = materialized.requested_weight
                    AND existing.admitted_weight = materialized.admitted_weight
                    AND existing.execution_weight = materialized.execution_weight
                    AND existing.blocking_reason_code IS NOT DISTINCT FROM materialized.blocking_reason_code
                    THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM trade_materialized_intent materialized
        LEFT JOIN trade_order_intent existing
            ON existing.order_intent_nk = materialized.order_intent_nk
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE trade_materialized_execution_with_action AS
        SELECT
            materialized.*,
            CASE
                WHEN existing.order_execution_nk IS NULL THEN 'inserted'
                WHEN existing.order_intent_nk = materialized.order_intent_nk
                    AND existing.portfolio_id = materialized.portfolio_id
                    AND existing.symbol = materialized.symbol
                    AND existing.execution_status = materialized.execution_status
                    AND existing.execution_trade_date IS NOT DISTINCT FROM materialized.execution_trade_date
                    AND existing.execution_price IS NOT DISTINCT FROM materialized.execution_price
                    AND existing.executed_weight = materialized.executed_weight
                    AND existing.blocking_reason_code IS NOT DISTINCT FROM materialized.blocking_reason_code
                    AND existing.source_price_line = materialized.source_price_line
                    THEN 'reused'
                ELSE 'rematerialized'
            END AS materialization_action
        FROM trade_materialized_execution materialized
        LEFT JOIN trade_order_execution existing
            ON existing.order_execution_nk = materialized.order_execution_nk
        """
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
                WHEN SUM(CASE WHEN intent.materialization_action != 'reused' THEN 1 ELSE 0 END) = 0
                    AND SUM(CASE WHEN execution.materialization_action != 'reused' THEN 1 ELSE 0 END) = 0
                    THEN 'reused'
                ELSE 'completed'
            END AS status
        FROM trade_source_work_unit_summary summary
        LEFT JOIN trade_materialized_intent_with_action intent
            ON intent.portfolio_id = summary.portfolio_id
            AND intent.symbol = summary.symbol
        LEFT JOIN trade_materialized_execution_with_action execution
            ON execution.order_intent_nk = intent.order_intent_nk
        GROUP BY summary.portfolio_id, summary.symbol, summary.source_row_count,
            summary.last_reference_trade_date, summary.source_fingerprint
        """
    )
    intent_counts = dict(
        connection.execute(
            """
            SELECT materialization_action, COUNT(*)
            FROM trade_materialized_intent_with_action
            GROUP BY materialization_action
            """
        ).fetchall()
    )
    execution_counts = dict(
        connection.execute(
            """
            SELECT materialization_action, COUNT(*)
            FROM trade_materialized_execution_with_action
            GROUP BY materialization_action
            """
        ).fetchall()
    )
    work_units_updated = int(
        connection.execute("SELECT COUNT(*) FROM trade_work_unit_actions WHERE status != 'reused'").fetchone()[0]
    )
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            "DELETE FROM trade_order_execution WHERE (portfolio_id, symbol) IN (SELECT portfolio_id, symbol FROM trade_source_work_unit_summary)"
        )
        connection.execute(
            "DELETE FROM trade_order_intent WHERE (portfolio_id, symbol) IN (SELECT portfolio_id, symbol FROM trade_source_work_unit_summary)"
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
                CASE WHEN materialization_action = 'inserted' THEN ? ELSE ? END,
                ?
            FROM trade_materialized_intent_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, _first_seen_for_rematerialized(run_id), run_id],
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
                CASE WHEN materialization_action = 'inserted' THEN ? ELSE ? END,
                ?
            FROM trade_materialized_execution_with_action
            """,
            [TRADE_CONTRACT_VERSION, run_id, _first_seen_for_rematerialized(run_id), run_id],
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
        },
        work_units_updated,
        latest_reference_trade_date,
    )


def _trade_checkpoint_fast_path_available(*, connection: duckdb.DuckDBPyConnection) -> bool:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS work_unit_count,
            COALESCE(SUM(source.source_row_count), 0) AS source_row_count,
            SUM(
                CASE
                    WHEN checkpoint.portfolio_id IS NOT NULL
                        AND checkpoint.last_reference_trade_date IS NOT DISTINCT FROM source.last_reference_trade_date
                        AND checkpoint.last_source_fingerprint = source.source_fingerprint
                        THEN 1
                    ELSE 0
                END
            ) AS matching_checkpoint_count
        FROM trade_source_work_unit_summary source
        LEFT JOIN trade_checkpoint checkpoint
            ON checkpoint.portfolio_id = source.portfolio_id
            AND checkpoint.symbol = source.symbol
        """
    ).fetchone()
    work_unit_count = int(row[0] or 0)
    source_row_count = int(row[1] or 0)
    matching_checkpoint_count = int(row[2] or 0)
    if work_unit_count == 0 or matching_checkpoint_count != work_unit_count:
        return False
    intent_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_intent intent
        INNER JOIN trade_source_work_unit_summary source
            ON source.portfolio_id = intent.portfolio_id
            AND source.symbol = intent.symbol
        """
    ).fetchone()[0]
    execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_execution execution
        INNER JOIN trade_source_work_unit_summary source
            ON source.portfolio_id = execution.portfolio_id
            AND source.symbol = execution.symbol
        """
    ).fetchone()[0]
    return int(intent_count) == source_row_count and int(execution_count) == source_row_count


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
            FROM trade_order_intent intent
            INNER JOIN trade_source_work_unit_summary source
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
    intent_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_intent intent
        INNER JOIN trade_source_work_unit_summary source
            ON source.portfolio_id = intent.portfolio_id
            AND source.symbol = intent.symbol
        """
    ).fetchone()[0]
    execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_execution execution
        INNER JOIN trade_source_work_unit_summary source
            ON source.portfolio_id = execution.portfolio_id
            AND source.symbol = execution.symbol
        """
    ).fetchone()[0]
    latest_reference_trade_date = connection.execute(
        "SELECT MAX(last_reference_trade_date) FROM trade_source_work_unit_summary"
    ).fetchone()[0]
    return (
        {
            "intents_inserted": 0,
            "intents_reused": int(intent_count),
            "intents_rematerialized": 0,
            "executions_inserted": 0,
            "executions_reused": int(execution_count),
            "executions_rematerialized": 0,
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


def _max_reference_trade_date(rows) -> date | None:
    dates = [row.reference_trade_date for row in rows if row.reference_trade_date is not None]
    return max(dates) if dates else None


def _load_trade_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    symbol: str,
) -> dict[str, object] | None:
    row = connection.execute(
        """
        SELECT last_reference_trade_date, last_source_fingerprint
        FROM trade_checkpoint
        WHERE portfolio_id = ? AND symbol = ?
        """,
        [portfolio_id, symbol],
    ).fetchone()
    if row is None:
        return None
    return {"last_reference_trade_date": row[0], "last_source_fingerprint": row[1]}


def _record_reused_work_unit(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    portfolio_id: str,
    symbol: str,
) -> tuple[int, int]:
    intent_rows = connection.execute(
        """
        SELECT order_intent_nk, intent_status
        FROM trade_order_intent
        WHERE portfolio_id = ? AND symbol = ?
        ORDER BY order_intent_nk
        """,
        [portfolio_id, symbol],
    ).fetchall()
    for order_intent_nk, intent_status in intent_rows:
        connection.execute(
            """
            INSERT INTO trade_run_order_intent (
                run_id, order_intent_nk, intent_status, materialization_action
            ) VALUES (?, ?, ?, ?)
            """,
            [run_id, order_intent_nk, intent_status, TradeMaterializationAction.REUSED.value],
        )
    execution_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM trade_order_execution
        WHERE portfolio_id = ? AND symbol = ?
        """,
        [portfolio_id, symbol],
    ).fetchone()[0]
    return len(intent_rows), int(execution_count)


def _load_existing_intent_signatures(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    symbol: str,
) -> dict[str, tuple[object, ...]]:
    rows = connection.execute(
        """
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
            blocking_reason_code
        FROM trade_order_intent
        WHERE portfolio_id = ? AND symbol = ?
        """,
        [portfolio_id, symbol],
    ).fetchall()
    return {row[0]: tuple(row[1:]) for row in rows}


def _load_existing_execution_signatures(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    symbol: str,
) -> dict[str, tuple[object, ...]]:
    rows = connection.execute(
        """
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
            source_price_line
        FROM trade_order_execution
        WHERE portfolio_id = ? AND symbol = ?
        """,
        [portfolio_id, symbol],
    ).fetchall()
    return {row[0]: tuple(row[1:]) for row in rows}


def _replace_trade_work_unit_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    symbol: str,
) -> None:
    connection.execute("DELETE FROM trade_order_execution WHERE portfolio_id = ? AND symbol = ?", [portfolio_id, symbol])
    connection.execute("DELETE FROM trade_order_intent WHERE portfolio_id = ? AND symbol = ?", [portfolio_id, symbol])


def _insert_trade_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    intents: list,
    executions: list[TradeExecutionRecord],
    existing_intents: dict[str, tuple[object, ...]],
    existing_executions: dict[str, tuple[object, ...]],
) -> list[str]:
    intent_actions: list[str] = []
    for intent in intents:
        action = _classify_intent_action(intent=intent, existing_intents=existing_intents)
        first_seen_run_id = run_id if action == TradeMaterializationAction.INSERTED.value else _first_seen_for_rematerialized(run_id)
        connection.execute(
            """
            INSERT INTO trade_order_intent (
                order_intent_nk, plan_snapshot_nk, candidate_nk, portfolio_id, symbol,
                reference_trade_date, planned_trade_date, position_action_decision, intent_status,
                requested_weight, admitted_weight, execution_weight, blocking_reason_code,
                trade_contract_version, first_seen_run_id, last_materialized_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                intent.order_intent_nk,
                intent.plan_snapshot_nk,
                intent.candidate_nk,
                intent.portfolio_id,
                intent.symbol,
                intent.reference_trade_date,
                intent.planned_trade_date,
                intent.position_action_decision,
                intent.intent_status,
                intent.requested_weight,
                intent.admitted_weight,
                intent.execution_weight,
                intent.blocking_reason_code,
                TRADE_CONTRACT_VERSION,
                first_seen_run_id,
                run_id,
            ],
        )
        connection.execute(
            """
            INSERT INTO trade_run_order_intent (
                run_id, order_intent_nk, intent_status, materialization_action
            ) VALUES (?, ?, ?, ?)
            """,
            [run_id, intent.order_intent_nk, intent.intent_status, action],
        )
        intent_actions.append(action)

    for execution in executions:
        action = _classify_execution_action(execution=execution, existing_executions=existing_executions)
        first_seen_run_id = run_id if action == TradeMaterializationAction.INSERTED.value else _first_seen_for_rematerialized(run_id)
        connection.execute(
            """
            INSERT INTO trade_order_execution (
                order_execution_nk, order_intent_nk, portfolio_id, symbol, execution_status,
                execution_trade_date, execution_price, executed_weight, blocking_reason_code,
                source_price_line, trade_contract_version, first_seen_run_id, last_materialized_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                execution.order_execution_nk,
                execution.order_intent_nk,
                execution.portfolio_id,
                execution.symbol,
                execution.execution_status,
                execution.execution_trade_date,
                execution.execution_price,
                execution.executed_weight,
                execution.blocking_reason_code,
                execution.source_price_line,
                TRADE_CONTRACT_VERSION,
                first_seen_run_id,
                run_id,
            ],
        )
    return intent_actions


def _classify_intent_action(*, intent, existing_intents: dict[str, tuple[object, ...]]) -> str:
    existing_signature = existing_intents.get(intent.order_intent_nk)
    if existing_signature is None:
        return TradeMaterializationAction.INSERTED.value
    if existing_signature == intent.signature():
        return TradeMaterializationAction.REMATERIALIZED.value
    return TradeMaterializationAction.REMATERIALIZED.value


def _classify_execution_action(
    *,
    execution: TradeExecutionRecord,
    existing_executions: dict[str, tuple[object, ...]],
) -> str:
    existing_signature = existing_executions.get(execution.order_execution_nk)
    if existing_signature is None:
        return TradeMaterializationAction.INSERTED.value
    if existing_signature == execution.signature():
        return TradeMaterializationAction.REMATERIALIZED.value
    return TradeMaterializationAction.REMATERIALIZED.value


def _first_seen_for_rematerialized(run_id: str) -> str:
    return run_id


def _upsert_trade_checkpoint(
    *,
    connection: duckdb.DuckDBPyConnection,
    portfolio_id: str,
    symbol: str,
    run_id: str,
    last_reference_trade_date: date | None,
    source_fingerprint: str,
) -> None:
    updated_at = datetime.utcnow()
    connection.execute(
        """
        INSERT INTO trade_checkpoint (
            portfolio_id, symbol, last_reference_trade_date, last_source_fingerprint, last_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(portfolio_id, symbol) DO UPDATE
        SET
            last_reference_trade_date = excluded.last_reference_trade_date,
            last_source_fingerprint = excluded.last_source_fingerprint,
            last_run_id = excluded.last_run_id,
            updated_at = excluded.updated_at
        """,
        [portfolio_id, symbol, last_reference_trade_date, source_fingerprint, run_id, updated_at],
    )
