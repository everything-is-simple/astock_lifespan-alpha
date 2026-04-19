"""Stage-five trade runner."""

from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings
from astock_lifespan_alpha.trade.contracts import (
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
    source = load_trade_source_rows(settings=workspace, portfolio_id=portfolio_id)
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
                len(source.rows_by_work_unit),
            ],
        )
        connection.execute("DELETE FROM trade_work_queue")

        for (row_portfolio_id, symbol), rows in source.rows_by_work_unit.items():
            work_unit_last_reference_date = _max_reference_trade_date(rows)
            if work_unit_last_reference_date is not None and (
                latest_reference_trade_date is None or work_unit_last_reference_date > latest_reference_trade_date
            ):
                latest_reference_trade_date = work_unit_last_reference_date

            bundle = materialize_trade_work_unit(
                rows=rows,
                execution_prices=source.execution_prices_by_symbol.get(symbol, []),
            )
            queue_id = f"{run_id}:{row_portfolio_id}:{symbol}"
            connection.execute(
                """
                INSERT INTO trade_work_queue (
                    queue_id, portfolio_id, symbol, status, source_row_count,
                    last_reference_trade_date, source_fingerprint, claimed_at
                ) VALUES (?, ?, ?, 'running', ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    queue_id,
                    row_portfolio_id,
                    symbol,
                    len(rows),
                    work_unit_last_reference_date,
                    bundle.source_fingerprint,
                ],
            )

            checkpoint = _load_trade_checkpoint(
                connection=connection,
                portfolio_id=row_portfolio_id,
                symbol=symbol,
            )
            if (
                checkpoint is not None
                and checkpoint["last_source_fingerprint"] == bundle.source_fingerprint
                and checkpoint["last_reference_trade_date"] == work_unit_last_reference_date
            ):
                reused_intents, reused_executions = _record_reused_work_unit(
                    connection=connection,
                    run_id=run_id,
                    portfolio_id=row_portfolio_id,
                    symbol=symbol,
                )
                counts["intents_reused"] += reused_intents
                counts["executions_reused"] += reused_executions
                _upsert_trade_checkpoint(
                    connection=connection,
                    portfolio_id=row_portfolio_id,
                    symbol=symbol,
                    run_id=run_id,
                    last_reference_trade_date=work_unit_last_reference_date,
                    source_fingerprint=bundle.source_fingerprint,
                )
                connection.execute(
                    """
                    UPDATE trade_work_queue
                    SET status = 'reused', finished_at = CURRENT_TIMESTAMP
                    WHERE queue_id = ?
                    """,
                    [queue_id],
                )
                continue

            existing_intents = _load_existing_intent_signatures(
                connection=connection,
                portfolio_id=row_portfolio_id,
                symbol=symbol,
            )
            existing_executions = _load_existing_execution_signatures(
                connection=connection,
                portfolio_id=row_portfolio_id,
                symbol=symbol,
            )
            _replace_trade_work_unit_rows(connection=connection, portfolio_id=row_portfolio_id, symbol=symbol)
            intent_actions = _insert_trade_rows(
                connection=connection,
                run_id=run_id,
                intents=bundle.intents,
                executions=bundle.executions,
                existing_intents=existing_intents,
                existing_executions=existing_executions,
            )
            for action in intent_actions:
                counts[f"intents_{action}"] += 1
            for execution in bundle.executions:
                action = _classify_execution_action(
                    execution=execution,
                    existing_executions=existing_executions,
                )
                counts[f"executions_{action}"] += 1
            _upsert_trade_checkpoint(
                connection=connection,
                portfolio_id=row_portfolio_id,
                symbol=symbol,
                run_id=run_id,
                last_reference_trade_date=work_unit_last_reference_date,
                source_fingerprint=bundle.source_fingerprint,
            )
            connection.execute(
                """
                UPDATE trade_work_queue
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE queue_id = ?
                """,
                [queue_id],
            )
            work_units_updated += 1

        if not source.rows_by_work_unit:
            message = "trade schema initialized without portfolio_plan rows."

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
            work_units_seen=len(source.rows_by_work_unit),
            work_units_updated=work_units_updated,
            latest_reference_trade_date=latest_reference_trade_date.isoformat()
            if latest_reference_trade_date is not None
            else None,
        ),
    )


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
