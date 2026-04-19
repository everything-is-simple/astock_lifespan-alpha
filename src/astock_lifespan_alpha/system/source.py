"""Source adapters for stage-six system readout runners."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from astock_lifespan_alpha.core.paths import WorkspaceRoots
from astock_lifespan_alpha.system.contracts import SystemTradeReadoutRecord


REQUIRED_TRADE_TABLES = ("trade_order_intent", "trade_order_execution")


@dataclass(frozen=True)
class SystemTradeSourceDataset:
    """Trade rows consumed by system readout."""

    trade_source_path: Path | None
    readout_rows: list[SystemTradeReadoutRecord]
    source_available: bool


def load_system_trade_readout_rows(*, settings: WorkspaceRoots, portfolio_id: str) -> SystemTradeSourceDataset:
    """Load the formal trade rows for system readout."""

    trade_path = settings.databases.trade
    if not trade_path.exists():
        return SystemTradeSourceDataset(trade_source_path=None, readout_rows=[], source_available=False)

    with duckdb.connect(str(trade_path), read_only=True) as connection:
        available_tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if not set(REQUIRED_TRADE_TABLES).issubset(available_tables):
            return SystemTradeSourceDataset(trade_source_path=trade_path, readout_rows=[], source_available=False)

        rows = connection.execute(
            """
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
            FROM trade_order_execution AS execution
            INNER JOIN trade_order_intent AS intent
                ON intent.order_intent_nk = execution.order_intent_nk
            WHERE intent.portfolio_id = ?
            ORDER BY intent.portfolio_id, intent.symbol, execution.execution_trade_date, execution.order_execution_nk
            """,
            [portfolio_id],
        ).fetchall()

    readout_rows = [
        SystemTradeReadoutRecord(
            system_readout_nk=f"system:{order_execution_nk}",
            order_intent_nk=str(order_intent_nk),
            order_execution_nk=str(order_execution_nk),
            portfolio_id=str(row_portfolio_id),
            symbol=str(symbol),
            reference_trade_date=_as_date(reference_trade_date) if reference_trade_date is not None else None,
            planned_trade_date=_as_date(planned_trade_date) if planned_trade_date is not None else None,
            execution_trade_date=_as_date(execution_trade_date) if execution_trade_date is not None else None,
            position_action_decision=str(position_action_decision),
            intent_status=str(intent_status),
            execution_status=str(execution_status),
            requested_weight=float(requested_weight),
            admitted_weight=float(admitted_weight),
            execution_weight=float(execution_weight),
            executed_weight=float(executed_weight),
            execution_price=float(execution_price) if execution_price is not None else None,
            blocking_reason_code=str(blocking_reason_code) if blocking_reason_code is not None else None,
            source_price_line=str(source_price_line),
        )
        for (
            order_intent_nk,
            order_execution_nk,
            row_portfolio_id,
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
        ) in rows
    ]
    return SystemTradeSourceDataset(trade_source_path=trade_path, readout_rows=readout_rows, source_available=True)


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()

