"""Stage-five trade materialization engine."""

from __future__ import annotations

import hashlib
import json

from astock_lifespan_alpha.trade.contracts import (
    EXECUTION_PRICE_LINE,
    TRADE_CONTRACT_VERSION,
    ExecutionPriceRow,
    PortfolioPlanTradeInputRow,
    TradeExecutionRecord,
    TradeExecutionStatus,
    TradeIntentRecord,
    TradeIntentStatus,
    TradeMaterializationBundle,
)
from astock_lifespan_alpha.trade.source import pick_next_execution_price


def materialize_trade_work_unit(
    *,
    rows: list[PortfolioPlanTradeInputRow],
    execution_prices: list[ExecutionPriceRow],
) -> TradeMaterializationBundle:
    """Convert portfolio_plan rows into trade intent and execution rows."""

    intents: list[TradeIntentRecord] = []
    executions: list[TradeExecutionRecord] = []
    fingerprint_payload: list[dict[str, object]] = []

    for row in rows:
        execution_price = pick_next_execution_price(
            rows=execution_prices,
            reference_trade_date=row.reference_trade_date,
        )
        intent, execution = _materialize_row(row=row, execution_price=execution_price)
        intents.append(intent)
        executions.append(execution)
        fingerprint_payload.append(
            {
                "plan": row.__dict__,
                "execution_price": execution_price.__dict__ if execution_price is not None else None,
                "intent": intent.signature(),
                "execution": execution.signature(),
            }
        )

    return TradeMaterializationBundle(
        intents=intents,
        executions=executions,
        source_fingerprint=_stable_fingerprint(fingerprint_payload),
    )


def _materialize_row(
    *,
    row: PortfolioPlanTradeInputRow,
    execution_price: ExecutionPriceRow | None,
) -> tuple[TradeIntentRecord, TradeExecutionRecord]:
    planned_trade_date = execution_price.trade_date if execution_price is not None else None
    planned_trade_date_token = planned_trade_date.isoformat() if planned_trade_date is not None else "no_execution_date"
    order_intent_nk = f"{row.portfolio_id}:{row.candidate_nk}:{planned_trade_date_token}:{TRADE_CONTRACT_VERSION}"

    intent_status = TradeIntentStatus.BLOCKED.value
    execution_status = TradeExecutionStatus.REJECTED.value
    execution_weight = 0.0
    executed_weight = 0.0
    blocking_reason_code = _blocking_reason(row=row, execution_price=execution_price)

    if blocking_reason_code is None:
        intent_status = TradeIntentStatus.PLANNED.value
        execution_status = TradeExecutionStatus.FILLED.value
        execution_weight = round(row.admitted_weight, 8)
        executed_weight = round(row.admitted_weight, 8)

    intent = TradeIntentRecord(
        order_intent_nk=order_intent_nk,
        plan_snapshot_nk=row.plan_snapshot_nk,
        candidate_nk=row.candidate_nk,
        portfolio_id=row.portfolio_id,
        symbol=row.symbol,
        reference_trade_date=row.reference_trade_date,
        planned_trade_date=planned_trade_date,
        position_action_decision=row.position_action_decision,
        intent_status=intent_status,
        requested_weight=row.requested_weight,
        admitted_weight=row.admitted_weight,
        execution_weight=execution_weight,
        blocking_reason_code=blocking_reason_code,
    )
    execution_trade_date = planned_trade_date
    execution_price_value = execution_price.open_price if execution_price is not None else None
    order_execution_nk = (
        f"{order_intent_nk}:"
        f"{execution_trade_date.isoformat() if execution_trade_date is not None else 'no_execution_date'}:"
        f"{execution_status}"
    )
    execution = TradeExecutionRecord(
        order_execution_nk=order_execution_nk,
        order_intent_nk=order_intent_nk,
        portfolio_id=row.portfolio_id,
        symbol=row.symbol,
        execution_status=execution_status,
        execution_trade_date=execution_trade_date,
        execution_price=execution_price_value,
        executed_weight=executed_weight,
        blocking_reason_code=blocking_reason_code,
        source_price_line=EXECUTION_PRICE_LINE,
    )
    return intent, execution


def _blocking_reason(
    *,
    row: PortfolioPlanTradeInputRow,
    execution_price: ExecutionPriceRow | None,
) -> str | None:
    if row.plan_status == "blocked":
        return row.blocking_reason_code or "plan_blocked"
    if row.plan_status not in {"admitted", "trimmed"}:
        return row.blocking_reason_code or "unsupported_plan_status"
    if row.position_action_decision != "open":
        return "unsupported_position_action"
    if row.admitted_weight <= 0:
        return "invalid_admitted_weight"
    if row.reference_trade_date is None:
        return "missing_reference_trade_date"
    if execution_price is None:
        return "missing_next_execution_trade_date"
    if execution_price.open_price is None:
        return "missing_execution_open_price"
    return None


def _stable_fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
