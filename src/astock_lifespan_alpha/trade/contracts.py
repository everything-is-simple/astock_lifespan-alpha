"""Formal trade contracts used by stage-five runners and ledgers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


TRADE_CONTRACT_VERSION = "stage5_trade_v1"
EXECUTION_PRICE_LINE = "execution_price_line"


class TradeRunStatus(str, Enum):
    """Stable trade runner status values."""

    COMPLETED = "completed"


class TradeIntentStatus(str, Enum):
    """Stable stage-five order intent statuses."""

    PLANNED = "planned"
    BLOCKED = "blocked"


class TradeExecutionStatus(str, Enum):
    """Stable stage-five execution statuses.

    ACCEPTED is reserved for a later real execution lifecycle. The first
    stage-five runner only materializes FILLED and REJECTED.
    """

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    FILLED = "filled"


class TradeMaterializationAction(str, Enum):
    """Stable stage-five materialization actions."""

    INSERTED = "inserted"
    REUSED = "reused"
    REMATERIALIZED = "rematerialized"


@dataclass(frozen=True)
class TradeCheckpointSummary:
    """Serialized checkpoint summary returned by trade runners."""

    work_units_seen: int
    work_units_updated: int
    latest_reference_trade_date: str | None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "work_units_seen": self.work_units_seen,
            "work_units_updated": self.work_units_updated,
            "latest_reference_trade_date": self.latest_reference_trade_date,
        }


@dataclass(frozen=True)
class TradeRunSummary:
    """Stable stage-five trade runner summary."""

    runner_name: str
    run_id: str
    status: str
    target_path: str
    source_paths: dict[str, str | None]
    message: str
    materialization_counts: dict[str, int] = field(default_factory=dict)
    checkpoint_summary: TradeCheckpointSummary = field(
        default_factory=lambda: TradeCheckpointSummary(
            work_units_seen=0,
            work_units_updated=0,
            latest_reference_trade_date=None,
        )
    )

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "source_paths": dict(self.source_paths),
            "message": self.message,
            "materialization_counts": dict(self.materialization_counts),
            "checkpoint_summary": self.checkpoint_summary.as_dict(),
        }


@dataclass(frozen=True)
class PortfolioPlanTradeInputRow:
    """Minimal portfolio_plan row consumed by trade."""

    plan_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    symbol: str
    reference_trade_date: date | None
    position_action_decision: str
    requested_weight: float
    admitted_weight: float
    trimmed_weight: float
    plan_status: str
    blocking_reason_code: str | None


@dataclass(frozen=True)
class ExecutionPriceRow:
    """Execution-price-line daily open price row."""

    symbol: str
    trade_date: date
    open_price: float | None


@dataclass(frozen=True)
class TradeIntentRecord:
    """Materialized order intent row."""

    order_intent_nk: str
    plan_snapshot_nk: str
    candidate_nk: str
    portfolio_id: str
    symbol: str
    reference_trade_date: date | None
    planned_trade_date: date | None
    position_action_decision: str
    intent_status: str
    requested_weight: float
    admitted_weight: float
    execution_weight: float
    blocking_reason_code: str | None

    def signature(self) -> tuple[object, ...]:
        return (
            self.plan_snapshot_nk,
            self.candidate_nk,
            self.portfolio_id,
            self.symbol,
            self.reference_trade_date,
            self.planned_trade_date,
            self.position_action_decision,
            self.intent_status,
            self.requested_weight,
            self.admitted_weight,
            self.execution_weight,
            self.blocking_reason_code,
        )


@dataclass(frozen=True)
class TradeExecutionRecord:
    """Materialized minimal execution report row."""

    order_execution_nk: str
    order_intent_nk: str
    portfolio_id: str
    symbol: str
    execution_status: str
    execution_trade_date: date | None
    execution_price: float | None
    executed_weight: float
    blocking_reason_code: str | None
    source_price_line: str

    def signature(self) -> tuple[object, ...]:
        return (
            self.order_intent_nk,
            self.portfolio_id,
            self.symbol,
            self.execution_status,
            self.execution_trade_date,
            self.execution_price,
            self.executed_weight,
            self.blocking_reason_code,
            self.source_price_line,
        )


@dataclass(frozen=True)
class TradeMaterializationBundle:
    """Intent and execution rows generated for a bounded work unit."""

    intents: list[TradeIntentRecord]
    executions: list[TradeExecutionRecord]
    source_fingerprint: str
