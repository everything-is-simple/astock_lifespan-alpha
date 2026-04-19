"""Formal system contracts used by stage-six readout runners."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum


SYSTEM_CONTRACT_VERSION = "stage6_system_v1"


class SystemRunStatus(str, Enum):
    """Stable system runner status values."""

    COMPLETED = "completed"


@dataclass(frozen=True)
class SystemRunSummary:
    """Stable stage-six system runner summary."""

    runner_name: str
    run_id: str
    status: str
    target_path: str
    source_paths: dict[str, str | None]
    message: str
    readout_rows: int
    summary_rows: int

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "source_paths": dict(self.source_paths),
            "message": self.message,
            "readout_rows": self.readout_rows,
            "summary_rows": self.summary_rows,
        }


@dataclass(frozen=True)
class SystemTradeReadoutRecord:
    """Minimal system-facing projection of trade intent and execution rows."""

    system_readout_nk: str
    order_intent_nk: str
    order_execution_nk: str
    portfolio_id: str
    symbol: str
    reference_trade_date: date | None
    planned_trade_date: date | None
    execution_trade_date: date | None
    position_action_decision: str
    intent_status: str
    execution_status: str
    requested_weight: float
    admitted_weight: float
    execution_weight: float
    executed_weight: float
    execution_price: float | None
    blocking_reason_code: str | None
    source_price_line: str

