"""Formal position contracts used by stage-four runners and ledgers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class CandidateStatus(str, Enum):
    """Stable stage-four candidate statuses."""

    ADMITTED = "admitted"
    BLOCKED = "blocked"


class CapacityStatus(str, Enum):
    """Stable stage-four capacity statuses."""

    ENABLED = "enabled"
    BLOCKED = "blocked"


class PositionActionDecision(str, Enum):
    """Stable stage-four sizing actions."""

    OPEN = "open"
    BLOCKED = "blocked"


class PositionRunStatus(str, Enum):
    """Stable runner status values."""

    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class PositionCheckpointSummary:
    """Serialized checkpoint summary returned by position runners."""

    work_units_seen: int
    work_units_updated: int
    latest_signal_date: str | None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "work_units_seen": self.work_units_seen,
            "work_units_updated": self.work_units_updated,
            "latest_signal_date": self.latest_signal_date,
        }


@dataclass(frozen=True)
class PositionRunSummary:
    """Stable stage-four position runner summary."""

    runner_name: str
    run_id: str
    status: str
    target_path: str
    source_paths: dict[str, str | None]
    message: str
    materialization_counts: dict[str, int] = field(default_factory=dict)
    checkpoint_summary: PositionCheckpointSummary = field(
        default_factory=lambda: PositionCheckpointSummary(work_units_seen=0, work_units_updated=0, latest_signal_date=None)
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
class PositionInputRow:
    """Joined alpha_signal and market reference row."""

    signal_nk: str
    symbol: str
    signal_date: date
    trigger_type: str
    formal_signal_status: str
    source_trigger_event_nk: str
    wave_id: str
    direction: str
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str
    reference_trade_date: date | None
    reference_price: float | None
