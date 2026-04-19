"""Formal Alpha contracts used by stage-three runners and ledgers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class TriggerType(str, Enum):
    """Supported alpha trigger types."""

    BOF = "bof"
    TST = "tst"
    PB = "pb"
    CPB = "cpb"
    BPB = "bpb"
    SIGNAL = "alpha_signal"


class FormalSignalStatus(str, Enum):
    """Formal signal status contract."""

    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"


class AlphaRunStatus(str, Enum):
    """Stable runner status values."""

    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class AlphaCheckpointSummary:
    """Serialized checkpoint summary returned by alpha runners."""

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
class AlphaRunSummary:
    """Stable stage-three alpha runner summary."""

    runner_name: str
    scope: str
    run_id: str
    status: str
    target_path: str
    source_paths: dict[str, str | None]
    message: str
    materialization_counts: dict[str, int] = field(default_factory=dict)
    checkpoint_summary: AlphaCheckpointSummary = field(
        default_factory=lambda: AlphaCheckpointSummary(work_units_seen=0, work_units_updated=0, latest_signal_date=None)
    )

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "scope": self.scope,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "source_paths": dict(self.source_paths),
            "message": self.message,
            "materialization_counts": dict(self.materialization_counts),
            "checkpoint_summary": self.checkpoint_summary.as_dict(),
        }


@dataclass(frozen=True)
class AlphaInputRow:
    """Joined market-base and MALF snapshot input row."""

    symbol: str
    signal_date: date
    open: float
    high: float
    low: float
    close: float
    wave_id: str
    direction: str
    new_count: int
    no_new_span: int
    life_state: str
    update_rank: float
    stagnation_rank: float
    wave_position_zone: str
