"""Formal MALF contracts used by stage-two runners and ledgers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Timeframe(str, Enum):
    """Supported MALF build timeframes."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class LifeState(str, Enum):
    """Formal MALF life-state contract."""

    ALIVE = "alive"
    BROKEN = "broken"
    REBORN = "reborn"


class WaveDirection(str, Enum):
    """Formal MALF wave-direction contract."""

    UP = "up"
    DOWN = "down"


class WavePositionZone(str, Enum):
    """Formal alpha-facing wave position zones."""

    EARLY_PROGRESS = "early_progress"
    MATURE_PROGRESS = "mature_progress"
    MATURE_STAGNATION = "mature_stagnation"
    WEAK_STAGNATION = "weak_stagnation"


class RunStatus(str, Enum):
    """Stable runner status values."""

    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class OhlcBar:
    """Minimal fact-layer OHLC bar contract."""

    symbol: str
    bar_dt: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class CheckpointSummary:
    """Serialized checkpoint summary returned by MALF runners."""

    symbols_seen: int
    symbols_updated: int
    latest_bar_dt: str | None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "symbols_seen": self.symbols_seen,
            "symbols_updated": self.symbols_updated,
            "latest_bar_dt": self.latest_bar_dt,
        }


@dataclass(frozen=True)
class WriteTimingSummary:
    """Write-path timing summary returned by MALF runners and diagnostics."""

    delete_old_rows_seconds: float = 0.0
    insert_ledgers_seconds: float = 0.0
    checkpoint_seconds: float = 0.0
    queue_update_seconds: float = 0.0
    write_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "delete_old_rows_seconds": self.delete_old_rows_seconds,
            "insert_ledgers_seconds": self.insert_ledgers_seconds,
            "checkpoint_seconds": self.checkpoint_seconds,
            "queue_update_seconds": self.queue_update_seconds,
            "write_seconds": self.write_seconds,
        }


@dataclass(frozen=True)
class MalfRunSummary:
    """Stable stage-two MALF runner summary."""

    runner_name: str
    timeframe: str
    run_id: str
    status: str
    target_path: str
    source_path: str | None
    message: str
    materialization_counts: dict[str, int] = field(default_factory=dict)
    checkpoint_summary: CheckpointSummary = field(
        default_factory=lambda: CheckpointSummary(symbols_seen=0, symbols_updated=0, latest_bar_dt=None)
    )
    write_timing_summary: WriteTimingSummary = field(default_factory=WriteTimingSummary)

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "timeframe": self.timeframe,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "source_path": self.source_path,
            "message": self.message,
            "materialization_counts": dict(self.materialization_counts),
            "checkpoint_summary": self.checkpoint_summary.as_dict(),
            "write_timing_summary": self.write_timing_summary.as_dict(),
        }
