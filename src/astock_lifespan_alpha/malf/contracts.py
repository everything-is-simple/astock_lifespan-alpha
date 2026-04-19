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
class SegmentSummary:
    """Serialized segment-selection summary returned by MALF runners."""

    start_symbol: str | None = None
    end_symbol: str | None = None
    symbol_limit: int | None = None
    resume: bool = True
    full_universe: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "start_symbol": self.start_symbol,
            "end_symbol": self.end_symbol,
            "symbol_limit": self.symbol_limit,
            "resume": self.resume,
            "full_universe": self.full_universe,
        }


@dataclass(frozen=True)
class ProgressSummary:
    """Serialized progress summary returned by MALF runners."""

    symbols_total: int = 0
    symbols_seen: int = 0
    symbols_completed: int = 0
    current_symbol: str | None = None
    elapsed_seconds: float = 0.0
    estimated_remaining_symbols: int = 0
    ledger_rows_written: dict[str, int] = field(default_factory=dict)
    progress_path: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "symbols_total": self.symbols_total,
            "symbols_seen": self.symbols_seen,
            "symbols_completed": self.symbols_completed,
            "current_symbol": self.current_symbol,
            "elapsed_seconds": self.elapsed_seconds,
            "estimated_remaining_symbols": self.estimated_remaining_symbols,
            "ledger_rows_written": dict(self.ledger_rows_written),
            "progress_path": self.progress_path,
        }


@dataclass(frozen=True)
class ArtifactSummary:
    """Serialized artifact summary returned by MALF runners."""

    active_build_path: str | None = None
    abandoned_build_artifacts: tuple[str, ...] = ()
    promoted_to_target: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "active_build_path": self.active_build_path,
            "abandoned_build_artifacts": list(self.abandoned_build_artifacts),
            "promoted_to_target": self.promoted_to_target,
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
    segment_summary: SegmentSummary = field(default_factory=SegmentSummary)
    progress_summary: ProgressSummary = field(default_factory=ProgressSummary)
    artifact_summary: ArtifactSummary = field(default_factory=ArtifactSummary)

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
            "segment_summary": self.segment_summary.as_dict(),
            "progress_summary": self.progress_summary.as_dict(),
            "artifact_summary": self.artifact_summary.as_dict(),
        }
