"""Formal portfolio plan contracts used by stage-four runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PortfolioPlanStatus(str, Enum):
    """Stable portfolio plan status values."""

    COMPLETED = "completed"


@dataclass(frozen=True)
class PortfolioPlanRunSummary:
    """Stable stage-four portfolio plan runner summary."""

    runner_name: str
    run_id: str
    status: str
    target_path: str
    source_paths: dict[str, str | None]
    message: str
    materialization_counts: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "source_paths": dict(self.source_paths),
            "message": self.message,
            "materialization_counts": dict(self.materialization_counts),
        }
