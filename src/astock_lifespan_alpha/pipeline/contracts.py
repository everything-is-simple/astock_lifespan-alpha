"""Formal pipeline contracts used by stage-eight orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


PIPELINE_CONTRACT_VERSION = "stage8_pipeline_v1"


class PipelineRunStatus(str, Enum):
    """Stable pipeline runner status values."""

    COMPLETED = "completed"


@dataclass(frozen=True)
class PipelineStepSummary:
    """Stable summary of one orchestrated runner step."""

    step_order: int
    runner_name: str
    run_id: str
    status: str
    target_path: str | None
    message: str | None
    summary: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "step_order": self.step_order,
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "message": self.message,
            "summary": dict(self.summary),
        }


@dataclass(frozen=True)
class PipelineRunSummary:
    """Stable stage-eight pipeline runner summary."""

    runner_name: str
    run_id: str
    status: str
    target_path: str
    portfolio_id: str
    step_count: int
    message: str
    steps: list[PipelineStepSummary] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_name": self.runner_name,
            "run_id": self.run_id,
            "status": self.status,
            "target_path": self.target_path,
            "portfolio_id": self.portfolio_id,
            "step_count": self.step_count,
            "message": self.message,
            "steps": [step.as_dict() for step in self.steps],
        }

