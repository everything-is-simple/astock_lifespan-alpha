"""Shared contracts used by foundation runner stubs."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class RunnerStubSummary:
    """Minimal non-materializing summary returned by foundation stubs."""

    runner_name: str
    module_name: str
    status: str
    phase: str
    target_path: str | None
    message: str

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


def build_stub_summary(*, runner_name: str, module_name: str, target_path: str | None) -> RunnerStubSummary:
    """Build a stable foundation-stage placeholder response."""

    return RunnerStubSummary(
        runner_name=runner_name,
        module_name=module_name,
        status="stub",
        phase="foundation_bootstrap",
        target_path=target_path,
        message="Foundation bootstrap stub. Business logic is intentionally not implemented yet.",
    )

