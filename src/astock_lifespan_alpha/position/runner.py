"""Foundation-stage position runner stubs."""

from __future__ import annotations

from astock_lifespan_alpha.core.contracts import RunnerStubSummary, build_stub_summary
from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings


def run_position_from_alpha_signal(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    return build_stub_summary(
        runner_name="run_position_from_alpha_signal",
        module_name="position",
        target_path=str(workspace.databases.position),
    )

