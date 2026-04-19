"""Foundation-stage MALF runner stubs."""

from __future__ import annotations

from astock_lifespan_alpha.core.contracts import RunnerStubSummary, build_stub_summary
from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings


def run_malf_day_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    return build_stub_summary(
        runner_name="run_malf_day_build",
        module_name="malf",
        target_path=str(workspace.databases.malf_day),
    )


def run_malf_week_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    return build_stub_summary(
        runner_name="run_malf_week_build",
        module_name="malf",
        target_path=str(workspace.databases.malf_week),
    )


def run_malf_month_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    return build_stub_summary(
        runner_name="run_malf_month_build",
        module_name="malf",
        target_path=str(workspace.databases.malf_month),
    )

