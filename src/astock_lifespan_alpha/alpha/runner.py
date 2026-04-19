"""Foundation-stage alpha runner stubs."""

from __future__ import annotations

from astock_lifespan_alpha.core.contracts import RunnerStubSummary, build_stub_summary
from astock_lifespan_alpha.core.paths import WorkspaceRoots, default_settings


def _alpha_stub(runner_name: str, target_path: str, settings: WorkspaceRoots | None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    workspace.ensure_directories()
    return build_stub_summary(runner_name=runner_name, module_name="alpha", target_path=target_path)


def run_alpha_bof_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_bof_build", str(workspace.databases.alpha_bof), workspace)


def run_alpha_tst_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_tst_build", str(workspace.databases.alpha_tst), workspace)


def run_alpha_pb_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_pb_build", str(workspace.databases.alpha_pb), workspace)


def run_alpha_cpb_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_cpb_build", str(workspace.databases.alpha_cpb), workspace)


def run_alpha_bpb_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_bpb_build", str(workspace.databases.alpha_bpb), workspace)


def run_alpha_signal_build(*, settings: WorkspaceRoots | None = None) -> RunnerStubSummary:
    workspace = settings or default_settings()
    return _alpha_stub("run_alpha_signal_build", str(workspace.databases.alpha_signal), workspace)

