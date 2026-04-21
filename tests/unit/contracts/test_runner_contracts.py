from __future__ import annotations

from astock_lifespan_alpha.alpha import (
    run_alpha_bof_build,
    run_alpha_bpb_build,
    run_alpha_cpb_build,
    run_alpha_pb_build,
    run_alpha_signal_build,
    run_alpha_tst_build,
)
from astock_lifespan_alpha.malf import run_malf_day_build, run_malf_month_build, run_malf_week_build
from astock_lifespan_alpha.pipeline import run_data_to_system_pipeline
from astock_lifespan_alpha.portfolio_plan import run_portfolio_plan_build
from astock_lifespan_alpha.position import run_position_from_alpha_signal
from astock_lifespan_alpha.system import run_system_from_trade


def test_foundation_runner_names_are_stable(monkeypatch, tmp_path):
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("LIFESPAN_REPO_ROOT", str(workspace / "repo"))
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(workspace / "data"))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(workspace / "report"))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(workspace / "temp"))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(workspace / "validated"))
    (workspace / "repo").mkdir(parents=True, exist_ok=True)

    malf_summaries = [
        run_malf_day_build(),
        run_malf_week_build(),
        run_malf_month_build(),
    ]
    alpha_summaries = [
        run_alpha_bof_build(),
        run_alpha_tst_build(),
        run_alpha_pb_build(),
        run_alpha_cpb_build(),
        run_alpha_bpb_build(),
        run_alpha_signal_build(),
    ]
    stage_four_summaries = [
        run_position_from_alpha_signal(),
        run_portfolio_plan_build(),
    ]
    stage_six_summary = run_system_from_trade()
    stage_eight_summary = run_data_to_system_pipeline()

    assert [summary.runner_name for summary in malf_summaries + alpha_summaries + stage_four_summaries] == [
        "run_malf_day_build",
        "run_malf_week_build",
        "run_malf_month_build",
        "run_alpha_bof_build",
        "run_alpha_tst_build",
        "run_alpha_pb_build",
        "run_alpha_cpb_build",
        "run_alpha_bpb_build",
        "run_alpha_signal_build",
        "run_position_from_alpha_signal",
        "run_portfolio_plan_build",
    ]
    assert [summary.timeframe for summary in malf_summaries] == ["day", "week", "month"]
    assert all(summary.status == "completed" for summary in malf_summaries)
    assert all("phase" not in summary.as_dict() for summary in malf_summaries)
    assert all("segment_summary" in summary.as_dict() for summary in malf_summaries)
    assert all("progress_summary" in summary.as_dict() for summary in malf_summaries)
    assert all("artifact_summary" in summary.as_dict() for summary in malf_summaries)
    assert [summary.scope for summary in alpha_summaries] == [
        "bof",
        "tst",
        "pb",
        "cpb",
        "bpb",
        "alpha_signal",
    ]
    assert all(summary.status == "completed" for summary in alpha_summaries)
    assert all("phase" not in summary.as_dict() for summary in alpha_summaries)
    assert all(summary.status == "completed" for summary in stage_four_summaries)
    assert all("phase" not in summary.as_dict() for summary in stage_four_summaries)
    assert all("checkpoint_summary" in summary.as_dict() for summary in stage_four_summaries)
    assert "checkpoint_summary" in stage_six_summary.as_dict()
    assert "resume_summary" in stage_eight_summary.as_dict()
