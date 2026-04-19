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
from astock_lifespan_alpha.position import run_position_from_alpha_signal


def test_foundation_runner_names_are_stable():
    summaries = [
        run_malf_day_build(),
        run_malf_week_build(),
        run_malf_month_build(),
        run_alpha_bof_build(),
        run_alpha_tst_build(),
        run_alpha_pb_build(),
        run_alpha_cpb_build(),
        run_alpha_bpb_build(),
        run_alpha_signal_build(),
        run_position_from_alpha_signal(),
    ]

    assert [summary.runner_name for summary in summaries] == [
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
    ]
    assert all(summary.status == "stub" for summary in summaries)
    assert all(summary.phase == "foundation_bootstrap" for summary in summaries)

