from __future__ import annotations

from pathlib import Path


def test_portfolio_plan_specs_capture_stage_four_and_stage_seventeen_contracts():
    repo_root = Path(__file__).resolve().parents[3]
    stage_four = (
        repo_root / "docs" / "02-spec" / "07-portfolio-plan-minimal-bridge-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")
    stage_seventeen = (
        repo_root / "docs" / "02-spec" / "22-stage-seventeen-rolling-backtest-minimal-v1-spec-20260421.md"
    ).read_text(encoding="utf-8")

    for term in [
        "`run_portfolio_plan_build`",
        "`portfolio_plan_snapshot`",
        "`portfolio_plan_run_snapshot`",
        "`admitted / blocked / trimmed`",
    ]:
        assert term in stage_four

    for term in [
        "`portfolio_gross_cap_weight`",
        "`0.50`",
        "live active-cap accounting",
        "`planned_entry_trade_date`",
        "`scheduled_exit_trade_date`",
        "`current_portfolio_gross_weight`",
        "`remaining_portfolio_capacity_weight`",
    ]:
        assert term in stage_seventeen


def test_portfolio_plan_freeze_gate_and_card50_regate_are_registered_and_marked_pending_fix():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    board = (
        repo_root
        / "docs"
        / "03-execution"
        / "47-mainline-module-freeze-campaign-governance-board-conclusion-20260422.md"
    ).read_text(encoding="utf-8")
    conclusion = (
        repo_root
        / "docs"
        / "03-execution"
        / "50-portfolio-plan-live-050-cutover-performance-repair-and-regate-conclusion-20260422.md"
    ).read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}\n{board}\n{conclusion}"
    required_terms = [
        "`49` portfolio_plan 阶段十七 live freeze gate",
        "`50` portfolio_plan live `0.50` cutover 性能修复与重验收",
        "`docs/03-execution/49-portfolio-plan-stage-seventeen-live-freeze-gate-conclusion-20260422.md`",
        "`docs/03-execution/50-portfolio-plan-live-050-cutover-performance-repair-and-regate-conclusion-20260422.md`",
        "`portfolio_plan = 待修`",
        "`portfolio-plan-0875345c4aa5`",
        "`position`",
        "`放行`",
        "`portfolio_plan`",
    ]

    for term in required_terms:
        assert term in combined
