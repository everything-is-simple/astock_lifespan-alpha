from __future__ import annotations

from pathlib import Path


def test_alpha_signal_to_position_bridge_spec_freezes_stage_four_contract():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "05-alpha-signal-to-position-bridge-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`alpha_signal -> position`",
        "`signal_nk`",
        "`wave_position_zone`",
        "`market_base_day`",
        "`position` 的唯一正式上游是 `alpha_signal`",
    ]

    for term in required_terms:
        assert term in content


def test_position_and_portfolio_plan_specs_define_minimal_stage_four_ledgers():
    repo_root = Path(__file__).resolve().parents[3]
    position_content = (
        repo_root / "docs" / "02-spec" / "06-position-minimal-ledger-and-runner-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")
    portfolio_content = (
        repo_root / "docs" / "02-spec" / "07-portfolio-plan-minimal-bridge-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    for term in [
        "`position_run`",
        "`position_work_queue`",
        "`position_checkpoint`",
        "`position_candidate_audit`",
        "`position_capacity_snapshot`",
        "`position_sizing_snapshot`",
        "`run_position_from_alpha_signal`",
    ]:
        assert term in position_content

    for term in [
        "`portfolio_plan_run`",
        "`portfolio_plan_snapshot`",
        "`portfolio_plan_run_snapshot`",
        "`admitted / blocked / trimmed`",
        "`run_portfolio_plan_build`",
    ]:
        assert term in portfolio_content
