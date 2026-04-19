from __future__ import annotations

from pathlib import Path


def test_trade_spec_freezes_stage_five_minimal_execution_ledgers():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`trade_run`",
        "`trade_work_queue`",
        "`trade_checkpoint`",
        "`trade_order_intent`",
        "`trade_order_execution`",
        "`trade_run_order_intent`",
        "`run_trade_from_portfolio_plan`",
        "`accepted`",
        "`rejected`",
        "`filled`",
        "`analysis_price_line`",
        "`execution_price_line`",
        "阶段五 `trade` 只做最小执行账本",
    ]

    for term in required_terms:
        assert term in content


def test_portfolio_plan_to_trade_bridge_spec_freezes_price_line_and_errata():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`portfolio_plan_snapshot`",
        "`portfolio_plan -> trade`",
        "`position_action_decision`",
        "`requested_weight`",
        "`admitted_weight`",
        "`trimmed_weight`",
        "`plan_status`",
        "`blocking_reason_code`",
        "`trade` 的唯一正式上游固定为：",
        "`analysis_price_line`",
        "`execution_price_line`",
        "`reference_trade_date`",
        "`reference_price`",
        "只是最小桥接参考，不等于正式执行价格口径",
        "阶段五只做最小桥接，不扩展到 `carry / exit / system`",
    ]

    for term in required_terms:
        assert term in content
