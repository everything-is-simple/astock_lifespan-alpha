from __future__ import annotations

from pathlib import Path


def test_stage_six_system_spec_freezes_trade_readout_contract():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "11-system-minimal-readout-and-runner-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-six-system`",
        "`trade -> system`",
        "`run_system_from_trade`",
        "`system_trade_readout`",
        "`system_portfolio_trade_summary`",
        "只读取 `trade` 正式输出",
        "不回读 `alpha / position / portfolio_plan`",
        "不触发上游 runner",
        "`settings.databases.trade`",
        "`trade_order_intent`",
        "`trade_order_execution`",
        "`stage6_system_v1`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_six_system_spec_freeze_conclusion_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}"
    required_terms = [
        "`26` 阶段六 system 读出规格冻结",
        "`docs/03-execution/26-stage-six-system-readout-spec-freeze-conclusion-20260419.md`",
        "`stage-six-system`",
        "阶段六规格冻结，工程待实施",
        "`trade -> system`",
    ]

    for term in required_terms:
        assert term in combined
