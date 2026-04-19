from __future__ import annotations

from pathlib import Path


def test_reconstruction_part2_freezes_stage_five_closeout():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root
        / "docs"
        / "02-spec"
        / "10-astock-lifespan-alpha-reconstruction-plan-part2-stage-five-trade-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`reconstruction-plan-part2`",
        "第五阶段文档先行与工程实施计划",
        "`portfolio_plan -> trade`",
        "`PathConfig.source_databases.market_base`",
        "`execution_price_line`",
        "次日开盘执行",
        "`filled / rejected`",
        "`accepted`",
        "`portfolio_id + symbol`",
        "阶段五完成",
        "阶段六 `system`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_five_engineering_closeout_conclusion_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")

    required_terms = [
        "`25` 阶段五 trade 工程收口",
        "`docs/03-execution/25-stage-five-engineering-closeout-conclusion-20260419.md`",
        "`reconstruction-plan-part2`",
        "阶段五完成",
        "阶段六 system",
    ]

    combined = f"{catalog}\n{docs_index}"
    for term in required_terms:
        assert term in combined
