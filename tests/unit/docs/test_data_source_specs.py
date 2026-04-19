from __future__ import annotations

from pathlib import Path


def test_stage_seven_data_source_contract_freezes_real_local_stock_sources():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "12-data-source-fact-contract-alignment-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-seven-data-source-contract`",
        "`market_base.duckdb`",
        "`market_base_week.duckdb`",
        "`market_base_month.duckdb`",
        "`raw_market.duckdb`",
        "`raw_market_week.duckdb`",
        "`raw_market_month.duckdb`",
        "`stock_daily_adjusted`",
        "`stock_weekly_adjusted`",
        "`stock_monthly_adjusted`",
        "`code -> symbol`",
        "`trade_date -> bar_dt`",
        "只读 stock",
        "阶段八",
        "`data -> system`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_seven_data_source_contract_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}"
    required_terms = [
        "`28` 阶段七 data 源事实契约规格冻结",
        "`docs/03-execution/28-data-source-fact-contract-alignment-spec-freeze-conclusion-20260419.md`",
        "`stage-seven-data-source-contract`",
        "阶段七规格冻结，工程待实施",
        "阶段八 `data -> system`",
    ]

    for term in required_terms:
        assert term in combined

