from __future__ import annotations

from pathlib import Path


def test_stage_eleven_malf_day_repair_spec_freezes_boundary():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "16-stage-eleven-malf-day-repair-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-eleven-malf-day-repair`",
        "`stock_daily_adjusted`",
        "`adjust_method = backward`",
        "`symbol + trade_date -> 1 day bar`",
        "`snapshot_nk / pivot_nk`",
        "`_rank_snapshots()`",
        "`_build_profiles()`",
        "`profile_malf_day_real_data`",
        "阶段九重演待在新瓶颈上重新发起",
    ]

    for term in required_terms:
        assert term in content


def test_stage_eleven_malf_day_repair_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}"
    required_terms = [
        "`36` 阶段十一 MALF day repair 规格冻结",
        "`docs/03-execution/36-stage-eleven-malf-day-repair-spec-freeze-conclusion-20260419.md`",
        "`stage-eleven-malf-day-repair`",
        "`adjust_method = backward`",
    ]

    for term in required_terms:
        assert term in combined


def test_stage_eleven_malf_day_repair_engineering_closeout_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    conclusion = (
        repo_root
        / "docs"
        / "03-execution"
        / "37-stage-eleven-malf-day-repair-engineering-closeout-conclusion-20260419.md"
    ).read_text(encoding="utf-8")

    combined = f"{catalog}\n{docs_index}\n{readme}\n{conclusion}"
    required_terms = [
        "`37` 阶段十一 MALF day repair 工程收口",
        "`docs/03-execution/37-stage-eleven-malf-day-repair-engineering-closeout-conclusion-20260419.md`",
        "`engine_seconds = 1.419344`",
        "`write_timing`",
        "`adjust_method = backward`",
        "阶段十一完成",
    ]

    for term in required_terms:
        assert term in combined
