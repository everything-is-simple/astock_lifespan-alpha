from __future__ import annotations

from pathlib import Path


def test_stage_ten_malf_day_diagnosis_spec_freezes_boundary():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "15-malf-day-real-data-diagnosis-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-ten-malf-day-diagnosis`",
        "`run_malf_day_build`",
        "`stock_daily_adjusted`",
        "`PYTHONPATH`",
        "`source load timing`",
        "`engine timing`",
        "`write timing`",
        "不修改 MALF 业务语义",
        "阶段九重演待重新发起",
    ]

    for term in required_terms:
        assert term in content


def test_stage_ten_malf_day_diagnosis_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}"
    required_terms = [
        "`34` 阶段十 MALF day 真实库诊断规格冻结",
        "`docs/03-execution/34-malf-day-real-data-diagnosis-spec-freeze-conclusion-20260419.md`",
        "`stage-ten-malf-day-diagnosis`",
        "阶段十规格冻结，诊断待实施",
        "`run_malf_day_build`",
        "`stock_daily_adjusted`",
    ]

    for term in required_terms:
        assert term in combined
