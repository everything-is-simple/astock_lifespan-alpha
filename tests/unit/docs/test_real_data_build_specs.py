from __future__ import annotations

from pathlib import Path


def test_stage_nine_real_data_build_spec_freezes_rehearsal_contract():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "14-real-data-build-rehearsal-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-nine-real-data-build`",
        "`H:\\Lifespan-data`",
        "`H:\\Lifespan-data\\astock_lifespan_alpha`",
        "`module-by-module build`",
        "`pipeline replay`",
        "`Go+DuckDB deferred`",
        "`run_data_to_system_pipeline`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_nine_real_data_build_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}"
    required_terms = [
        "`32` 阶段九真实建库演练规格冻结",
        "`docs/03-execution/32-real-data-build-rehearsal-spec-freeze-conclusion-20260419.md`",
        "`stage-nine-real-data-build`",
        "阶段九规格冻结，真实建库待执行",
        "`module-by-module build`",
        "`pipeline replay`",
    ]

    for term in required_terms:
        assert term in combined
