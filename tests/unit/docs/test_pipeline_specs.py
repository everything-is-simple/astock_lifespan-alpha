from __future__ import annotations

from pathlib import Path


def test_stage_eight_pipeline_spec_freezes_orchestration_contract():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-eight-pipeline`",
        "`data -> system`",
        "`run_data_to_system_pipeline`",
        "`pipeline_run`",
        "`pipeline_step_run`",
        "`PipelineRunSummary`",
        "`PipelineStepSummary`",
        "固定 runner 顺序",
        "pipeline 不直接写业务表",
        "`run_malf_day_build`",
        "`run_alpha_signal_build`",
        "`run_system_from_trade`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_eight_pipeline_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}"
    required_terms = [
        "`30` 阶段八 data -> system pipeline 编排规格冻结",
        "`docs/03-execution/30-data-to-system-pipeline-orchestration-spec-freeze-conclusion-20260419.md`",
        "`stage-eight-pipeline`",
        "阶段八规格冻结，工程待实施",
        "`data -> system`",
    ]

    for term in required_terms:
        assert term in combined


def test_stage_eight_pipeline_engineering_closeout_is_registered():
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
        / "31-data-to-system-pipeline-orchestration-engineering-closeout-conclusion-20260419.md"
    ).read_text(encoding="utf-8")

    combined = f"{catalog}\n{docs_index}\n{readme}\n{conclusion}"
    required_terms = [
        "`31` 阶段八 data -> system pipeline 编排工程收口",
        "`docs/03-execution/31-data-to-system-pipeline-orchestration-engineering-closeout-conclusion-20260419.md`",
        "`run_data_to_system_pipeline`",
        "`pipeline_run / pipeline_step_run`",
        "阶段八完成",
        "下一阶段待规划",
        "只调用 public runner",
        "不直接写业务表",
    ]

    for term in required_terms:
        assert term in combined
