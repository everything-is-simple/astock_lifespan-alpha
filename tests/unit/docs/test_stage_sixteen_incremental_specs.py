from __future__ import annotations

from pathlib import Path


def test_stage_sixteen_incremental_resume_spec_freezes_new_contracts():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root
        / "docs"
        / "02-spec"
        / "21-stage-sixteen-portfolio-plan-system-pipeline-incremental-resume-spec-v1-20260421.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-sixteen-incremental-resume`",
        "`portfolio_plan_work_queue`",
        "`portfolio_plan_checkpoint`",
        "`PortfolioPlanCheckpointSummary`",
        "`system_work_queue`",
        "`system_checkpoint`",
        "`SystemCheckpointSummary`",
        "`pipeline_step_checkpoint`",
        "`PipelineResumeSummary`",
        "`resume_start_step`",
        "`pipeline_action='reused_checkpoint'`",
        "`run_portfolio_plan_build`",
        "`run_system_from_trade`",
        "`run_data_to_system_pipeline`",
    ]

    for term in required_terms:
        assert term in content


def test_stage_sixteen_execution_closeout_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}"
    required_terms = [
        "`45` 阶段十六正式增量与自动续跑规格冻结",
        "`46` 阶段十六正式增量与自动续跑工程收口",
        "`docs/03-execution/45-stage-sixteen-incremental-resume-spec-freeze-conclusion-20260421.md`",
        "`docs/03-execution/46-stage-sixteen-incremental-resume-engineering-closeout-conclusion-20260421.md`",
        "`stage-sixteen-incremental-resume`",
    ]

    for term in required_terms:
        assert term in combined
