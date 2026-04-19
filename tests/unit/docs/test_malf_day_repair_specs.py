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


def test_stage_twelve_malf_day_write_path_replay_unblock_spec_freezes_boundary():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "17-stage-twelve-malf-day-write-path-replay-unblock-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-twelve-malf-day-write-path-replay-unblock`",
        "`write_timing`",
        "`delete_old_rows_seconds`",
        "`insert_ledgers_seconds`",
        "`checkpoint_seconds`",
        "`queue_update_seconds`",
        "`run_malf_day_build`",
        "不修改 MALF 语义状态机",
        "阶段九真实重演",
    ]

    for term in required_terms:
        assert term in content


def test_stage_twelve_malf_day_write_path_replay_unblock_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}"
    required_terms = [
        "`38` 阶段十二 MALF day 写路径重演 unblock 规格冻结",
        "`docs/03-execution/38-stage-twelve-malf-day-write-path-replay-unblock-spec-freeze-conclusion-20260419.md`",
        "`stage-twelve-malf-day-write-path-replay-unblock`",
        "`write_timing`",
        "阶段九真实重演 unblock",
    ]

    for term in required_terms:
        assert term in combined


def test_stage_twelve_malf_day_write_path_engineering_closeout_is_registered():
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
        / "39-stage-twelve-malf-day-write-path-replay-unblock-engineering-closeout-conclusion-20260419.md"
    ).read_text(encoding="utf-8")

    combined = f"{catalog}\n{docs_index}\n{readme}\n{conclusion}"
    required_terms = [
        "`39` 阶段十二 MALF day 写路径重演 unblock 工程收口",
        "`docs/03-execution/39-stage-twelve-malf-day-write-path-replay-unblock-engineering-closeout-conclusion-20260419.md`",
        "`write_timing_summary`",
        "`write_seconds = 0.911749`",
        "阶段九重演尚未登记为完成",
    ]

    for term in required_terms:
        assert term in combined


def test_stage_thirteen_malf_day_segmented_build_completion_spec_freezes_boundary():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "18-stage-thirteen-malf-day-segmented-build-completion-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`stage-thirteen-malf-day-segmented-build-completion`",
        "`segmented build`",
        "`resume`",
        "`progress`",
        "`abandoned build artifacts`",
        "`100 / 500 / 1000 symbol`",
        "阶段九 replay 待阶段十三完成后重新发起",
    ]

    for term in required_terms:
        assert term in content


def test_stage_thirteen_malf_day_segmented_build_completion_spec_freeze_is_registered():
    repo_root = Path(__file__).resolve().parents[3]
    catalog = (repo_root / "docs" / "03-execution" / "00-conclusion-catalog-20260419.md").read_text(
        encoding="utf-8"
    )
    execution_index = (repo_root / "docs" / "03-execution" / "README.md").read_text(encoding="utf-8")
    docs_index = (repo_root / "docs" / "README.md").read_text(encoding="utf-8")
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    combined = f"{catalog}\n{execution_index}\n{docs_index}\n{readme}"
    required_terms = [
        "`40` 阶段十三 MALF day segmented build completion 规格冻结",
        "`docs/03-execution/40-stage-thirteen-malf-day-segmented-build-completion-spec-freeze-conclusion-20260419.md`",
        "`stage-thirteen-malf-day-segmented-build-completion`",
        "`segmented build`",
    ]

    for term in required_terms:
        assert term in combined


def test_stage_thirteen_malf_day_segmented_build_completion_engineering_closeout_is_registered():
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
        / "41-stage-thirteen-malf-day-segmented-build-completion-engineering-closeout-conclusion-20260419.md"
    ).read_text(encoding="utf-8")

    combined = f"{catalog}\n{docs_index}\n{readme}\n{conclusion}"
    required_terms = [
        "`41` 阶段十三 MALF day segmented build completion 工程收口",
        "`docs/03-execution/41-stage-thirteen-malf-day-segmented-build-completion-engineering-closeout-conclusion-20260419.md`",
        "`progress_summary`",
        "`artifact_summary`",
        "`100 / 500 / 1000 symbol`",
        "阶段九 replay 待阶段十三完成后重新发起",
    ]

    for term in required_terms:
        assert term in combined
