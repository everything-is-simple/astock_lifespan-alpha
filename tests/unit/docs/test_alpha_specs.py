from __future__ import annotations

from pathlib import Path


def test_alpha_pas_trigger_spec_defines_frozen_terms():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "03-alpha-pas-trigger-semantic-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`BOF / TST / PB / CPB / BPB`",
        "`market_base_day`",
        "`malf_day.malf_wave_scale_snapshot`",
        "`candidate / confirmed`",
        "最小验收样例",
    ]

    for term in required_terms:
        assert term in content


def test_alpha_signal_spec_freezes_fields_and_aggregation():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "04-alpha-signal-aggregation-spec-v1-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`signal_nk`",
        "`formal_signal_status`",
        "`source_trigger_db`",
        "不做跨 trigger 去重",
        "`alpha_signal` 是阶段三唯一正式输出账本",
    ]

    for term in required_terms:
        assert term in content


def test_alpha_pas_upgrade_boundary_freezes_current_role_and_legacy_exclusions():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`trigger ledger producer`",
        "`PAS scoring engine`",
        "`market_base_day`",
        "`malf_day.malf_wave_scale_snapshot`",
        "`alpha_trigger_event`",
        "`alpha_trigger_profile`",
        "`alpha_signal`",
        "alpha(PAS) 只消费 MALF 正式字段",
        "`opportunity_score`",
        "`quality_flag`",
        "`risk_reward_ratio`",
        "`16-cell` 当前系统不存在",
        "`16-cell` 不作为下一轮治理候选",
        "下一轮最小实现方向是治理升级",
    ]

    for term in required_terms:
        assert term in content


def test_alpha_pas_reading_path_and_runner_script_names_are_aligned():
    repo_root = Path(__file__).resolve().parents[3]
    readme = (repo_root / "docs" / "02-spec" / "README.md").read_text(encoding="utf-8")
    content = (
        repo_root / "docs" / "02-spec" / "28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md"
    ).read_text(encoding="utf-8")

    required_readme_terms = [
        "## Alpha(PAS) 阅读路径",
        "`docs/02-spec/03-alpha-pas-trigger-semantic-spec-v1-20260419.md`",
        "`docs/02-spec/04-alpha-signal-aggregation-spec-v1-20260419.md`",
        "`docs/02-spec/28-alpha-pas-upgrade-boundary-and-legacy-delta-selection-v1-20260424.md`",
        "`docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`",
    ]
    for term in required_readme_terms:
        assert term in readme

    expected_runner_names = [
        "run_alpha_bof_build",
        "run_alpha_tst_build",
        "run_alpha_pb_build",
        "run_alpha_cpb_build",
        "run_alpha_bpb_build",
        "run_alpha_signal_build",
    ]
    expected_script_names = [f"{runner_name}.py" for runner_name in expected_runner_names]

    script_names = {
        path.name
        for path in (repo_root / "scripts" / "alpha").glob("*.py")
    }
    assert set(expected_script_names).issubset(script_names)

    for runner_name in expected_runner_names:
        assert runner_name in content
        assert f"{runner_name}.py" in content
