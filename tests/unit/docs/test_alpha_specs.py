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
