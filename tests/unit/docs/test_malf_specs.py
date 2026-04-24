from __future__ import annotations

from pathlib import Path


def test_malf_semantic_spec_defines_frozen_terms():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "01-malf-wave-scale-semantic-spec-v1-placeholder-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "`HH / HL / LH / LL`",
        "`new_count`",
        "`no_new_span`",
        "`life_state`",
        "guard",
        "reborn",
        "`break != confirmation`",
        "三周期独立性",
        "最小验收样例",
    ]

    for term in required_terms:
        assert term in content

    assert "`broken` 是旧波终止态，正式写入 `malf_wave_ledger`。" in content
    assert "`malf_state_snapshot` 只描述当前正在展开的新波生命周期。" in content
    assert "正式 materialize `reborn / alive`，不单独展开 `broken`" in content


def test_malf_diagram_spec_maps_text_rules():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "02-malf-wave-scale-diagram-edition-placeholder-20260419.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "状态：`冻结`",
        "文本条款 -> 图版位置对照",
        "文本规格优先于图版",
        "reborn",
        "guard",
        "wave_position_zone",
    ]

    for term in required_terms:
        assert term in content


def test_stage_nineteen_spec_freezes_broken_to_ledger_boundary():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "24-stage-nineteen-malf-day-engine-semantic-repair-spec-v1-20260423.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "`broken` 仍定义为旧波终止态，正式写入 `malf_wave_ledger`。",
        "`malf_state_snapshot` 继续只描述当前正在展开的新波生命周期。",
        "正式 materialize `reborn / alive`，不单独展开 `broken`。",
        "`zone_coverage` 只解释为 `state_snapshot_sample` 的 sample coverage。",
    ]

    for term in required_terms:
        assert term in content


def test_malf_foundation_canon_freezes_core_concepts():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "26-malf-foundation-canon-v1-20260424.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "MALF 不是交易系统",
        "MALF 不输出买卖动作",
        "唯一输入是 `price bars`",
        "`HH / HL / LL / LH / break`",
        "`new-count × no-new-span × life-state`",
        "`break != confirmation`",
        "`reborn -> alive` 必须由新方向正式 `HH / LL` 确认",
        "`WavePosition = (direction, update-rank, stagnation-rank, life-state)`",
    ]

    for term in required_terms:
        assert term in content
