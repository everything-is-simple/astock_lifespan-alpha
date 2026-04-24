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


def test_spec_index_declares_malf_foundation_reading_path():
    repo_root = Path(__file__).resolve().parents[3]
    content = (repo_root / "docs" / "02-spec" / "README.md").read_text(encoding="utf-8")

    required_terms = [
        "当前系统地基：`MALF`",
        "`data -> malf -> alpha -> position -> portfolio_plan -> trade -> system`",
        "`docs/02-spec/26-malf-foundation-canon-v1-20260424.md`",
        "`docs/02-spec/27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md`",
        "`docs/02-spec/29-malf-structural-lifespan-proof-harness-v1-20260424.md`",
        "`26 MALF Canon -> 27 MALF+Alpha lineage lessons -> 03/04 Alpha specs`",
        "`26 MALF Canon -> 29 MALF structural lifespan proof harness -> 62/65 conclusions`",
        "阶段规格不得反向改写 MALF 地基语义",
        "`malf = 放行`",
    ]

    for term in required_terms:
        assert term in content


def test_downstream_specs_preserve_malf_as_consumed_foundation():
    repo_root = Path(__file__).resolve().parents[3]
    spec_paths = [
        "03-alpha-pas-trigger-semantic-spec-v1-20260419.md",
        "04-alpha-signal-aggregation-spec-v1-20260419.md",
        "05-alpha-signal-to-position-bridge-spec-v1-20260419.md",
        "06-position-minimal-ledger-and-runner-spec-v1-20260419.md",
        "07-portfolio-plan-minimal-bridge-spec-v1-20260419.md",
        "08-trade-minimal-execution-ledger-and-runner-spec-v1-20260419.md",
        "09-portfolio-plan-to-trade-bridge-spec-v1-20260419.md",
        "11-system-minimal-readout-and-runner-spec-v1-20260419.md",
        "13-data-to-system-minimal-pipeline-orchestration-spec-v1-20260419.md",
    ]

    for relative_path in spec_paths:
        content = (repo_root / "docs" / "02-spec" / relative_path).read_text(encoding="utf-8")
        assert "只消费 MALF 事实，不反向定义 MALF" in content


def test_lineage_lessons_freeze_malf_alpha_pas_core():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "27-lineage-lessons-malf-alpha-pas-core-v1-20260424.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "MALF+alpha(PAS) 为系统核心",
        "`data -> MALF -> alpha(PAS)`",
        "`G:\\history-lifespan\\lifespan-0.01`",
        "`G:\\history-lifespan\\MarketLifespan-Quant`",
        "`G:\\history-lifespan\\EmotionQuant-gamma`",
        "`H:\\Lifespan-Validated`",
        "`H:\\astock_lifespan-alpha`",
        "不恢复 `structure/filter/family/formal_signal` 为上游真值",
        "实验素材不得直接进入核心",
        "alpha(PAS) 只消费 MALF 事实",
        "`alpha_trigger_event`",
        "`alpha_signal`",
    ]

    for term in required_terms:
        assert term in content


def test_malf_structural_lifespan_proof_harness_freezes_card65_semantics():
    repo_root = Path(__file__).resolve().parents[3]
    content = (
        repo_root / "docs" / "02-spec" / "29-malf-structural-lifespan-proof-harness-v1-20260424.md"
    ).read_text(encoding="utf-8")

    required_terms = [
        "MALF 是结构事实账本",
        "唯一输入仍然是 `price bars`",
        "状态先由结构决定，再由寿命坐标定位",
        "`break != confirmation`",
        "`reborn -> alive` 必须由新方向正式 `HH / LL` 确认",
        "`new-count × no-new-span × life-state`",
        "`WavePosition = (direction, update-rank, stagnation-rank, life-state)`",
        "不输出交易动作",
        "不输出收益概率",
        "不使用均线语义",
        "不反向消费 alpha 判断",
    ]

    for term in required_terms:
        assert term in content
