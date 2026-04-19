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
