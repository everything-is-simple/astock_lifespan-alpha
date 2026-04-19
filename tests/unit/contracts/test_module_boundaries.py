from __future__ import annotations

from pathlib import Path


def test_codebase_has_no_structure_or_filter_module_references():
    repo_root = Path(__file__).resolve().parents[3]
    checked_files = list((repo_root / "src").rglob("*.py")) + list((repo_root / "scripts").rglob("*.py"))

    assert checked_files, "Expected source and script files to exist."
    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8")
        assert "astock_lifespan_alpha.structure" not in content
        assert "astock_lifespan_alpha.filter" not in content


def test_system_module_does_not_call_upstream_runners():
    repo_root = Path(__file__).resolve().parents[3]
    checked_files = list((repo_root / "src" / "astock_lifespan_alpha" / "system").rglob("*.py"))

    assert checked_files, "Expected system source files to exist."
    forbidden_terms = [
        "astock_lifespan_alpha.alpha",
        "astock_lifespan_alpha.position",
        "astock_lifespan_alpha.portfolio_plan",
        "run_alpha_",
        "run_position_from_alpha_signal",
        "run_portfolio_plan_build",
        "run_trade_from_portfolio_plan",
    ]
    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8")
        for term in forbidden_terms:
            assert term not in content

