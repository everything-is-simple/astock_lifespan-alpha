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


def test_business_modules_do_not_import_pipeline():
    repo_root = Path(__file__).resolve().parents[3]
    module_roots = [
        repo_root / "src" / "astock_lifespan_alpha" / module_name
        for module_name in ("malf", "alpha", "position", "portfolio_plan", "trade", "system")
    ]
    checked_files = [file_path for module_root in module_roots for file_path in module_root.rglob("*.py")]

    assert checked_files, "Expected business source files to exist."
    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8")
        assert "astock_lifespan_alpha.pipeline" not in content


def test_pipeline_module_does_not_write_business_tables_directly():
    repo_root = Path(__file__).resolve().parents[3]
    checked_files = list((repo_root / "src" / "astock_lifespan_alpha" / "pipeline").rglob("*.py"))

    assert checked_files, "Expected pipeline source files to exist."
    business_tables = [
        "malf_wave_scale",
        "alpha_signal",
        "position_candidate",
        "portfolio_plan_snapshot",
        "trade_order",
        "system_trade_readout",
        "system_portfolio_trade_summary",
    ]
    write_prefixes = ("INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE")
    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8")
        upper_content = content.upper()
        for table_name in business_tables:
            for prefix in write_prefixes:
                assert f"{prefix} {table_name.upper()}" not in upper_content
