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

