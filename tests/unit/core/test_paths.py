from __future__ import annotations

from pathlib import Path

from astock_lifespan_alpha.core.paths import NEW_DATA_NAMESPACE, default_settings


def test_workspace_paths_resolve_with_new_namespace(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    report_root = tmp_path / "report"
    temp_root = tmp_path / "temp"
    validated_root = tmp_path / "validated"
    monkeypatch.setenv("LIFESPAN_DATA_ROOT", str(data_root))
    monkeypatch.setenv("LIFESPAN_REPORT_ROOT", str(report_root))
    monkeypatch.setenv("LIFESPAN_TEMP_ROOT", str(temp_root))
    monkeypatch.setenv("LIFESPAN_VALIDATED_ROOT", str(validated_root))

    settings = default_settings(repo_root=repo_root)
    settings.ensure_directories()

    assert settings.repo_root == repo_root.resolve()
    assert settings.source_databases.raw_market == data_root.resolve() / "raw" / "raw_market.duckdb"
    assert settings.source_databases.market_base == data_root.resolve() / "base" / "market_base.duckdb"
    assert settings.databases.namespace_root == data_root.resolve() / NEW_DATA_NAMESPACE
    assert settings.databases.malf_day == data_root.resolve() / NEW_DATA_NAMESPACE / "malf" / "malf_day.duckdb"
    assert settings.module_temp_root("malf") == temp_root.resolve() / NEW_DATA_NAMESPACE / "malf"
    assert settings.module_report_root("alpha") == report_root.resolve() / NEW_DATA_NAMESPACE / "alpha"
    assert settings.module_validated_root("system") == validated_root.resolve() / NEW_DATA_NAMESPACE / "system"

