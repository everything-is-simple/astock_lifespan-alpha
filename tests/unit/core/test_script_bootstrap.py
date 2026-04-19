from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_bootstrap_helper():
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / "scripts" / "_bootstrap.py"
    spec = importlib.util.spec_from_file_location("script_bootstrap", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ensure_repo_src_on_path


def test_ensure_repo_src_on_path_injects_src_directory(monkeypatch, tmp_path):
    ensure_repo_src_on_path = _load_bootstrap_helper()
    repo_root = tmp_path / "repo"
    script_path = repo_root / "scripts" / "malf" / "run_malf_day_build.py"
    src_path = repo_root / "src"
    (repo_root / "scripts" / "malf").mkdir(parents=True, exist_ok=True)
    src_path.mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")
    script_path.write_text("print('demo')\n", encoding="utf-8")
    monkeypatch.setattr(sys, "path", [])

    injected = ensure_repo_src_on_path(script_path)

    assert injected == src_path
    assert sys.path[0] == str(src_path)
