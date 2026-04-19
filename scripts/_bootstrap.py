"""Shared helpers for running repository scripts directly."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_src_on_path(script_file: str | Path | None = None) -> Path:
    """Inject the repository's src directory into sys.path for direct script execution."""

    anchor = Path(script_file).resolve() if script_file is not None else Path(__file__).resolve()
    repo_root = _discover_repo_root(anchor)
    src_path = repo_root / "src"
    src_str = str(src_path)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
    return src_path


def _discover_repo_root(anchor: Path) -> Path:
    for candidate in (anchor.parent, *anchor.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError(f"Could not locate repository root from script path: {anchor}")
