"""CLI stub for run_alpha_tst_build."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._bootstrap import ensure_repo_src_on_path

ensure_repo_src_on_path(__file__)
from astock_lifespan_alpha.alpha import run_alpha_tst_build


if __name__ == "__main__":
    print(json.dumps(run_alpha_tst_build().as_dict(), indent=2))
