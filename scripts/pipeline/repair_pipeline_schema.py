"""CLI entrypoint for pipeline schema repair/probe."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._bootstrap import ensure_repo_src_on_path

ensure_repo_src_on_path(__file__)
from astock_lifespan_alpha.pipeline import repair_pipeline_schema


if __name__ == "__main__":
    print(json.dumps(repair_pipeline_schema().as_dict(), indent=2))
