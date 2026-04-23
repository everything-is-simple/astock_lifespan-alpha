"""CLI entrypoint for profile_trade_live_path."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._bootstrap import ensure_repo_src_on_path

ensure_repo_src_on_path(__file__)
from astock_lifespan_alpha.trade.runner import profile_trade_live_path


if __name__ == "__main__":
    print(json.dumps(profile_trade_live_path(), indent=2))
