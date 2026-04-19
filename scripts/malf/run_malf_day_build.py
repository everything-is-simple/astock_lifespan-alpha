"""CLI stub for run_malf_day_build."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._bootstrap import ensure_repo_src_on_path

ensure_repo_src_on_path(__file__)
from astock_lifespan_alpha.malf import run_malf_day_build


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the MALF day build.")
    parser.add_argument("--start-symbol", dest="start_symbol")
    parser.add_argument("--end-symbol", dest="end_symbol")
    parser.add_argument("--symbol-limit", dest="symbol_limit", type=int)
    parser.add_argument("--resume", dest="resume", action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--progress-path", dest="progress_path", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(
        json.dumps(
            run_malf_day_build(
                start_symbol=args.start_symbol,
                end_symbol=args.end_symbol,
                symbol_limit=args.symbol_limit,
                resume=args.resume,
                progress_path=args.progress_path,
            ).as_dict(),
            indent=2,
        )
    )
