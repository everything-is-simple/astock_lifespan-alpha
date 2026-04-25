"""Run stock-only raw_market -> market_base build in an isolated data root."""

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
from astock_lifespan_alpha.data import run_market_base_build


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build isolated stock market_base from isolated raw_market.")
    parser.add_argument("--target-data-root", type=Path, required=True)
    parser.add_argument("--timeframe", choices=("day", "week", "month"), default="day")
    parser.add_argument("--adjust-method", choices=("backward", "forward", "none"), default="backward")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--build-mode", choices=("full", "incremental"), default="full")
    parser.add_argument("--consume-dirty-only", dest="consume_dirty_only", action="store_true")
    parser.add_argument("--no-consume-dirty-only", dest="consume_dirty_only", action="store_false")
    parser.set_defaults(consume_dirty_only=None)
    parser.add_argument("--keep-dirty-on-success", dest="mark_clean_on_success", action="store_false")
    parser.set_defaults(mark_clean_on_success=True)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    summary = run_market_base_build(
        target_data_root=args.target_data_root,
        timeframe=args.timeframe,
        adjust_method=args.adjust_method,
        instruments=args.instruments,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        build_mode=args.build_mode,
        consume_dirty_only=args.consume_dirty_only,
        mark_clean_on_success=args.mark_clean_on_success,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
