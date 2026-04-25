"""Run stock-only TDX offline raw ingest into an isolated data root."""

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
from astock_lifespan_alpha.data import run_tdx_stock_raw_ingest


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest local TDX stock files into isolated raw_market.")
    parser.add_argument("--target-data-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=Path("H:/tdx_offline_Data"))
    parser.add_argument("--timeframe", choices=("day", "week", "month"), default="day")
    parser.add_argument("--adjust-method", choices=("backward", "forward", "none"), default="backward")
    parser.add_argument("--run-mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--force-hash", action="store_true")
    parser.add_argument("--instrument", dest="instruments", action="append", default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    summary = run_tdx_stock_raw_ingest(
        source_root=args.source_root,
        target_data_root=args.target_data_root,
        timeframe=args.timeframe,
        adjust_method=args.adjust_method,
        run_mode=args.run_mode,
        force_hash=args.force_hash,
        instruments=args.instruments,
        limit=args.limit,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
