"""Audit isolated stock producer target ledgers."""

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
from astock_lifespan_alpha.data import audit_stock_producer_target


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit isolated stock producer target ledgers.")
    parser.add_argument("--target-data-root", type=Path, required=True)
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON summary.")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    indent = 2 if args.pretty else None
    summary = audit_stock_producer_target(target_data_root=args.target_data_root)
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=indent, default=str))


if __name__ == "__main__":
    main()
