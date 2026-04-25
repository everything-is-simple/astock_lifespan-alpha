"""Audit existing source fact ledgers in read-only mode."""

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
from astock_lifespan_alpha.data import audit_data_source_fact_freeze


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit source fact DuckDB ledgers without writing them.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON summary.")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    indent = 2 if args.pretty else None
    print(json.dumps(audit_data_source_fact_freeze().as_dict(), ensure_ascii=False, indent=indent))


if __name__ == "__main__":
    main()
