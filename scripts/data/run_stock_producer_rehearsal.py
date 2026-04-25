"""Run isolated stock producer rehearsal."""

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
from astock_lifespan_alpha.data import run_data_stock_producer_rehearsal


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run isolated TDX -> raw_market -> market_base stock rehearsal.")
    parser.add_argument("--target-data-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=Path("H:/tdx_offline_Data"))
    parser.add_argument("--scope", action="append", default=[], help="Scope as timeframe:adjust_method.")
    parser.add_argument("--raw-limit", type=int, default=100)
    parser.add_argument("--base-limit", type=int, default=1000)
    parser.add_argument("--run-id")
    parser.add_argument("--summary-path", type=Path)
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    scopes = _parse_scopes(args.scope)
    summary = run_data_stock_producer_rehearsal(
        source_root=args.source_root,
        target_data_root=args.target_data_root,
        scopes=scopes,
        raw_limit=args.raw_limit,
        base_limit=args.base_limit,
        run_id=args.run_id,
        summary_path=args.summary_path,
    )
    print(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, default=str))


def _parse_scopes(raw_scopes: list[str]) -> tuple[tuple[str, str], ...]:
    if not raw_scopes:
        from astock_lifespan_alpha.data.rehearsal_runner import DEFAULT_REHEARSAL_SCOPES

        return DEFAULT_REHEARSAL_SCOPES
    scopes: list[tuple[str, str]] = []
    for raw_scope in raw_scopes:
        if ":" not in raw_scope:
            raise ValueError(f"Scope must use timeframe:adjust_method format: {raw_scope}")
        timeframe, adjust_method = raw_scope.split(":", 1)
        scopes.append((timeframe.strip(), adjust_method.strip()))
    return tuple(scopes)


if __name__ == "__main__":
    main()
