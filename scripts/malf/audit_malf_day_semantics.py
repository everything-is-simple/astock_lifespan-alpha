"""CLI entrypoint for MALF day semantic audit."""

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
from astock_lifespan_alpha.malf.audit import audit_malf_day_semantics


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the MALF day semantic ledger.")
    parser.add_argument("--run-id", dest="run_id")
    parser.add_argument("--sample-count", dest="sample_count", type=int, default=12)
    parser.add_argument("--output-root", dest="output_root", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(
        json.dumps(
            audit_malf_day_semantics(
                run_id=args.run_id,
                sample_count=args.sample_count,
                output_root=args.output_root,
            ).as_dict(),
            ensure_ascii=False,
            indent=2,
        )
    )
