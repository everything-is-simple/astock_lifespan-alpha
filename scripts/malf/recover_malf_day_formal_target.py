"""CLI entrypoint for MALF day formal target recovery."""

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
from astock_lifespan_alpha.malf import recover_malf_day_formal_target


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover the MALF day formal target from a materialized baseline run.")
    parser.add_argument("--baseline-run-id", dest="baseline_run_id")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(json.dumps(recover_malf_day_formal_target(baseline_run_id=args.baseline_run_id).as_dict(), indent=2))
