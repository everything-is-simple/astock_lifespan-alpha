"""CLI stub for run_malf_day_build."""

from __future__ import annotations

import json

from astock_lifespan_alpha.malf import run_malf_day_build


if __name__ == "__main__":
    print(json.dumps(run_malf_day_build().as_dict(), indent=2))

