"""CLI stub for run_alpha_cpb_build."""

from __future__ import annotations

import json

from astock_lifespan_alpha.alpha import run_alpha_cpb_build


if __name__ == "__main__":
    print(json.dumps(run_alpha_cpb_build().as_dict(), indent=2))

