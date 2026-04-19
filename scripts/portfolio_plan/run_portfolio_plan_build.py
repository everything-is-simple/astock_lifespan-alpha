"""CLI entrypoint for run_portfolio_plan_build."""

from __future__ import annotations

import json

from astock_lifespan_alpha.portfolio_plan import run_portfolio_plan_build


if __name__ == "__main__":
    print(json.dumps(run_portfolio_plan_build().as_dict(), indent=2))
