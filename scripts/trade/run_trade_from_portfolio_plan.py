"""CLI entrypoint for run_trade_from_portfolio_plan."""

from __future__ import annotations

import json

from astock_lifespan_alpha.trade import run_trade_from_portfolio_plan


if __name__ == "__main__":
    print(json.dumps(run_trade_from_portfolio_plan().as_dict(), indent=2))
