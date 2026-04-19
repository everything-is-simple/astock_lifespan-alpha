"""CLI entrypoint for run_system_from_trade."""

from __future__ import annotations

import json

from astock_lifespan_alpha.system import run_system_from_trade


if __name__ == "__main__":
    print(json.dumps(run_system_from_trade().as_dict(), indent=2))

