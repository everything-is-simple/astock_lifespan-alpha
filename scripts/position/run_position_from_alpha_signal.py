"""CLI entrypoint for run_position_from_alpha_signal."""

from __future__ import annotations

import json

from astock_lifespan_alpha.position import run_position_from_alpha_signal


if __name__ == "__main__":
    print(json.dumps(run_position_from_alpha_signal().as_dict(), indent=2))
