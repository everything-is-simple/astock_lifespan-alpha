"""CLI entrypoint for run_data_to_system_pipeline."""

from __future__ import annotations

import json

from astock_lifespan_alpha.pipeline import run_data_to_system_pipeline


if __name__ == "__main__":
    print(json.dumps(run_data_to_system_pipeline().as_dict(), indent=2))

